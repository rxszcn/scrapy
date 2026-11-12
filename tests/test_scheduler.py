from __future__ import annotations

import json
import warnings
from abc import ABC, abstractmethod
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import TYPE_CHECKING, Any, Self
from unittest.mock import Mock

import pytest

from scrapy.core.downloader import Downloader
from scrapy.core.scheduler import Scheduler
from scrapy.crawler import Crawler
from scrapy.dupefilters import BaseDupeFilter
from scrapy.exceptions import ScrapyDeprecationWarning
from scrapy.http import Request, Response
from scrapy.pqueues import ScrapyPriorityQueue
from scrapy.signals import response_received
from scrapy.spiders import Spider
from scrapy.utils.defer import ensure_awaitable
from scrapy.utils.misc import build_from_crawler, load_object
from scrapy.utils.request import RequestFingerprinter
from scrapy.utils.test import get_crawler
from tests.mockserver.http import MockServer
from tests.utils.decorators import coroutine_test, inline_callbacks_test
from tests.utils.downloader import MockDownloader

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable
    from pathlib import Path


class MockCrawler(Crawler):
    def __init__(self, priority_queue_cls: str, jobdir: Path | None, **settings: Any):
        settings = {
            "SCHEDULER_DEBUG": False,
            "SCHEDULER_DISK_QUEUE": "scrapy.squeues.PickleLifoDiskQueue",
            "SCHEDULER_MEMORY_QUEUE": "scrapy.squeues.LifoMemoryQueue",
            "SCHEDULER_PRIORITY_QUEUE": priority_queue_cls,
            "JOBDIR": str(jobdir) if jobdir is not None else None,
            "DUPEFILTER_CLASS": "scrapy.dupefilters.BaseDupeFilter",
            **settings,
        }
        super().__init__(Spider, settings)
        self.engine = Mock(downloader=MockDownloader())
        self.stats = load_object(self.settings["STATS_CLASS"])(self)


@asynccontextmanager
async def create_scheduler(
    priority_queue_cls: str, jobdir: Path | None, **settings: Any
) -> AsyncGenerator[Scheduler]:
    mock_crawler = MockCrawler(priority_queue_cls, jobdir, **settings)
    scheduler = build_from_crawler(Scheduler, mock_crawler)
    spider = Spider.from_crawler(mock_crawler, name="spider")
    await ensure_awaitable(scheduler.open(spider))
    try:
        yield scheduler
    finally:
        await ensure_awaitable(scheduler.close("finished"))
        await mock_crawler.stop_async()
        mock_crawler.engine.downloader.close()


_PRIORITIES = [
    ("http://foo.com/a", -2),
    ("http://foo.com/d", 1),
    ("http://foo.com/b", -1),
    ("http://foo.com/c", 0),
    ("http://foo.com/e", 2),
]


_URLS = {"http://foo.com/a", "http://foo.com/b", "http://foo.com/c"}


def _active_json(jobdir: Path) -> Any:
    path = jobdir / "requests.queue" / "active.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _enqueue_across_queues(scheduler: Scheduler) -> None:
    for name, priority in (("memory", 0), ("disk", 1), ("disk", 0), ("memory", -1)):
        request = Request(f"https://example.com/{name}{priority}", priority=priority)
        if name == "memory":
            request.meta["unserializable"] = lambda: None
        scheduler.enqueue_request(request)


def _dequeue_urls(scheduler: Scheduler) -> list[str]:
    urls = []
    while request := scheduler.next_request():
        urls.append(request.url)
    return urls


class TestSchedulerBase(ABC):
    reopen = False

    @property
    @abstractmethod
    def priority_queue_cls(self) -> str:
        raise NotImplementedError

    @pytest.fixture
    def jobdir(self) -> Path | None:
        return None

    def create_scheduler(
        self, jobdir: Path | None, **settings: Any
    ) -> AbstractAsyncContextManager[Scheduler]:
        return create_scheduler(self.priority_queue_cls, jobdir, **settings)

    @asynccontextmanager
    async def create_scheduler_for_assertions(
        self, jobdir: Path | None, setup: Callable[[Scheduler], None]
    ) -> AsyncGenerator[Scheduler]:
        if self.reopen:
            async with self.create_scheduler(jobdir) as scheduler:
                setup(scheduler)
            async with self.create_scheduler(jobdir) as scheduler:
                yield scheduler
        else:
            async with self.create_scheduler(jobdir) as scheduler:
                setup(scheduler)
                yield scheduler


class SchedulerTestMixin(TestSchedulerBase):
    @coroutine_test
    async def test_length(self, jobdir: Path | None) -> None:
        def _setup(scheduler: Scheduler) -> None:
            assert not scheduler.has_pending_requests()
            assert len(scheduler) == 0

            for url in _URLS:
                scheduler.enqueue_request(Request(url))

        async with self.create_scheduler_for_assertions(jobdir, _setup) as scheduler:
            assert scheduler.has_pending_requests()
            assert len(scheduler) == len(_URLS)

    @coroutine_test
    async def test_dequeue(self, jobdir: Path | None) -> None:
        def _setup(scheduler: Scheduler) -> None:
            for url in _URLS:
                scheduler.enqueue_request(Request(url))

        urls = set()
        async with self.create_scheduler_for_assertions(jobdir, _setup) as scheduler:
            while scheduler.has_pending_requests():
                request = scheduler.next_request()
                assert request is not None
                urls.add(request.url)

        assert urls == _URLS

    @coroutine_test
    async def test_dequeue_priorities(self, jobdir: Path | None) -> None:
        def _setup(scheduler: Scheduler) -> None:
            for url, priority in _PRIORITIES:
                scheduler.enqueue_request(Request(url, priority=priority))

        priorities = []
        async with self.create_scheduler_for_assertions(jobdir, _setup) as scheduler:
            while scheduler.has_pending_requests():
                request = scheduler.next_request()
                assert request is not None
                priorities.append(request.priority)

        assert priorities == sorted([x[1] for x in _PRIORITIES], key=lambda x: -x)


class TestSchedulerInMemoryBase(SchedulerTestMixin):
    pass


class TestSchedulerOnDiskBase(SchedulerTestMixin):
    reopen = True

    @pytest.fixture
    def jobdir(self, tmp_path: Path) -> Path | None:
        return tmp_path

    @coroutine_test
    async def test_dequeue_priorities_across_queues(self, jobdir: Path | None) -> None:
        async with self.create_scheduler(jobdir) as scheduler:
            _enqueue_across_queues(scheduler)
            assert _dequeue_urls(scheduler) == [
                "https://example.com/disk1",
                "https://example.com/memory0",
                "https://example.com/disk0",
                "https://example.com/memory-1",
            ]


class TestSchedulerInMemory(TestSchedulerInMemoryBase):
    priority_queue_cls = "scrapy.pqueues.ScrapyPriorityQueue"


class TestSchedulerOnDisk(TestSchedulerOnDiskBase):
    priority_queue_cls = "scrapy.pqueues.ScrapyPriorityQueue"

    @coroutine_test
    async def test_active_json(self, jobdir: Path | None) -> None:
        assert jobdir is not None
        path = jobdir / "requests.queue" / "active.json"
        async with self.create_scheduler(jobdir) as scheduler:
            for url, priority in _PRIORITIES:
                scheduler.enqueue_request(Request(url, priority=priority))
            assert not path.exists()
        assert _active_json(jobdir) == [-2, -1, 0, 1, 2]

    @coroutine_test
    async def test_active_json_sync_every(self, jobdir: Path | None) -> None:
        assert jobdir is not None
        async with self.create_scheduler(jobdir, JOBDIR_SYNC_EVERY=1) as scheduler:
            for url, priority in _PRIORITIES:
                scheduler.enqueue_request(Request(url, priority=priority))
            assert _active_json(jobdir) == [-2, -1, 0, 1, 2]
            scheduler.next_request()
            assert _active_json(jobdir) == [-1, 0, 1, 2]

    @coroutine_test
    async def test_active_json_sync_window(self, jobdir: Path | None) -> None:
        assert jobdir is not None
        path = jobdir / "requests.queue" / "active.json"
        async with self.create_scheduler(jobdir, JOBDIR_SYNC_EVERY=2) as scheduler:
            scheduler.enqueue_request(Request("http://foo.com/a", priority=-2))
            assert not path.exists()
            scheduler.enqueue_request(Request("http://foo.com/b", priority=1))
            assert _active_json(jobdir) == [-1, 2]
            scheduler.enqueue_request(Request("http://foo.com/c", priority=0))
            assert _active_json(jobdir) == [-1, 2]
            scheduler.enqueue_request(Request("http://foo.com/d", priority=1))
            assert _active_json(jobdir) == [-1, 2]
            scheduler.enqueue_request(Request("http://foo.com/e", priority=2))
            assert _active_json(jobdir) == [-2, -1, 0, 2]


_URLS_WITH_SLOTS = [
    ("http://foo.com/a", "a"),
    ("http://foo.com/b", "a"),
    ("http://foo.com/c", "b"),
    ("http://foo.com/d", "b"),
    ("http://foo.com/e", "c"),
    ("http://foo.com/f", "c"),
]


class TestMigration:
    @coroutine_test
    async def test_migration(self, tmp_path: Path) -> None:
        async with create_scheduler(
            "scrapy.pqueues.ScrapyPriorityQueue", tmp_path
        ) as prev_scheduler:
            for url in _URLS:
                prev_scheduler.enqueue_request(Request(url))

        with pytest.raises(
            ValueError,
            match="DownloaderAwarePriorityQueue accepts ``slot_startprios`` as a dict",
        ):
            async with create_scheduler(
                "scrapy.pqueues.DownloaderAwarePriorityQueue", tmp_path
            ):
                pass


def _is_scheduling_fair(enqueued_slots: list[str], dequeued_slots: list[str]) -> bool:
    """
    We enqueued same number of requests for every slot.
    Assert correct order, e.g.

    >>> enqueued = ['a', 'b', 'c'] * 2
    >>> correct = ['a', 'c', 'b', 'b', 'a', 'c']
    >>> incorrect = ['a', 'a', 'b', 'c', 'c', 'b']
    >>> _is_scheduling_fair(enqueued, correct)
    True
    >>> _is_scheduling_fair(enqueued, incorrect)
    False
    """
    if len(dequeued_slots) != len(enqueued_slots):
        return False

    slots_number = len(set(enqueued_slots))
    for i in range(0, len(dequeued_slots), slots_number):
        part = dequeued_slots[i : i + slots_number]
        if len(part) != len(set(part)):
            return False

    return True


class DownloaderAwareSchedulerTestMixin(TestSchedulerBase):
    reopen = False
    priority_queue_cls = "scrapy.pqueues.DownloaderAwarePriorityQueue"

    @coroutine_test
    async def test_logic(self, jobdir: Path | None) -> None:
        def _setup(scheduler: Scheduler) -> None:
            for url, slot in _URLS_WITH_SLOTS:
                request = Request(url)
                request.meta[Downloader.DOWNLOAD_SLOT] = slot
                scheduler.enqueue_request(request)

        def _assert(scheduler: Scheduler) -> None:
            dequeued_slots: list[str] = []
            requests: list[Request] = []
            assert scheduler.crawler
            downloader = scheduler.crawler.engine.downloader
            assert isinstance(downloader, MockDownloader)
            while scheduler.has_pending_requests():
                request = scheduler.next_request()
                assert request is not None
                slot = downloader.get_slot_key(request)
                dequeued_slots.append(slot)
                downloader.increment(slot)
                requests.append(request)

            for request in requests:
                slot = downloader.get_slot_key(request)
                downloader.decrement(slot)

            assert _is_scheduling_fair([s for u, s in _URLS_WITH_SLOTS], dequeued_slots)
            assert sum(len(s.active) for s in downloader.slots.values()) == 0

        if self.reopen:
            async with self.create_scheduler(jobdir) as scheduler:
                _setup(scheduler)
            async with self.create_scheduler(jobdir) as scheduler:
                _assert(scheduler)
        else:
            async with self.create_scheduler(jobdir) as scheduler:
                _setup(scheduler)
                _assert(scheduler)


class TestSchedulerWithDownloaderAwareInMemory(
    DownloaderAwareSchedulerTestMixin, TestSchedulerInMemoryBase
):
    pass


class TestSchedulerWithDownloaderAwareOnDisk(
    DownloaderAwareSchedulerTestMixin, TestSchedulerOnDiskBase
):
    reopen = True

    @coroutine_test
    async def test_active_json_sync_every(self, jobdir: Path | None) -> None:
        assert jobdir is not None
        async with self.create_scheduler(jobdir, JOBDIR_SYNC_EVERY=1) as scheduler:

            def enqueue(slot: str, priority: int) -> None:
                request = Request(f"http://foo.com/{slot}{priority}", priority=priority)
                request.meta[Downloader.DOWNLOAD_SLOT] = slot
                scheduler.enqueue_request(request)

            enqueue("a", 1)
            assert _active_json(jobdir) == {"a": [-1]}
            enqueue("a", 2)
            assert _active_json(jobdir) == {"a": [-2, -1]}
            enqueue("b", 1)
            assert _active_json(jobdir) == {"a": [-2, -1], "b": [-1]}
            scheduler.next_request()
            assert _active_json(jobdir) == {"a": [-1], "b": [-1]}


class StartUrlsSpider(Spider):
    def __init__(self, start_urls):
        self.start_urls = start_urls
        super().__init__(name="StartUrlsSpider")

    def parse(self, response):
        pass


class TestIntegrationWithDownloaderAwareInMemory:
    def setup_method(self):
        self.crawler = get_crawler(
            spidercls=StartUrlsSpider,
            settings_dict={
                "SCHEDULER_PRIORITY_QUEUE": "scrapy.pqueues.DownloaderAwarePriorityQueue",
                "DUPEFILTER_CLASS": "scrapy.dupefilters.BaseDupeFilter",
            },
        )

    @inline_callbacks_test
    def test_integration_downloader_aware_priority_queue(self):
        with MockServer() as mockserver:
            url = mockserver.url("/status?n=200", is_secure=False)
            start_urls = [url] * 6
            yield self.crawler.crawl(start_urls)
            assert self.crawler.stats.get_value("downloader/response_count") == len(
                start_urls
            )


class TestIncompatibility:
    def _incompatible(self) -> None:
        settings = {
            "SCHEDULER_PRIORITY_QUEUE": "scrapy.pqueues.DownloaderAwarePriorityQueue",
            "CONCURRENT_REQUESTS_PER_IP": 1,
        }
        crawler = get_crawler(Spider, settings)
        scheduler = build_from_crawler(Scheduler, crawler)
        spider = Spider.from_crawler(crawler, name="spider")
        scheduler.open(spider)

    def test_incompatibility(self):
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                category=ScrapyDeprecationWarning,
                message="The CONCURRENT_REQUESTS_PER_IP setting is deprecated",
            )
            with pytest.raises(
                ValueError, match="does not support CONCURRENT_REQUESTS_PER_IP"
            ):
                self._incompatible()


class _PriorityQueueWithoutNextPriority:
    @classmethod
    def from_crawler(cls, crawler: Crawler, *args: Any, **kwargs: Any) -> Self:
        return cls(ScrapyPriorityQueue.from_crawler(crawler, *args, **kwargs))

    def __init__(self, queue: ScrapyPriorityQueue):
        self._queue = queue

    def push(self, request: Request) -> None:
        self._queue.push(request)

    def pop(self) -> Request | None:
        return self._queue.pop()

    def close(self) -> list[int]:
        return self._queue.close()

    def __len__(self) -> int:
        return len(self._queue)


@coroutine_test
async def test_priority_queue_without_next_priority(tmp_path: Path) -> None:
    async with create_scheduler(
        f"{__name__}._PriorityQueueWithoutNextPriority", tmp_path
    ) as scheduler:
        _enqueue_across_queues(scheduler)
        assert _dequeue_urls(scheduler) == [
            "https://example.com/memory0",
            "https://example.com/memory-1",
            "https://example.com/disk1",
            "https://example.com/disk0",
        ]


def test_scheduler_without_crawler() -> None:
    """The crawler argument of Scheduler is optional."""
    scheduler = Scheduler(BaseDupeFilter())
    assert scheduler.crawler is None


@coroutine_test
async def test_unserializable_requests(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Requests that cannot be serialized fall back to the memory queue."""

    class UnserializableSpider(Spider):
        name = "unserializable"

        async def start(self):
            for index in range(2):
                yield Request(f"data:,{index}", meta={"callable": lambda: None})

    crawler = get_crawler(
        UnserializableSpider, {"JOBDIR": str(tmp_path), "SCHEDULER_DEBUG": True}
    )
    await crawler.crawl_async()
    assert crawler.stats.get_value("scheduler/unserializable") == 2
    assert crawler.stats.get_value("scheduler/enqueued/memory") == 2
    assert caplog.text.count("Unable to serialize request") == 1


class TestIncrementalScheduler:
    """Tests for INCREMENTAL_CRAWL_ENABLED."""

    @asynccontextmanager
    async def create_scheduler(
        self, jobdir: Path | None, **settings: Any
    ) -> AsyncGenerator[Scheduler]:
        settings = {
            "DUPEFILTER_CLASS": "scrapy.dupefilters.RFPDupeFilter",
            "HTTPCACHE_ENABLED": True,
            "INCREMENTAL_CRAWL_ENABLED": True,
            **settings,
        }
        mock_crawler = MockCrawler(
            "scrapy.pqueues.ScrapyPriorityQueue",
            jobdir,
            **settings,
        )
        # Set by Crawler._apply_settings() in a real crawl.
        mock_crawler.request_fingerprinter = RequestFingerprinter()
        scheduler = build_from_crawler(Scheduler, mock_crawler)
        spider = Spider.from_crawler(mock_crawler, name="spider")
        await ensure_awaitable(scheduler.open(spider))
        try:
            yield scheduler
        finally:
            await ensure_awaitable(scheduler.close("finished"))
            await mock_crawler.stop_async()
            mock_crawler.engine.downloader.close()

    @staticmethod
    def _download(scheduler: Scheduler, request: Request) -> None:
        """Simulate a successful download of a dequeued request."""
        assert scheduler.crawler is not None
        scheduler.crawler.signals.send_catch_log(
            response_received,
            response=Response(request.url, request=request),
            request=request,
            spider=scheduler.spider,
        )

    @coroutine_test
    async def test_inflight_requests_are_recovered(self, tmp_path: Path) -> None:
        """Requests dequeued when the job stops are scheduled again on the
        next run, no matter how many were in-flight (i.e. the concurrency
        of the previous run), and no URL is scheduled twice."""
        urls = sorted(f"https://example.com/{i}" for i in range(5))
        async with self.create_scheduler(tmp_path) as scheduler:
            for url in urls:
                scheduler.enqueue_request(Request(url))
            assert scheduler.next_request() is not None
            assert scheduler.next_request() is not None
        async with self.create_scheduler(tmp_path) as scheduler:
            assert scheduler.stats is not None
            assert scheduler.stats.get_value("incremental/recovered") == 2
            # The spider yielding the same URLs again changes nothing: the
            # in-flight ones are already scheduled, the rest are still in
            # the persisted queue.
            for url in urls:
                assert not scheduler.enqueue_request(Request(url))
            assert sorted(_dequeue_urls(scheduler)) == urls

    @coroutine_test
    async def test_completed_requests_are_not_recovered(self, tmp_path: Path) -> None:
        """Requests whose response reached the engine are not scheduled
        again on the next run."""
        async with self.create_scheduler(tmp_path) as scheduler:
            scheduler.enqueue_request(Request("https://example.com/a"))
            request = scheduler.next_request()
            assert request is not None
            self._download(scheduler, request)
        async with self.create_scheduler(tmp_path) as scheduler:
            assert not scheduler.has_pending_requests()

    @coroutine_test
    async def test_done_requests_are_rechecked(self, tmp_path: Path) -> None:
        """A request that finished in a previous run is scheduled again for
        a cache recheck instead of being filtered out, but only once per
        run."""
        async with self.create_scheduler(tmp_path) as scheduler:
            scheduler.enqueue_request(Request("https://example.com/a"))
            request = scheduler.next_request()
            assert request is not None
            self._download(scheduler, request)
        async with self.create_scheduler(tmp_path) as scheduler:
            request = Request("https://example.com/a")
            assert scheduler.enqueue_request(request)
            assert request.meta["_incremental_recheck"] is True
            assert scheduler.stats is not None
            assert scheduler.stats.get_value("incremental/recheck") == 1
            # A second yield of the same URL in the same run is filtered.
            assert not scheduler.enqueue_request(Request("https://example.com/a"))
            assert _dequeue_urls(scheduler) == ["https://example.com/a"]

    @coroutine_test
    async def test_seen_but_unfinished_requests_are_filtered(
        self, tmp_path: Path
    ) -> None:
        """A request that was scheduled but never finished is filtered out
        when yielded again; the persisted queue and the in-flight journal
        already track it."""
        async with self.create_scheduler(tmp_path) as scheduler:
            scheduler.enqueue_request(Request("https://example.com/a"))
            scheduler.enqueue_request(Request("https://example.com/b"))
            assert scheduler.next_request() is not None  # b, in-flight
        async with self.create_scheduler(tmp_path) as scheduler:
            assert not scheduler.enqueue_request(Request("https://example.com/a"))
            assert not scheduler.enqueue_request(Request("https://example.com/b"))
            assert sorted(_dequeue_urls(scheduler)) == [
                "https://example.com/a",
                "https://example.com/b",
            ]

    @coroutine_test
    async def test_failed_retry_keeps_done_state(self, tmp_path: Path) -> None:
        """A failed retry of a recheck does not dirty the done state of a
        fingerprint: the next run still treats it as done and does not
        recover it as a lost request."""
        url = "https://example.com/a"
        async with self.create_scheduler(tmp_path) as scheduler:
            scheduler.enqueue_request(Request(url))
            request = scheduler.next_request()
            assert request is not None
            self._download(scheduler, request)
        async with self.create_scheduler(tmp_path) as scheduler:
            assert scheduler.enqueue_request(Request(url))
            request = scheduler.next_request()
            assert request is not None
            assert request.meta["_incremental_recheck"] is True
            # The recheck fails and is retried once, without success (no
            # response_received signal is sent).
            retry = Request(url, meta={"skip_dupefilter_once": True})
            assert scheduler.enqueue_request(retry)
            assert "_incremental_recheck" not in retry.meta
            assert scheduler.next_request() is not None
        async with self.create_scheduler(tmp_path) as scheduler:
            assert scheduler.stats is not None
            assert not scheduler.stats.get_value("incremental/recovered")
            request = Request(url)
            assert scheduler.enqueue_request(request)
            assert request.meta["_incremental_recheck"] is True

    @coroutine_test
    async def test_unserializable_inflight_requests_are_not_recovered(
        self, tmp_path: Path
    ) -> None:
        """Requests that cannot be serialized cannot be recovered."""
        async with self.create_scheduler(tmp_path) as scheduler:
            request = Request(
                "https://example.com/a", meta={"unserializable": lambda: None}
            )
            scheduler.enqueue_request(request)
            assert scheduler.next_request() is not None
        async with self.create_scheduler(tmp_path) as scheduler:
            assert not scheduler.has_pending_requests()

    @coroutine_test
    async def test_dont_filter_requests_are_rechecked(self, tmp_path: Path) -> None:
        """Requests with dont_filter (e.g. start requests) skip the
        duplicate filter, but if they were downloaded in a previous run they
        are still scheduled for a cache recheck."""
        url = "https://example.com/a"
        async with self.create_scheduler(tmp_path) as scheduler:
            assert scheduler.enqueue_request(Request(url, dont_filter=True))
            request = scheduler.next_request()
            assert request is not None
            self._download(scheduler, request)
        async with self.create_scheduler(tmp_path) as scheduler:
            request = Request(url, dont_filter=True)
            assert scheduler.enqueue_request(request)
            assert request.meta["_incremental_recheck"] is True
            # A regular request to a URL only ever downloaded through a
            # dont_filter request is also rechecked instead of downloaded
            # and parsed blindly.
            request = Request(url)
            assert scheduler.enqueue_request(request)
            assert request.meta["_incremental_recheck"] is True

    @coroutine_test
    async def test_no_jobdir_disables_incremental(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level("WARNING"):
            async with self.create_scheduler(None) as scheduler:
                scheduler.enqueue_request(Request("https://example.com/a"))
                assert scheduler.next_request() is not None
        assert "JOBDIR" in caplog.text

    @coroutine_test
    async def test_no_httpcache_disables_recheck(self, tmp_path: Path) -> None:
        """Without HTTPCACHE_ENABLED, done requests are filtered out as
        usual instead of being scheduled for a recheck."""
        settings = {"HTTPCACHE_ENABLED": False}
        async with self.create_scheduler(tmp_path, **settings) as scheduler:
            scheduler.enqueue_request(Request("https://example.com/a"))
            request = scheduler.next_request()
            assert request is not None
            self._download(scheduler, request)
        async with self.create_scheduler(tmp_path, **settings) as scheduler:
            assert not scheduler.enqueue_request(Request("https://example.com/a"))


class _IncrementalSpider(Spider):
    name = "incremental"

    def __init__(self, urls, parsed, **kwargs):
        super().__init__(**kwargs)
        self.start_urls = urls
        self.parsed = parsed

    def parse(self, response):
        self.parsed.append(response.url)


@coroutine_test
async def test_incremental_recrawl(tmp_path: Path) -> None:
    """A second run of a finished job re-checks every page against the HTTP
    cache: fresh cached responses are neither downloaded nor parsed again."""
    settings = {
        "JOBDIR": str(tmp_path / "job"),
        "HTTPCACHE_ENABLED": True,
        "HTTPCACHE_DIR": str(tmp_path / "cache"),
        "INCREMENTAL_CRAWL_ENABLED": True,
    }
    with MockServer() as mockserver:
        urls = [
            mockserver.url("/status?n=200", is_secure=False),
            mockserver.url("/echo?body=test", is_secure=False),
        ]
        parsed: list[str] = []
        crawler = get_crawler(_IncrementalSpider, settings)
        await crawler.crawl_async(urls=urls, parsed=parsed)
        assert sorted(parsed) == sorted(urls)
        assert crawler.stats.get_value("downloader/response_count") == 2

        parsed = []
        crawler = get_crawler(_IncrementalSpider, settings)
        await crawler.crawl_async(urls=urls, parsed=parsed)
        assert parsed == []
        assert not crawler.stats.get_value("downloader/response_count")
        assert crawler.stats.get_value("incremental/recheck") == 2
        assert crawler.stats.get_value("incremental/unchanged") == 2
