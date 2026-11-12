.. _topics-jobs:

=================================
Jobs: pausing and resuming crawls
=================================

Sometimes, for big sites, it's desirable to pause crawls and be able to resume
them later.

Scrapy supports this functionality out of the box through the following
facilities:

* a scheduler that persists scheduled requests on disk

* a duplicates filter that persists visited requests on disk

* an extension that keeps some spider state (key/value pairs) persistent
  between batches

.. _job-dir:

Job directory
=============

To enable persistence support, define a *job directory* through the
:setting:`JOBDIR` setting.

The job directory will store all required data to keep the state of a *single*
job (i.e. a spider run), so that if stopped cleanly, it can be resumed later.

.. warning:: This directory must *not* be shared by different spiders, or even
    different jobs of the same spider. See :ref:`job-dir-spider-name`.

.. warning:: Treat the job directory with the same security care as your
    Scrapy project source code. Do not point ``JOBDIR`` to a path that
    untrusted parties can write to.

See also :ref:`job-dir-contents`.

How to use it
=============

To start a spider with persistence support enabled, run it like this::

    scrapy crawl somespider -s JOBDIR=crawls/somespider-1

Then you can stop the spider safely at any time (by pressing Ctrl-C or sending
a signal), and resume it later by issuing the same command::

    scrapy crawl somespider -s JOBDIR=crawls/somespider-1

.. _job-dir-spider-name:

Deriving the job directory from the spider name
===============================================

One way to keep job directories apart is to build the path in
:meth:`~scrapy.Spider.update_settings`, so that a group of spiders shares the
same path except for the spider name:

.. code-block:: python

    from pathlib import Path

    from scrapy import Spider


    class BaseSpider(Spider):
        @classmethod
        def update_settings(cls, settings):
            super().update_settings(settings)
            settings.set("JOBDIR", str(Path("crawls", cls.name)), priority="spider")


.. _topics-keeping-persistent-state-between-batches:

Keeping persistent state between batches
========================================

Sometimes you'll want to keep persistent spider state between pause/resume
batches. Use the ``spider.state`` attribute for that. It should be a dict.
There's :ref:`a built-in extension <topics-extensions-ref-spiderstate>` that
takes care of serializing, storing and loading that attribute from the job
directory when the spider starts and stops.

Here's an example of a callback that uses the spider state (other spider code
is omitted for brevity):

.. code-block:: python

    def parse_item(self, response):
        # parse item here
        self.state["items_count"] = self.state.get("items_count", 0) + 1

.. _incremental-recrawls:

Incremental recrawls
====================

A regular resumed job only sends requests that were still pending when the
job stopped: requests that finished are never sent again, so already-crawled
pages are never updated. Setting :setting:`INCREMENTAL_CRAWL_ENABLED` to
``True`` changes that, so that a job can be recrawled periodically to refresh
its data.

Incremental recrawling requires :setting:`JOBDIR` and
:setting:`HTTPCACHE_ENABLED`. It works as follows:

-   Requests that finished downloading in a previous run are scheduled
    again, but instead of being downloaded and parsed blindly, they are
    checked against the :ref:`HTTP cache <httpcache-middleware>`:

    -   If the cached response is still fresh, the request is dropped
        without downloading or parsing anything.

    -   If the cached response is stale, a conditional request is sent when
        the cached response allows it, e.g. with the
        :class:`~scrapy.extensions.httpcache.RFC2616Policy` cache policy. If
        the server answers that the response has not changed (304), the
        cached response is kept and, again, nothing is parsed. Only if the
        response has changed is the new response parsed.

    -   If there is no cached response, the request is downloaded and parsed
        as usual.

-   Requests that were being downloaded when the previous run stopped, and
    requests whose download failed, are scheduled again on the next run, so
    no page is lost even after an unclean shutdown.

Because unchanged pages are not parsed again, their items are not yielded
again either, so recrawling a job does not duplicate items.

Keep the following in mind:

-   Whether a cached response is fresh depends on the cache policy and
    :setting:`HTTPCACHE_EXPIRATION_SECS`. The default policy,
    :class:`~scrapy.extensions.httpcache.DummyPolicy`, considers every
    cached response fresh, so pages are only re-downloaded after their cache
    entries expire, and expiring entries cannot be revalidated, hence pages
    whose entries expire are downloaded and parsed again. Keep
    :setting:`HTTPCACHE_EXPIRATION_SECS` at ``0`` (no expiration) to avoid
    that.

-   Requests that cannot be cached, e.g. with the :reqmeta:`dont_cache`
    meta key, are dropped without downloading on recrawls, since there is no
    cached response to check them against.

-   Requests with :attr:`~scrapy.http.Request.dont_filter` enabled, such as
    :ref:`start requests <start-requests>`, skip the duplicate filter but
    are still checked against the cache like any other request.

The following :ref:`stats <topics-stats>` reflect incremental recrawling:

-   ``incremental/recheck``: requests scheduled to re-check their cached
    response.

-   ``incremental/recovered``: requests scheduled again on startup because
    their download did not finish in the previous run.

-   ``incremental/unchanged``: rechecked requests resolved from the cache
    without parsing.

-   ``incremental/uncacheable``: rechecked requests dropped because they
    cannot be cached.


Persistence gotchas
===================

There are a few things to keep in mind if you want to use Scrapy's persistence
support:

Pause limitations
-----------------

Job pausing and resuming is only supported when the spider is paused by
stopping it cleanly. Forced, sudden or otherwise unclean shutdown can lead to
data corruption in the job directory, which may prevent the spider from
resuming correctly.

Scrapy version changes
----------------------

The contents of a job directory are an implementation detail of the Scrapy
version that wrote them. A job must be resumed with the same Scrapy version
that paused it; after upgrading or downgrading Scrapy, start a new job with a
new job directory.

Feed exports
------------

When a job is resumed, the :ref:`feeds <topics-feed-exports>` of the previous
run already exist, so their :ref:`mode <feed-mode>` determines what happens to
the items of the resumed run: ``"append"`` adds them to those feeds,
``"overwrite"`` replaces the items of the previous run with them, and
``"create"`` does not write them anywhere.

Cookie expiration
-----------------

Cookies may expire, so if you don't resume your spider quickly, the scheduled
requests may no longer work. This won't be an issue if your spider doesn't rely
on cookies.

.. _request-serialization:

Request serialization
---------------------

For persistence to work, :class:`~scrapy.Request` objects must be
serializable with :mod:`pickle`, except for the :ref:`callback
<callbacks>` and :ref:`errback
<errbacks>` values passed to their ``__init__``
method, which must be methods of the running :class:`~scrapy.Spider` class.

Requests that cannot be serialized are kept in memory only: they are still
sent, but they are lost when the crawl is paused.

If you wish to log the requests that couldn't be serialized, you can set the
:setting:`SCHEDULER_DEBUG` setting to ``True`` in the project's settings page.
It is ``False`` by default.

.. note:: Because requests are serialized with :mod:`pickle`, the objects you
    store on a request, such as the values of its
    :attr:`~scrapy.Request.cb_kwargs` and :attr:`~scrapy.Request.meta`
    dictionaries, are deep-copied when the request is written to and later read
    back from the job directory. As a result, the callback receives a *copy* of
    those objects rather than the original ones, and changes made to the copy are
    not reflected in the original object. Keep this in mind if you rely on
    sharing mutable state through ``cb_kwargs`` or ``meta``.

.. _job-dir-contents:

Job directory contents
======================

The contents of a job directory depend on the components used during the job.
Components known to write in the job directory include the :ref:`scheduler
<topics-scheduler>` and the :class:`~scrapy.extensions.spiderstate.SpiderState`
extension. See the reference documentation of the corresponding components for
details.

For example, with default settings, the job directory may look like this:

.. code-block:: none

    ├── requests.queue
    |   ├── active.json
    |   └── {hostname}-{hash}
    |       └── {priority}{s?}
    |           ├── q{00000}
    |           └── info.json
    ├── requests.seen
    └── spider.state

Where:

-   :class:`~scrapy.core.scheduler.Scheduler` creates the ``requests.queue/``
    directory and the ``active.json`` file, the latter containing the state
    data of :class:`~scrapy.pqueues.DownloaderAwarePriorityQueue`, written
    when the job stops and, with :setting:`JOBDIR_SYNC_EVERY`, as it runs.

-   :class:`~scrapy.pqueues.DownloaderAwarePriorityQueue` creates the
    ``{hostname}-{hash}`` directories.

-   :class:`~scrapy.pqueues.ScrapyPriorityQueue` creates the ``{priority}{s?}``
    directories.

-   :class:`scrapy.squeues.PickleFifoDiskQueue`, a subclass of
    :class:`queuelib.FifoDiskQueue` that uses :mod:`pickle` to serialize
    :class:`dict` representations of :class:`scrapy.Request` objects, creates
    the ``info.json`` and ``q{00000}`` files.

-   :class:`~scrapy.dupefilters.RFPDupeFilter` creates the ``requests.seen``
    file.

-   :class:`~scrapy.extensions.spiderstate.SpiderState` creates the
    ``spider.state`` file.
