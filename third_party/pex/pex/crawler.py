# Copyright 2014 Pants project contributors (see CONTRIBUTORS.md).
# Licensed under the Apache License, Version 2.0 (see LICENSE).

"""Support for webpage parsing and crawling."""

import os
import re
import threading
import traceback

from .compatibility import PY3
from .http import Context
from .link import Link
from .tracer import TRACER
from .util import Memoizer

if PY3:
  from queue import Empty, Queue
  from urllib.parse import urlparse
else:
  from Queue import Empty, Queue
  from urlparse import urlparse


def unescape(s):
  """Unescapes html. Taken from https://wiki.python.org/moin/EscapingHtml"""
  s = s.replace("&lt;", "<")
  s = s.replace("&gt;", ">")
  # this has to be last:
  s = s.replace("&amp;", "&")
  return s


class PageParser(object):
  """A helper class to extract and differentiate ordinary and download links from webpages."""

  HREF_RE = re.compile(r"""href=(?:"([^"]*)"|\'([^\']*)\'|([^>\s\n]*))""", re.I | re.S)
  REL_RE = re.compile(r"""<[^>]*\srel\s*=\s*['"]?([^'">]+)[^>]*>""", re.I)
  REL_SKIP_EXTENSIONS = frozenset(['.zip', '.tar', '.tar.gz', '.tar.bz2', '.tgz', '.exe'])
  REL_TYPES = frozenset(['homepage', 'download'])

  @classmethod
  def href_match_to_url(cls, match):
    def pick(group):
      return '' if group is None else group
    return unescape(pick(match.group(1)) or pick(match.group(2)) or pick(match.group(3)))

  @classmethod
  def rel_links(cls, page):
    """return rel= links that should be scraped, skipping obviously data links."""
    for match in cls.REL_RE.finditer(page):
      href, rel = match.group(0), match.group(1)
      if rel not in cls.REL_TYPES:
        continue
      href_match = cls.HREF_RE.search(href)
      if href_match:
        href = cls.href_match_to_url(href_match)
        parsed_href = urlparse(href)
        if any(parsed_href.path.endswith(ext) for ext in cls.REL_SKIP_EXTENSIONS):
          continue
        yield href

  @classmethod
  def links(cls, page):
    """return all links on a page, including potentially rel= links."""
    for match in cls.HREF_RE.finditer(page):
      yield cls.href_match_to_url(match)


def partition(L, pred):
  return filter(lambda v: not pred(v), L), filter(lambda v: pred(v), L)


class Crawler(object):
  """A multi-threaded crawler that supports local (disk) and remote (web) crawling."""

  # Memoizer for calls to Crawler.crawl().
  _CRAWL_CACHE = Memoizer()

  @classmethod
  def reset_cache(cls):
    """Reset the internal crawl cache. This is intended primarily for tests."""
    cls._CRAWL_CACHE = Memoizer()

  @classmethod
  def crawl_local(cls, link):
    try:
      dirents = os.listdir(link.local_path)
    except OSError as e:
      TRACER.log('Failed to read %s: %s' % (link.local_path, e), V=1)
      return set(), set()
    files, dirs = partition([os.path.join(link.local_path, fn) for fn in dirents], os.path.isdir)
    return set(map(Link.from_filename, files)), set(map(Link.from_filename, dirs))

  @classmethod
  def crawl_remote(cls, context, link):
    try:
      content = context.content(link)
    except context.Error as e:
      TRACER.log('Failed to read %s: %s' % (link.url, e), V=1)
      return set(), set()
    links = set(link.join(href) for href in PageParser.links(content))
    rel_links = set(link.join(href) for href in PageParser.rel_links(content))
    return links, rel_links

  @classmethod
  def crawl_link(cls, context, link):
    if link.local:
      return cls.crawl_local(link)
    elif link.remote:
      return cls.crawl_remote(context, link)
    else:
      TRACER.log('Failed to crawl %s: unknown scheme %s' % (link.url, link.scheme))
      return set(), set()

  def __init__(self, context=None, threads=1):
    self._threads = threads
    self.context = context or Context.get()

  def _make_cache_key(self, links, follow_links):
    return (follow_links,) + tuple(links)

  def crawl(self, link_or_links, follow_links=False):
    links = list(Link.wrap_iterable(link_or_links))
    cache_key = self._make_cache_key(links, follow_links)

    # Memoize crawling to a global Memoizer (Crawler._CRAWL_CACHE).
    result = self._CRAWL_CACHE.get(cache_key)
    if result is None:
      result = self._crawl(links, follow_links)
      self._CRAWL_CACHE.store(cache_key, result)

    return result

  def _crawl(self, link_or_links, follow_links):
    links, seen = set(), set()
    queue = Queue()
    converged = threading.Event()

    def execute():
      while not converged.is_set():
        try:
          link = queue.get(timeout=0.01)
        except Empty:
          continue
        if link not in seen:
          seen.add(link)
          try:
            roots, rels = self.crawl_link(self.context, link)
          except Exception as e:
            TRACER.log('Unknown exception encountered: %s' % e)
            for line in traceback.format_exc().splitlines():
              TRACER.log(line)
            queue.task_done()
            continue
          links.update(roots)
          if follow_links:
            for rel in rels:
              if rel not in seen:
                queue.put(rel)
        queue.task_done()

    for i, link in enumerate(link_or_links):
      TRACER.log('crawling link i=%s link=%s follow_links=%s' % (i, link, follow_links), V=3)
      queue.put(link)

    workers = []
    for _ in range(self._threads):
      worker = threading.Thread(target=execute)
      workers.append(worker)
      worker.daemon = True
      worker.start()

    queue.join()
    converged.set()

    # We deliberately do not join the worker threads, since they are no longer of any use to us.
    return links
