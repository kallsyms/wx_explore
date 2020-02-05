import collections
import datetime
import functools
import logging
import requests
import time


logger = logging.getLogger(__name__)


def datetime2unix(dt: datetime.datetime) -> int:
    """
    Convert the given datetime `dt` to an integer unix timestamp.
    If `dt` has no tz associated with it, it is assumed to be utc.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return int(dt.timestamp())


def get_url(url, headers=None, retries=3):
    if headers is None:
        headers = {}

    for i in range(retries):
        try:
            r = requests.get(url, headers=headers)
            break
        except KeyboardInterrupt:
            raise
        except Exception:
            logger.exception("Exception while GETing %s. Retrying...", url)
            time.sleep(3**i)
            continue
    else:
        raise Exception(f"Unable to download file after {retries} retries!")

    if not (200 <= r.status_code < 300):
        raise Exception(f"Unable to download file: unexpected status code {r.status_code}")

    return r


def url_exists(url):
    r = requests.head(url)
    return 200 <= r.status_code < 300


class memoize(object):
   '''
   https://wiki.python.org/moin/PythonDecoratorLibrary#Memoize
   Decorator. Caches a function's return value each time it is called.
   If called later with the same arguments, the cached value is returned
   (not reevaluated).
   '''
   def __init__(self, func):
      self.func = func
      self.cache = {}
   def __call__(self, *args):
      if not isinstance(args, collections.Hashable):
         # uncacheable. a list, for instance.
         # better to not cache than blow up.
         return self.func(*args)
      if args in self.cache:
         return self.cache[args]
      else:
         value = self.func(*args)
         self.cache[args] = value
         return value
   def __repr__(self):
      '''Return the function's docstring.'''
      return self.func.__doc__
   def __get__(self, obj, objtype):
      '''Support instance methods.'''
      return functools.partial(self.__call__, obj)


class RangeDict(dict):
    def __getitem__(self, item):
        try:
            return super().__getitem__(item)
        except KeyError:
            if isinstance(item, (int, float)):
                for k, v in self.items():
                    if type(k) is range and k.start <= item < k.stop:
                        return v
            raise


class ContinuousTimeList(list):
    start: datetime.datetime
    end: datetime.datetime
    step: datetime.timedelta

    def __init__(self, start: datetime.datetime, end: datetime.datetime, step: datetime.timedelta, vals=None):
        self.start = start
        self.end = end
        self.step = step

        list_len = int((end-start)/step)
        if vals is None:
            vals = [None] * list_len
        if len(vals) != list_len:
            raise ValueError(f"Initial values array must have expected length (expected {list_len} got {len(vals)})")

        super().__init__(vals)

    def _idx_for_dt(self, dt: datetime.datetime) -> int:
        return int((dt - self.start).total_seconds() // self.step.total_seconds())

    def __getitem__(self, key):
        if isinstance(key, int):
            return super().__getitem__(key)
        elif isinstance(key, datetime.datetime):
            return super().__getitem__(self._idx_for_dt(key))
        elif isinstance(key, slice):
            if isinstance(key.start, datetime.datetime):
                key = slice(self._idx_for_dt(key.start), key.stop, key.step)
            if isinstance(key.stop, datetime.datetime):
                key = slice(key.start, self._idx_for_dt(key.stop), key.step)
            return super().__getitem__(key)
        else:
            raise TypeError("index must be int, datetime, or slice")

    def __setitem__(self, key, val):
        if isinstance(key, int):
            super().__setitem__(key, val)
        elif isinstance(key, datetime.datetime):
            super().__setitem__(self._idx_for_dt(key), val)
        elif isinstance(key, slice):
            if isinstance(key.start, datetime.datetime):
                key = slice(self._idx_for_dt(key.start), key.stop, key.step)
            if isinstance(key.stop, datetime.datetime):
                key = slice(key.start, self._idx_for_dt(key.stop), key.step)
            return super().__setitem__(key, val)
        else:
            TypeError("index must be int or datetime")
