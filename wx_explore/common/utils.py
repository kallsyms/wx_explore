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

    def get_any(self, r: range):
        """
        Returns the first object in the dictionary which has a key contained in the given range.
        """
        for i in r:
            try:
                return self[i]
            except KeyError:
                continue

        return None
