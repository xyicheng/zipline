"""
Cached object with an expiration date.
"""
from collections import namedtuple


class Expired(Exception):
    pass


class CachedObject(namedtuple("_CachedObject", "value expires")):
    """
    A simple struct for maintaining a cached object with an expiration date.

    Parameters
    ----------
    value : object
        The object to cache.
    expires : datetime-like
        Expiration date of `value`. The cache is considered invalid for dates
        **strictly greater** than `expires`.

    Methods
    -------
    get(self, dt)
        Get the cached object.

    Usage
    -----
    >>> from pandas import Timestamp, Timedelta
    >>> expires = Timestamp('2014', tz='UTC')
    >>> obj = CachedObject(1, expires)
    >>> obj.unwrap(expires - Timedelta('1 minute'))
    1
    >>> obj.unwrap(expires)
    1
    >>> obj.unwrap(expires + Timedelta('1 minute'))
    Traceback (most recent call last):
        ...
    Expired: 2014-01-01 00:00:00+00:00
    """

    def unwrap(self, dt):
        """
        Get the cached value.

        Returns
        -------
        value : object
            The cached value.

        Raises
        ------
        Expired
            Raised when `dt` is greater than self.expires.
        """
        if dt > self.expires:
            raise Expired(self.expires)
        return self.value


class ExpiringCache(object):

    def __init__(self, cache=None):
        if cache is not None:
            self._cache = cache
        else:
            self._cache = {}

    def get(self, key, ref_date):
        try:
            return self._cache[key].unwrap(ref_date)
        except Expired:
            del self._cache[key]
            raise KeyError(key)

    def set(self, key, value, ref_date):
        self._cache[key] = CachedObject(value, ref_date)
