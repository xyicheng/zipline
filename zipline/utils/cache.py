"""
Caching utilities for zipline
"""
from collections import namedtuple, MutableMapping
import errno
import os
import pickle
from shutil import rmtree, copyfile, copytree
from tempfile import mkdtemp, NamedTemporaryFile

import pandas as pd

from .context_tricks import nop_context
from .input_validation import expect_element
from .paths import ensure_directory


class Expired(Exception):
    """Marks that a :class:`CachedObject` has expired.
    """


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
    """
    A cache of multiple CachedObjects, which returns the wrapped the value
    or raises and deletes the CachedObject if the value has expired.

    Parameters
    ----------
    cache : dict-like, optional
        An instance of a dict-like object which needs to support at least:
        `__del__`, `__getitem__`, `__setitem__`
        If `None`, than a dict is used as a default.

    Usage
    -----
    >>> from pandas import Timestamp, Timedelta
    >>> expires = Timestamp('2014', tz='UTC')
    >>> value = 1
    >>> cache = ExpiringCache()
    >>> cache.set('foo', value, expires)
    >>> cache.get('foo', expires - Timedelta('1 minute'))
    1
    >>> cache.get('foo', expires + Timedelta('1 minute'))
    Traceback (most recent call last):
        ...
    KeyError: 'foo'
    """

    def __init__(self, cache=None):
        if cache is not None:
            self._cache = cache
        else:
            self._cache = {}

    def get(self, key, dt):
        """Get the value of a cached object.

        Parameters
        ----------
        key : any
            The key to lookup.
        dt : datetime
            The time of the lookup.

        Returns
        -------
        result : any
            The value for ``key``.

        Raises
        ------
        KeyError
            Raised if the key is not in the cache or the value for the key
            has expired.
        """
        try:
            return self._cache[key].unwrap(dt)
        except Expired:
            del self._cache[key]
            raise KeyError(key)

    def set(self, key, value, expiration_dt):
        """Adds a new key value pair to the cache.

        Parameters
        ----------
        key : any
            The key to use for the pair.
        value : any
            The value to store under the name ``key``.
        expiration_dt : datetime
            When should this mapping expire? The cache is considered invalid
            for dates **strictly greater** than ``expiration_dt``.
        """
        self._cache[key] = CachedObject(value, expiration_dt)


class dataframe_cache(MutableMapping):
    """A disk-backed cache for dataframes.

    This object may be used as a context manager to delete the cache directory
    on exit.

    Parameters
    ----------
    path : str, optional
        The directory path to the cache. Files will be written as
        ``path/<keyname>``.
    lock : Lock, optional
        Thread lock for multithreaded/multiprocessed access to the cache.
        If not provided no locking will be used.
    clean_on_failure : bool, optional
        Should the directory be cleaned up if an exception is raised in the
        context manager.
    serialize : {'msgpack', 'pickle'}, optional
        How should the data be serialized.

    Notes
    -----
    The cache uses a temporary file format that is subject to change between
    versions of zipline.
    """
    @expect_element(serialization={'msgpack', 'pickle'})
    def __init__(self,
                 path=None,
                 lock=None,
                 clean_on_failure=True,
                 serialization='msgpack'):
        self.path = path if path is not None else mkdtemp()
        self.lock = lock if lock is not None else nop_context
        self.clean_on_failure = clean_on_failure

        if serialization == 'msgpack':
            self.serialize = pd.DataFrame.to_msgpack
            self.deserialize = pd.read_msgpack
        else:
            self.serialize = pd.DataFrame.to_pickle
            self.deserialize = self._deserialize_pickle

        ensure_directory(self.path)

    def _deserialize_pickle(self, path):
        with open(path, 'rb') as f:
            return pickle.load(f)

    def _keypath(self, key):
        return os.path.join(self.path, key)

    def __enter__(self):
        return self

    def __exit__(self, type_, value, tb):
        if not (self.clean_on_failure or value is None):
            # we are not cleaning up after a failure and there was an exception
            return

        with self.lock:
            rmtree(self.path)

    def __getitem__(self, key):
        with self.lock:
            try:
                return self.deserialize(self._keypath(key))
            except UnboundLocalError:
                # This is how pandas fails if the file doesn't exist! #pandas
                raise KeyError(key)

    def __setitem__(self, key, value):
        with self.lock:
            try:
                del self[key]
            except KeyError:
                pass

            self.serialize(value, self._keypath(key))

    def __delitem__(self, key):
        with self.lock:
            try:
                os.remove(self._keypath(key))
            except OSError as e:
                if e.errno == errno.ENOENT:
                    # raise a keyerror if this directory did not exist
                    raise KeyError(key)
                # reraise the actual oserror otherwise
                raise

    def __iter__(self):
        return iter(os.listdir(self.path))

    def __len__(self):
        return len(os.listdir(self.path))


class transactional_file(object):
    """A context manager for managing a temporary file that will be moved
    to a non-temporary location if no exceptions are raised in the context.

    Parameters
    ----------
    final_path : str
        The location to move the file when committing.
    *args, **kwargs
        Forwarded to NamedTemporaryFile.

    Notes
    -----
    The file is committed on __exit__ if there are no exceptions.
    """
    def __init__(self, final_path, *args, **kwargs):
        self._tmpfile = NamedTemporaryFile(*args, **kwargs)
        self._final_path = final_path

    def commit(self):
        """Sync the temporary file to the final path.
        """
        copyfile(self.name, self._final_path)

    def __getattr__(self, attr):
        return getattr(self._tmpfile, attr)

    def __enter__(self):
        self._tmpfile.__enter__()
        return self

    def __exit__(self, *exc_info):
        if exc_info[0] is None:
            self.commit()
        self._tmpfile.__exit__(*exc_info)


class transactional_dir(object):
    """A context manager for managing a temporary directory that will be moved
    to a non-temporary location if no exceptions are raised in the context.

    Parameters
    ----------
    final_path : str
        The location to move the file when committing.
    *args, **kwargs
        Forwarded to tmp_dir.

    Notes
    -----
    The file is committed on __exit__ if there are no exceptions.
    """
    def __init__(self, final_path, *args, **kwargs):
        self.name = mkdtemp()
        self._final_path = final_path

    def commit(self):
        """Sync the temporary directory to the final path.
        """
        copytree(self.name, self._final_path)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        if exc_info[0] is None:
            self.commit()
        rmtree(self.name)
