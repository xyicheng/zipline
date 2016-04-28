from collections import namedtuple
import errno
import os
import shutil
import warnings

import click
import pandas as pd
from toolz import curry, complement

from ..us_equity_pricing import (
    BcolzDailyBarReader,
    BcolzDailyBarWriter,
    SQLiteAdjustmentReader,
    SQLiteAdjustmentWriter,
)
from ..minute_bars import (
    BcolzMinuteBarReader,
    BcolzMinuteBarWriter,
)
from zipline.assets import AssetDBWriter, AssetFinder, ASSET_DB_VERSION
from zipline.utils.cache import (
    dataframe_cache,
    transactional_file,
    transactional_dir,
)
from zipline.utils.compat import mappingproxy
from zipline.utils.input_validation import ensure_timestamp, optionally
import zipline.utils.paths as pth
from zipline.utils.preprocess import preprocess
from zipline.utils.tradingcalendar import trading_days, open_and_closes


def asset_db_path(bundle_name, timestr, environ=None):
    return pth.data_path(
        [bundle_name, timestr, 'assets-%d.sqlite' % ASSET_DB_VERSION],
        environ=environ,
    )


def minute_equity_path(bundle_name, timestr, environ=None):
    return pth.data_path(
        [bundle_name, timestr, 'minute_equities.bcolz'],
        environ=environ,
    )


def daily_equity_path(bundle_name, timestr, environ=None):
    return pth.data_path(
        [bundle_name, timestr, 'daily_equities.bcolz'],
        environ=environ,
    )


def adjustment_db_path(bundle_name, timestr, environ=None):
    return pth.data_path(
        [bundle_name, timestr, 'adjustments.sqlite'],
        environ=environ,
    )


def cache_path(bundle_name, timestr, environ=None):
    return pth.data_path(
        [bundle_name, timestr, '.cache'],
        environ=environ,
    )


_BundlePayload = namedtuple(
    '_BundlePayload',
    'calendar opens closes minutes_per_day ingest',
)


class UnknownBundle(click.ClickException, LookupError):
    """Raised if no bundle with the given name was registered.
    """
    exit_code = 1

    def __init__(self, name):
        super(UnknownBundle, self).__init__(
            'No bundle registered with the name %r' % name,
        )
        self.name = name

    def __str__(self):
        return self.message


def _make_bundle_core():
    """Create a family of data bundle functions that read from the same
    bundle mapping.

    Returns
    -------
    bundles : mappingproxy
        The mapping of bundles to bundle payloads.
    register : callable
        The function which registers new bundles in the ``bundles`` mapping.
    unregister : callable
        The function which deregisters bundles from the ``bundles`` mapping.
    ingest_bundle : callable
        The function which downloads and write data for a given data bundle.
    """
    _bundles = {}  # the registered bundles
    # Expose _bundles through a proxy so that users cannot mutate this
    # accidentally. Users may go through `register` to update this which will
    # warn when trampling another bundle.
    bundles = mappingproxy(_bundles)

    @curry
    def register(name,
                 f,
                 calendar=trading_days,
                 opens=open_and_closes['market_open'],
                 closes=open_and_closes['market_close'],
                 minutes_per_day=390):
        """Register a data bundle ingest function.

        Parameters
        ----------
        name : str
            The name of the bundle.
        f : callable
            The ingest function. This function will be passed:

              environ : mapping
                  The environment this is being run with.
              asset_db_writer : AssetDBWriter
                  The asset db writer to write into.
              minute_bar_writer : BcolzMinuteBarWriter
                  The minute bar writer to write into.
              daily_bar_writer : BcolzDailyBarWriter
                  The daily bar writer to write into.
              adjustment_writer : SQLiteAdjustmentWriter
                  The adjustment db writer to write into.
              calendar : pd.DatetimeIndex
                  The trading calendar to ingest for.
              cache : DataFrameCache
                  A mapping object to temporarily store dataframes.
                  This should be used to cache intermediates in case the load
                  fails. This will be automatically cleaned up after a
                  successful load.
              show_progress : bool
                  Show the progress for the current load where possible.
        calendar : pd.DatetimeIndex, optional
            The exchange calendar to align the data to. This defaults to the
            NYSE calendar.
        market_open : pd.DatetimeIndex, optional
            The minute when the market opens each day. This defaults to the
            NYSE calendar.
        market_close : pd.DatetimeIndex, optional
            The minute when the market closes each day. This defaults to the
            NYSE calendar.
        minutes_per_day : int, optional
            The number of minutes in each normal trading day.

        Notes
        -----
        This function my be used as a decorator, for example:

        .. code-block:: python

           @register('quandl')
           def quandl_ingest_function(...):
               ...

        See Also
        --------
        zipline.data.bundles.bundles
        """
        if name in bundles:
            warnings.warn(
                'Overwriting bundle with name %r' % name,
                stacklevel=3,
            )
        _bundles[name] = _BundlePayload(
            calendar,
            opens,
            closes,
            minutes_per_day,
            f,
        )
        return f

    def unregister(name):
        """Unregister a bundle.

        Parameters
        ----------
        name : str
            The name of the bundle to unregister.

        Raises
        ------
        UnknownBundle
            Raised when no bundle has been registered with the given name.

        See Also
        --------
        zipline.data.bundles.bundles
        """
        try:
            del _bundles[name]
        except KeyError:
            raise UnknownBundle(name)

    def ingest(name,
               environ=os.environ,
               timestamp=None,
               show_progress=True):
        """Ingest data for a given bundle.

        Parameters
        ----------
        name : str
            The name of the bundle.
        environ : mapping, optional
            The environment variables. By default this is os.environ.
        timestamp : datetime, optional
            The timestamp to use for the load.
            By default this is the current time.
        show_progress : bool, optional
            Tell the ingest function to display the progress where possible.
        """
        try:
            bundle = bundles[name]
        except KeyError:
            raise UnknownBundle(name)

        if timestamp is None:
            timestamp = pd.Timestamp.utcnow()
        timestamp = timestamp.tz_convert('utc').tz_localize(None)
        timestr = timestamp.isoformat()
        cachepath = cache_path(name, timestr, environ=environ)
        pth.ensure_directory(cachepath)

        with dataframe_cache(cachepath, clean_on_failure=False) as cache, \
                transactional_dir(
                    daily_equity_path(name, timestr, environ=environ),
                ) as daily_bars_dir, \
                transactional_dir(
                    minute_equity_path(name, timestr, environ=environ),
                ) as minute_bars_dir, \
                transactional_file(
                    asset_db_path(name, timestr, environ=environ),
                ) as asset_db_file, \
                transactional_file(
                    adjustment_db_path(name, timestr, environ=environ),
                ) as adjustment_db_file:
            # we use `cleanup_on_failure=False` so that we don't purge the
            # cache directory if the load fails in the middle
            daily_bar_writer = BcolzDailyBarWriter(
                daily_bars_dir.name,
                bundle.calendar,
            )
            daily_bar_writer.write(())
            bundle.ingest(
                environ,
                AssetDBWriter(asset_db_file.name),
                BcolzMinuteBarWriter(
                    bundle.calendar[0],
                    minute_bars_dir.name,
                    bundle.opens,
                    bundle.closes,
                    minutes_per_day=bundle.minutes_per_day,
                ),
                daily_bar_writer,
                SQLiteAdjustmentWriter(
                    adjustment_db_file.name,
                    BcolzDailyBarReader(daily_bars_dir.name),
                    bundle.calendar,
                    overwrite=True,
                ),
                bundle.calendar,
                cache,
                show_progress,
            )

    return bundles, register, unregister, ingest


bundles, register, unregister, ingest = _make_bundle_core()

BundleData = namedtuple(
    'BundleData',
    'asset_finder minute_bar_reader daily_bar_reader adjustment_reader',
)


def most_recent_data(bundle_name, timestamp, environ=None):
    """Get the path to the most recent data after ``date``for the given bundle.

    Parameters
    ----------
    bundle_name : str
        The name of the bundle to lookup.
    timestamp : datetime
        The timestamp to begin searching on or before.
    environ : dict, optional
        An environment dict to forward to zipline_root.
    """
    try:
        return pth.data_path(
            [bundle_name,
             max(
                 filter(
                     complement(pth.hidden),
                     os.listdir(pth.data_path([bundle_name], environ=environ)),
                 ),
                 key=pd.Timestamp,
             )],
            environ=environ,
        )
    except ValueError:
        raise ValueError(
            'no data for bundle %r on or before %s' % (
                bundle_name,
                timestamp,
            ),
        )
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise
        raise UnknownBundle(bundle_name)


def load(name, environ=os.environ, timestamp=None):
    """Loads a previously ingested bundle.

    Parameters
    ----------
    name : str
        The name of the bundle.
    environ : mapping, optional
        The environment variables. Defaults of os.environ.
    timestamp : datetime, optional
        The timestamp of the data to lookup.
        Defaults to the current time.

    Returns
    -------
    bundle_data : BundleData
        The raw data readers for this bundle.
    """
    if timestamp is None:
        timestamp = pd.Timestamp.utcnow()
    timestr = most_recent_data(name, timestamp, environ=environ)
    return BundleData(
        asset_finder=AssetFinder(
            asset_db_path(name, timestr, environ=environ),
        ),
        minute_bar_reader=BcolzMinuteBarReader(
            minute_equity_path(name, timestr,  environ=environ),
        ),
        daily_bar_reader=BcolzDailyBarReader(
            daily_equity_path(name, timestr, environ=environ),
        ),
        adjustment_reader=SQLiteAdjustmentReader(
            adjustment_db_path(name, timestr, environ=environ),
        ),
    )


class BadClean(click.ClickException, ValueError):
    """Exception indicating that an invalid argument set was passed to
    ``clean``.

    Parameters
    ----------
    before, after, keep_last : any
        The bad arguments to ``clean``.

    See Also
    --------
    clean
    """
    def __init__(self, before, after, keep_last):
        super(BadClean, self).__init__(
            'Cannot pass a combination of `before` and `after` with'
            '`keep_last`. Got: before=%r, after=%r, keep_n=%r\n' % (
                before,
                after,
                keep_last,
            ),
        )

    def __str__(self):
        return self.message


@preprocess(
    before=optionally(ensure_timestamp),
    after=optionally(ensure_timestamp),
)
def clean(name, before=None, after=None, keep_last=None, environ=os.environ):
    """Clean up data that was created with ``ingest`` or
    ``$ python -m zipline ingest``

    Parameters
    ----------
    name : str
        The name of the bundle to remove data for.
    before : datetime, optional
        Remove data ingested before this date.
        This argument is mutually exclusive with: keep_last
    after : datetime, optional
        Remove data ingested after this date.
        This argument is mutually exclusive with: keep_last
    keep_last : int, optional
        Remove all but the last ``keep_last`` ingestions.
        This argument is mutually exclusive with:
          before
          after

    Returns
    -------
    cleaned : set[str]
        The names of the runs that were removed.

    Raises
    ------
    BadClean
        Raised when ``before`` and or ``after`` are passed with ``keep_last``.
        This is a subclass of ``ValueError``.
    """
    try:
        all_runs = sorted(
            pd.Timestamp(f)
            for f in os.listdir(pth.data_path([name], environ=environ))
            if not pth.hidden(f)
        )
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise
        raise UnknownBundle(name)

    if (before is not None or after is not None) and keep_last is not None:
        raise BadClean(before, after, keep_last)

    if keep_last is None:
        def in_last_n(dt):
            return False
    else:
        last_n_dts = set(all_runs[:keep_last])

        def in_last_n(dt):
            return dt in last_n_dts

    def should_clean(name):
        dt = pd.Timestamp(name)

        return (
            (
                (before is not None and dt < before) or
                (after is not None and dt > after)
            ) and
            not in_last_n(dt)
        )

    cleaned = set()
    for run in all_runs:
        if should_clean(run):
            shutil.rmdir(run)
            cleaned.add(run)

    return cleaned
