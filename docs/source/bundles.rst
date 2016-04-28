Data Bundles
------------

A data bundle is a collection of pricing data, adjustment data, and an asset
database. Bundles allow us to preload all of the data we will need to run
backtests and store the data for future runs.

Ingesting Data
~~~~~~~~~~~~~~

The first step to using a data bundle is to ingest the data. This will invoke
some custom bundle command and then write the data to a standard location that
zipline can find. By default this location is ``$ZIPLINE_ROOT/data/<bundle>``
where by default ``ZIPLINE_ROOT=~/.zipline``. This step may take some time as it
could involve downloading and processing a lot of data. This can be run with:

.. code-block:: bash

   $ python -m zipline ingest <bundle>


where ``<bundle>`` is the name of the bundle to ingest.

Old Data
~~~~~~~~

When the ``ingest`` command is used it will write the new data to a subdirectory
of ``$ZIPLINE_ROOT/data/<bundle>`` which is named with the current date. This
makes it possible to look at older data or even run backtests with this older
copy. This makers it easier to reproduce backtest results later.

One drawback of saving all of this data by default is that the data directory
may grow quite large even if you do not want to use the data. To solve this
problem there is another command ``clean`` which will clear data bundles based
on some time constraints.

For example:

.. code-block:: bash

   # clean everything older than <date>
   $ python -m zipline clean <bundle> --before <date>

   # clean everything newer than <date>
   $ python -m zipline clean <bundle> --after <date>

   # keep everything in the range of [before, after] and delete the rest
   $ python -m zipline clean <bundle> --before <date> --after <after>

   # clean all but the last <int> runs
   $ python -m zipline clean <bundle> --keep-last <int>


Running Backtests with Data Bundles
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Now that the data has been ingested we can use it to run backtests with the
``run`` command. This can be specified with the ``--bundle`` option like:

.. code-block:: bash

   $ python -m zipline run --bundle <bundle> --algofile algo.py ...


We may also specify the date to use to look up the bundle data with the
``--bundle-date`` option. This will cause us to the the most recent bundle
ingestion that is less than or equal to the ``bundle-date``. This is how we can
run backtests with older data. The reason that this uses a less than or equal to
relationship is that we can specify the date that we ran an old backtest and get
the same data that would have been available to us on that date. the
``bundle-date`` defaults to the current day to use the most recent data.

Default Data Bundles
~~~~~~~~~~~~~~~~~~~~

.. _quandl-data-bundle:

Quandl WIKI Bundle
``````````````````

By default zipline comes with the ``quandl`` data bundle which uses quandl's
`WIKI dataset <https://www.quandl.com/data/WIKI>`_. This bundle includes daily
pricing data, splits, cash dividends, and asset metadata. This is the bundle
that ``run`` will use by default if no other bundle is specified. To ingest this
data bundle we recommend creating an account on quandl.com to get an API key to
be able to make more API requests. Once we have an API key we may run:

.. code-block:: bash

   $ QUANDL_API_KEY=<api-key> python -m zipline ingest quandl

though we may still run ``ingest`` as an anonymous quandl user (with no API
key). We may also set the ``QUANDL_DOWNLOAD_ATTEMPTS`` environment variable to
an integer which is the number of attempts that should be made to download data
from quandls servers. By default this will be 5, meaning that we will retry each
attempt 5 times.

.. note::

   This is not the total number of allowed failures, just the number of allowed
   failures per request. This loader will make one request per 100 equities for
   the metadata followed by one request per equity.


Yahoo Bundle Factories
``````````````````````

Zipline also ships with a factory function for creating a data bundle out of a
set of tickers from yahoo: :func:`~zipline.data.bundles.yahoo_equities`.
This makes it easy to pre-download and cache the data for a set of equities from
yahoo. This includes daily pricing data along with splits, cash dividends, and
inferred asset metadata. To create a bundle from a set of equities, add the
following to your ``~/.zipline/extensions.py`` file:

.. code-block:: python

   from zipline.bundles import register, yahoo_equities

   # these are the tickers you would like data for
   equities = {
       'AAPL',
       'MSFT',
       'GOOG',
   }
   register(
       'my-yahoo-equities-bundle',  # name this whatever you like
       equities,
   )


This may now be used like:

.. code-block:: bash

   $ python -m zipline ingest my-yahoo-equities-bundle
   $ python -m zipline run -f algo.py --bundle my-yahoo-equities-bundle


More than one yahoo equities bundle may be registered as long as they use
different names.

Writing a New Bundle
~~~~~~~~~~~~~~~~~~~~

Data bundles exist to make it easy to use different data sources with
zipline. To add a new bundle, one must implement an ingest function.

This function is responsible for loading the data into memory and passing it to
a set of writer objects provided by zipline to convert the data to zipline's
internal format. The ingest function may work by downloading data from a remote
location like the ``quandl`` bundle or yahoo bundles or it may just load files
that are already on the machine. The function is provided with writers that will
write the data to the correct location transactionally. If an ingestion fails
part way through the bundle will not be written in an incomplete state.

The signature of the ingest function should be:

.. code-block:: python

   ingest(environ,
          asset_db_writer,
          minute_bar_writer,
          daily_bar_writer,
          adjustment_writer,
          calendar,
          cache,
          show_progress)

``environ``
```````````

``environ`` is a mapping representing the environment variables to use. This is
where any custom arguments needed for the ingestion should be passed, for
example: the ``quandl`` bundle uses the enviornment to pass the API key and the
download retry attempt count.

``asset_db_writer``
```````````````````

``asset_db_writer`` is an instance of :class:`~zipline.assets.AssetDBWriter`.
This is the writer for the asset metadata which provides the asset lifetimes and
the symbol to asset id (sid) mapping. This may also contain the asset name,
exchange and a few other columns. To write data, invoke
:meth:`~zipline.assets.AssetDBWriter.write`. This is passed dataframes for the
various pieces of metadata, more information about the format of the data exists
in the docs for write.

``minute_bar_writer``
`````````````````````

``minute_bar_writer`` is an instance of
:class:`~zipline.data.minute_bars.BcolzMinuteBarWriter`. This writer is used to
convert data to zipline's internal bcolz format to later be read by a
:class:`~zipline.data.minute_bars.BcolzMinuteBarReader`. If minute data is
provided, users should call
:meth:`~zipline.data.minute_bars.BcolzMinuteBarWriter.write` with an iterable of
(sid, dataframe) tuples. The ``show_progress`` argument should also be forwarded
to this method. If the data source does not provide minute level data, then
there is no need to call the write method. It is also acceptable to pass an
empty iterator to :meth:`~zipline.data.minute_bars.BcolzMinuteBarWriter.write`
to signal that there is no minutely data.

.. note::

   The data passed to
   :meth:`~zipline.data.minute_bars.BcolzMinuteBarWriter.write` may be a lazy
   iterator or generator to avoid loading all of the minute data into memory at
   a single time. A given sid may also appear multiple times in the data as long
   as the dates are strictly increasing.

``daily_bar_writer``
````````````````````

``daily_bar_writer`` is an instance of
:class:`~zipline.data.us_equity_pricing.BcolzDailyBarWriter`. This writer is
used to convert data into zipline's internal bcolz format to later be read by a
:class:`~zipline.data.us_equity_pricing.BcolzDailyBarReader`. If daily data is
provided, users should call
:meth:`~zipline.data.minute_bars.BcolzDailyBarWriter.write` with an iterable of
(sid dataframe) tuples. The ``show_progress`` argument should also be forwarded
to this method. If the data shource does not provide daily data, then there is
no need to call the write method. It is also acceptable to pass an empty
iterable to :meth:`~zipline.data.minute_bars.BcolzMinuteBarWriter.write` to
signal that there is no daily data. If no daily data is provided but minute data
is provided, a daily rollup will happen to service daily history requests.

.. note::

   Like the ``minute_bar_writer``, the data passed to
   :meth:`~zipline.data.minute_bars.BcolzMinuteBarWriter.write` may be a lazy
   iterable or generator to avoid loading all of the data into memory at once.
   Unlike the ``minute_bar_writer``, a sid may only appear once in the data
   iterable.

``adjustment_writer``
`````````````````````

``adjustment_writer`` is an instance of
:class:`~zipline.data.us_equity_pricing.SQLiteAdjustmentWriter`. This writer is
used to store splits, mergers, dividends, and stock dividends. The data should
be provided as dataframes and passed to
:meth:`~zipline.data.us_equity_pricing.SQLiteAdjustmentWriter.write`. Each of
these fields are optional, but the writer can accept as much of the data as you
have.

``calendar``
````````````

``calendar`` is a ``pandas.DatetimeIndex`` object holding all of the trading
days that the bundle should load data for. This is to help some bundles generate
queries for the days needed.

``cache``
`````````

``cache`` is an instance of :class:`~zipline.utils.cache.dataframe_cache`. This
object is a mapping from strings to dataframes. This object is provided in case
an ingestion crashes part way through. The idea is that as the ingest function
should check the cache for raw data, if it doesn't exist in the cache, it should
acquire it and then store it in the cache. Then it can parse and write the
data. The cache will be cleared only after a successful load, this prevents the
ingest function from needing to redownload all the data if there is some bug in
the parsing. If it is very fast to get the data, for example if it is coming
from another local file, then there is no need to use this cache.

``show_progress``
`````````````````

``show_progress`` is a boolean indicating that the user would like to receive
feedback about the ingest function's progress fetching and writing the
data. Some examples for where to show how many files you have downloaded out of
the total needed, or how far into some data conversion the ingest function
is. One tool that may help with implementing ``show_progress`` for a loop is
:class:`~zipline.utils.cli.maybe_show_progress`. This argument should always be
forwarded to ``minute_bar_writer.write`` and ``daily_bar_writer.write``.
