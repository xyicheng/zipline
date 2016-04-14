#
# Copyright 2015 Quantopian, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logbook
import datetime

from six import string_types
from sqlalchemy import create_engine

from zipline.data.loader import load_market_data
from zipline.assets import AssetFinder
from zipline.assets.asset_writer import (
    AssetDBWriterFromList,
    AssetDBWriterFromDictionary,
    AssetDBWriterFromDataFrame)
from zipline.utils.calendars import default_nyse_schedule

log = logbook.Logger('Trading')

# The financial simulations in zipline depend on information
# about the benchmark index and the risk free rates of return.
# The benchmark index defines the benchmark returns used in
# the calculation of performance metrics such as alpha/beta. Many
# components, including risk, performance, transforms, and
# batch_transforms, need access to a calendar of trading days and
# market hours. The TradingEnvironment maintains two time keeping
# facilities:
#   - a DatetimeIndex of trading days for calendar calculations
#   - a timezone name, which should be local to the exchange
#   hosting the benchmark index. All dates are normalized to UTC
#   for serialization and storage, and the timezone is used to
#   ensure proper rollover through daylight savings and so on.
#
# User code will not normally need to use TradingEnvironment
# directly. If you are extending zipline's core financial
# components and need to use the environment, you must import the module and
# build a new TradingEnvironment object, then pass that TradingEnvironment as
# the 'env' arg to your TradingAlgorithm.


class TradingEnvironment(object):

    # Token used as a substitute for pickling objects that contain a
    # reference to a TradingEnvironment
    PERSISTENT_TOKEN = "<TradingEnvironment>"

    def __init__(
        self,
        load=None,
        bm_symbol='^GSPC',
        exchange_tz="US/Eastern",
        trading_schedule=default_nyse_schedule,
        asset_db_path=':memory:'
    ):
        """
        @load is function that returns benchmark_returns and treasury_curves
        The treasury_curves are expected to be a DataFrame with an index of
        dates and columns of the curve names, e.g. '10year', '1month', etc.
        """

        self.bm_symbol = bm_symbol
        if not load:
            load = load_market_data

        self.benchmark_returns, self.treasury_curves = load(
            trading_schedule.day,
            trading_schedule.schedule.index,
            self.bm_symbol,
        )

        self.exchange_tz = exchange_tz

        if isinstance(asset_db_path, string_types):
            asset_db_path = 'sqlite:///%s' % asset_db_path
            self.engine = engine = create_engine(asset_db_path)
            AssetDBWriterFromDictionary().init_db(engine)
        else:
            self.engine = engine = asset_db_path

        if engine is not None:
            self.asset_finder = AssetFinder(engine)
        else:
            self.asset_finder = None

    def write_data(self,
                   engine=None,
                   equities_data=None,
                   futures_data=None,
                   exchanges_data=None,
                   root_symbols_data=None,
                   equities_df=None,
                   futures_df=None,
                   exchanges_df=None,
                   root_symbols_df=None,
                   equities_identifiers=None,
                   futures_identifiers=None,
                   exchanges_identifiers=None,
                   root_symbols_identifiers=None,
                   allow_sid_assignment=True):
        """ Write the supplied data to the database.

        Parameters
        ----------
        equities_data: dict, optional
            A dictionary of equity metadata
        futures_data: dict, optional
            A dictionary of futures metadata
        exchanges_data: dict, optional
            A dictionary of exchanges metadata
        root_symbols_data: dict, optional
            A dictionary of root symbols metadata
        equities_df: pandas.DataFrame, optional
            A pandas.DataFrame of equity metadata
        futures_df: pandas.DataFrame, optional
            A pandas.DataFrame of futures metadata
        exchanges_df: pandas.DataFrame, optional
            A pandas.DataFrame of exchanges metadata
        root_symbols_df: pandas.DataFrame, optional
            A pandas.DataFrame of root symbols metadata
        equities_identifiers: list, optional
            A list of equities identifiers (sids, symbols, Assets)
        futures_identifiers: list, optional
            A list of futures identifiers (sids, symbols, Assets)
        exchanges_identifiers: list, optional
            A list of exchanges identifiers (ids or names)
        root_symbols_identifiers: list, optional
            A list of root symbols identifiers (ids or symbols)
        """
        if engine:
            self.engine = engine

        # If any pandas.DataFrame data has been provided,
        # write it to the database.
        has_rows = lambda df: df is not None and len(df) > 0
        if any(map(has_rows, [equities_df,
                              futures_df,
                              exchanges_df,
                              root_symbols_df])):
            self._write_data_dataframes(
                equities=equities_df,
                futures=futures_df,
                exchanges=exchanges_df,
                root_symbols=root_symbols_df,
            )

        # Same for dicts.
        has_data = lambda d: d is not None and len(d) > 0
        if any(map(has_data, [equities_data,
                              futures_data,
                              exchanges_data,
                              futures_data])):
            self._write_data_dicts(
                equities=equities_data,
                futures=futures_data,
                exchanges=exchanges_data,
                root_symbols=root_symbols_data
            )

        # Same for iterables.
        if any(map(has_data, [equities_identifiers,
                              futures_identifiers,
                              exchanges_identifiers,
                              root_symbols_identifiers])):
            self._write_data_lists(
                equities=equities_identifiers,
                futures=futures_identifiers,
                exchanges=exchanges_identifiers,
                root_symbols=root_symbols_identifiers,
                allow_sid_assignment=allow_sid_assignment
            )

    def _write_data_lists(self, equities=None, futures=None, exchanges=None,
                          root_symbols=None, allow_sid_assignment=True):
        AssetDBWriterFromList(equities, futures, exchanges, root_symbols)\
            .write_all(self.engine, allow_sid_assignment=allow_sid_assignment)

    def _write_data_dicts(self, equities=None, futures=None, exchanges=None,
                          root_symbols=None):
        AssetDBWriterFromDictionary(equities, futures, exchanges, root_symbols)\
            .write_all(self.engine)

    def _write_data_dataframes(self, equities=None, futures=None,
                               exchanges=None, root_symbols=None):
        AssetDBWriterFromDataFrame(equities, futures, exchanges, root_symbols)\
            .write_all(self.engine)


class SimulationParameters(object):
    def __init__(self, period_start, period_end,
                 capital_base=10e3,
                 emission_rate='daily',
                 data_frequency='daily',
                 trading_schedule=None):

        self.period_start = period_start
        self.period_end = period_end
        self.capital_base = capital_base

        self.emission_rate = emission_rate
        self.data_frequency = data_frequency

        # copied to algorithm's environment for runtime access
        self.arena = 'backtest'

        if trading_schedule is not None:
            self.update_internal_from_trading_schedule(
                trading_schedule=trading_schedule
            )

    def update_internal_from_trading_schedule(self, trading_schedule):

        assert self.period_start <= self.period_end, \
            "Period start falls after period end."

        assert self.period_start <= trading_schedule.last_execution_day, \
            "Period start falls after the last known trading day."
        assert self.period_end >= trading_schedule.first_execution_day, \
            "Period end falls before the first known trading day."

        self.first_open = self._calculate_first_open(trading_schedule)
        self.last_close = self._calculate_last_close(trading_schedule)

        # Take the length of an inclusive slice of trading dates
        self.trading_days = trading_schedule.trading_dates(
            self.first_open, self.last_close
        )
        self.days_in_period = len(self.trading_days)

    def _calculate_first_open(self, trading_schedule):
        """
        Finds the first trading day on or after self.period_start.
        """
        first_open = self.period_start
        one_day = datetime.timedelta(days=1)

        while not trading_schedule.is_executing_on_day(first_open):
            first_open = first_open + one_day

        mkt_open, _ = trading_schedule.start_and_end(first_open)
        return mkt_open

    def _calculate_last_close(self, trading_schedule):
        """
        Finds the last trading day on or before self.period_end
        """
        last_close = self.period_end
        one_day = datetime.timedelta(days=1)

        while not trading_schedule.is_executing_on_day(last_close):
            last_close = last_close - one_day

        _, mkt_close = trading_schedule.start_and_end(last_close)
        return mkt_close

    def __repr__(self):
        return """
{class_name}(
    period_start={period_start},
    period_end={period_end},
    capital_base={capital_base},
    data_frequency={data_frequency},
    emission_rate={emission_rate},
    first_open={first_open},
    last_close={last_close})\
""".format(class_name=self.__class__.__name__,
           period_start=self.period_start,
           period_end=self.period_end,
           capital_base=self.capital_base,
           data_frequency=self.data_frequency,
           emission_rate=self.emission_rate,
           first_open=self.first_open,
           last_close=self.last_close)


def noop_load(*args, **kwargs):
    """
    A method that can be substituted in as the load method in a
    TradingEnvironment to prevent it from loading benchmarks.

    Accepts any arguments, but returns only a tuple of Nones regardless
    of input.
    """
    return None, None
