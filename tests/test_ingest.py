from datetime import datetime

import pandas as pd
import pytest

from abundantia.adapters.databases.sqlite_client import SQLiteClient
from abundantia.adapters.exchanges.bitflyer_client import BitFlyerClient
from abundantia.adapters.exchanges.gmocoin_client import GMOCoinClient


@pytest.mark.integration
def test_ingest_klines_from_gmocoin_executions():
    gmo = GMOCoinClient()
    sqlite = SQLiteClient()

    interval = 60
    symbol = GMOCoinClient.SYMBOLS.BTC_JPY

    executions = gmo.get_executions_by_http(symbol, max_executions=300)
    klines: pd.DataFrame = gmo.convert_executions_to_common_klines(symbol, interval, executions, inclusive="neither")

    sqlite.create_tables()
    sqlite.insert_common_klines(klines)
    sqlite.drop_common_kline_table()


@pytest.mark.integration
def test_ingest_klines_from_bitflyer_executions():
    bitflyer = BitFlyerClient()
    sqlite = SQLiteClient()

    interval = 60
    symbol = BitFlyerClient.SYMBOLS.FX_BTC_JPY

    executions = bitflyer.get_executions_by_http(symbol, max_executions=300)
    start_date = datetime(2022, 5, 5)
    end_date = datetime(2022, 5, 6)
    klines: pd.DataFrame = bitflyer.convert_executions_to_common_klines(
        symbol, interval, start_date, end_date, executions
    )

    sqlite.create_tables()
    sqlite.insert_common_klines(klines)
    sqlite.drop_common_kline_table()
