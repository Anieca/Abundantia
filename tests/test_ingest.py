import pandas as pd
import pytest

from abundantia.adapters import GMOCoinClient, SQLiteClient
from abundantia.adapters.exchanges.bitflyer_client import BitFlyerClient


@pytest.mark.integration
def test_ingest_klines_from_gmocoin_executions():
    gmo = GMOCoinClient()
    sqlite = SQLiteClient()

    interval = 60
    symbol = GMOCoinClient.symbols.BTC_JPY

    executions = gmo.get_executions_by_http(symbol, max_executions=300)
    klines: pd.DataFrame = gmo.convert_executions_to_common_klines(symbol, interval, executions, inclusive="neither")

    sqlite.drop_common_kline_table()
    sqlite.create_tables()
    sqlite.insert_common_klines(klines)


@pytest.mark.integration
def test_ingest_klines_from_bitflyer_executions():
    bitflyer = BitFlyerClient()
    sqlite = SQLiteClient()

    interval = 60
    symbol = BitFlyerClient.symbols.FX_BTC_JPY

    executions = bitflyer.get_executions_by_http(symbol, max_executions=300)
    klines: pd.DataFrame = bitflyer.convert_executions_to_common_klines(symbol, interval, executions)

    sqlite.insert_common_klines(klines)
