import pandas as pd

from abundantia.adaptors import GMOCoinClient, SQLiteClient
from abundantia.adaptors.exchanges.bitflyer_client import BitFlyerClient


def test_ingest_duplicate_klines():
    gmo = GMOCoinClient()
    sqlite = SQLiteClient()

    interval = 60
    symbol = GMOCoinClient.symbols.BTC_JPY

    executions = gmo.get_executions_by_http(symbol, max_executions=300)
    klines: pd.DataFrame = gmo.convert_executions_to_common_klines(symbol, executions, interval, inclusive="neither")

    sqlite.drop_common_kline_table()
    sqlite.create_tables()
    sqlite.insert_common_klines(klines)
    sqlite.insert_common_klines(klines)


def test_ingest_klines_from_gmocoin_executions():
    gmo = GMOCoinClient()
    sqlite = SQLiteClient()

    interval = 60
    symbol = GMOCoinClient.symbols.BTC_JPY

    executions = gmo.get_executions_by_http(symbol, max_executions=300)
    klines: pd.DataFrame = gmo.convert_executions_to_common_klines(symbol, executions, interval, inclusive="neither")

    sqlite.drop_common_kline_table()
    sqlite.create_tables()
    sqlite.insert_common_klines(klines)


def test_ingest_klines_from_gmocoin_klines():
    gmo = GMOCoinClient()
    sqlite = SQLiteClient()

    interval = 60
    symbol = GMOCoinClient.symbols.BTC_JPY

    gmo_klines = gmo.get_klines_by_http(symbol, interval, "20220414")
    klines: pd.DataFrame = gmo.convert_klines_to_common_klines(symbol, gmo_klines, interval)

    sqlite.drop_common_kline_table()
    sqlite.create_tables()
    sqlite.insert_common_klines(klines)


def test_ingest_klines_from_bitflyer_executions():
    bitflyer = BitFlyerClient()
    sqlite = SQLiteClient()

    interval = 60
    symbol = BitFlyerClient.symbols.FX_BTC_JPY

    executions = bitflyer.get_executions_by_http(symbol, max_executions=300)
    klines: pd.DataFrame = bitflyer.convert_executions_to_common_klines(symbol, executions, interval)

    sqlite.insert_common_klines(klines)
