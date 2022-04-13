import pandas as pd

from abundantia.adaptors import GMOCoinClient, SQLiteClient


def test_gmocoin_klines_ingestion():

    gmo = GMOCoinClient()
    sqlite = SQLiteClient()

    symbol = GMOCoinClient.btc_jpy

    executions = gmo.get_executions_by_http(symbol, max_executions=1000)

    klines: pd.DataFrame = gmo.convert_executions_to_common_klines(symbol, executions, inclusive="neither")
    print(klines)

    sqlite.drop_common_kline_table()
    sqlite.create_tables()
    sqlite.insert_common_klines(klines)
