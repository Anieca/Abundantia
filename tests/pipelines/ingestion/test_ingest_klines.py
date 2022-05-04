from datetime import datetime

import pytest

from abundantia.adapters.databases.sqlite_client import SQLiteClient
from abundantia.pipelines.ingestion.ingest_klines import ingest_klines_to_sqlite_from_gmocoin_klines
from abundantia.schema import GMOCoinSymbols


@pytest.mark.integration
def test_ingest_klines_to_sqlite_from_gmocoin_klines():
    symbol = GMOCoinSymbols.BTC_JPY
    interval = 60
    start_date = datetime(2021, 4, 15)
    end_date = datetime(2021, 4, 20)

    sqlite = SQLiteClient()
    sqlite.create_tables()
    ingest_klines_to_sqlite_from_gmocoin_klines(symbol, interval, start_date, end_date)
    sqlite.drop_common_kline_table()
