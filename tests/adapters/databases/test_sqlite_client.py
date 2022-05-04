import pandas as pd
import pytest

from abundantia.adapters import SQLiteClient
from abundantia.schema.common import CommonKlineModel


@pytest.mark.integration
def test_select():
    sqlite = SQLiteClient()
    results = sqlite.select_common_klines()

    for result in results:
        assert isinstance(result, CommonKlineModel)


@pytest.mark.integration
def test_ingest_duplicate_klines():
    sqlite = SQLiteClient()

    klines = pd.DataFrame(
        [
            {
                "exchange": "GMOCoin",
                "symbol": "BTC_JPY",
                "interval": 60,
                "open_time": 1649883600000,
                "open": 5182111.0,
                "high": 5185122.0,
                "low": 5181512.0,
                "close": 5184938.0,
                "volume": 1.74,
            },
            {
                "exchange": "GMOCoin",
                "symbol": "BTC_JPY",
                "interval": 60,
                "open_time": 1649883660000,
                "open": 5184722.0,
                "high": 5186495.0,
                "low": 5182884.0,
                "close": 5184506.0,
                "volume": 1.72,
            },
        ]
    )

    sqlite.drop_common_kline_table()
    sqlite.create_tables()
    sqlite.insert_common_klines(klines)
    sqlite.insert_common_klines(klines)
