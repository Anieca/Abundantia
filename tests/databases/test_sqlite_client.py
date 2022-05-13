import pandas as pd
import pytest

from abundantia.databases.sqlite_client import SQLiteClient
from abundantia.schema.model import CommonKlineModel


@pytest.mark.database
class TestSQLiteClient:
    def setup_method(self):
        self.sqlite = SQLiteClient(file_path="sqlite:///resources/testdb.sqlite3")
        self.sqlite.create_tables()

    def teardown_method(self):
        self.sqlite.drop_common_kline_table()
        del self.sqlite

    def test_select(self):
        results = self.sqlite.select_common_klines()
        for result in results:
            assert isinstance(result, CommonKlineModel)

    def test_ingest_duplicate_klines(self):
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

        self.sqlite.insert_common_klines(klines)
        self.sqlite.insert_common_klines(klines)
