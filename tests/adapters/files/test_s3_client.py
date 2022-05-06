import pandas as pd
import pytest

from abundantia.adapters.files.s3_client import S3Client


@pytest.mark.integration
def test_dump_and_load():
    klines = pd.DataFrame(
        [
            {
                "open_time": 1649044800000,
                "exchange": "BitFlyer",
                "symbol": "FX_BTC_JPY",
                "interval": 60,
                "open": 5739670.0,
                "high": 5740584.0,
                "low": 5737244.0,
                "close": 5738799.0,
                "volume": 4.75922874,
            },
            {
                "open_time": 1649044860000,
                "exchange": "BitFlyer",
                "symbol": "FX_BTC_JPY",
                "interval": 60,
                "open": 5738975.0,
                "high": 5742809.0,
                "low": 5738359.0,
                "close": 5739313.0,
                "volume": 3.02027881,
            },
            {
                "open_time": 1649044920000,
                "exchange": "BitFlyer",
                "symbol": "FX_BTC_JPY",
                "interval": 60,
                "open": 5740024.0,
                "high": 5740124.0,
                "low": 5733284.0,
                "close": 5734513.0,
                "volume": 2.94878242,
            },
            {
                "open_time": 1649044980000,
                "exchange": "BitFlyer",
                "symbol": "FX_BTC_JPY",
                "interval": 60,
                "open": 5734510.0,
                "high": 5734510.0,
                "low": 5719042.0,
                "close": 5724633.0,
                "volume": 21.85205793,
            },
            {
                "open_time": 1649045040000,
                "exchange": "BitFlyer",
                "symbol": "FX_BTC_JPY",
                "interval": 60,
                "open": 5725342.0,
                "high": 5725342.0,
                "low": 5719847.0,
                "close": 5725114.0,
                "volume": 4.7008754,
            },
        ]
    )

    client = S3Client()
    client.dump("test.csv", klines, index=False)
    loaded_klines = client.load("test.csv")

    assert klines.loc[0, "open_time"] == loaded_klines.loc[0, "open_time"]
