from datetime import datetime

import pytest

from abundantia.pipelines.ingestion.ingest_klines import (
    ingest_klines_to_sqlite_from_bitflyer_klines,
    ingest_klines_to_sqlite_from_gmocoin_klines,
)
from abundantia.schema import BitFlyerSymbols, GMOCoinSymbols


@pytest.mark.integration
def test_ingest_klines_to_sqlite_from_gmocoin_klines():
    symbol = GMOCoinSymbols.BTC_JPY
    interval = 60
    start_date = datetime(2021, 4, 15)
    end_date = datetime(2021, 4, 20)
    ingest_klines_to_sqlite_from_gmocoin_klines(symbol, interval, start_date, end_date)


@pytest.mark.integration
def test_ingest_klines_to_sqlite_from_bitflyer_executions():
    symbol = BitFlyerSymbols.FX_BTC_JPY
    interval = 60
    start_date = datetime(2022, 5, 1)
    end_date = datetime(2022, 5, 3)
    ingest_klines_to_sqlite_from_bitflyer_klines(symbol, interval, start_date, end_date)
