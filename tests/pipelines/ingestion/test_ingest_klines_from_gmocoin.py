from datetime import datetime

from abundantia.pipelines.ingestion import ingest_gmocoin_klines_to_sqlite
from abundantia.schema.gmocoin import GMOCoinSymbols


def test_ingest_klines_from_gmocoin_klines():

    ingest_gmocoin_klines_to_sqlite(
        GMOCoinSymbols.BTC_JPY,
        60,
        datetime(2021, 4, 15),
        datetime(2021, 4, 20),
    )
