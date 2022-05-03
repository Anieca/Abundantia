import time
from datetime import datetime, timedelta

from abundantia.adapters import GMOCoinClient, SQLiteClient
from abundantia.schema.gmocoin import GMOCoinSymbols


def ingest_klines_from_gmocoin_klines(
    symbol: GMOCoinSymbols,
    interval: int,
    start_date: datetime,
    end_date: datetime,
) -> None:

    gmo = GMOCoinClient()
    sqlite = SQLiteClient()

    date = start_date
    while date < end_date:
        gmo_klines = gmo.get_klines_by_http(symbol, interval, date)
        klines = gmo.convert_klines_to_common_klines(symbol, interval, gmo_klines)
        sqlite.insert_common_klines(klines)
        date += timedelta(days=1)
        time.sleep(1)
