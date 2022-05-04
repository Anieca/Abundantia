from datetime import datetime

from abundantia.adapters import GMOCoinClient, SQLiteClient
from abundantia.schema import GMOCoinSymbols


def ingest_klines_to_sqlite_from_gmocoin_klines(
    symbol: GMOCoinSymbols, interval: int, start_date: datetime, end_date: datetime
) -> None:

    gmo = GMOCoinClient()
    sqlite = SQLiteClient()

    klines = gmo.get_klines(symbol, interval, start_date, end_date)
    sqlite.insert_common_klines(klines)
