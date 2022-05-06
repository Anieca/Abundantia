from datetime import datetime

from abundantia.adapters import BitFlyerClient, GMOCoinClient, SQLiteClient
from abundantia.schema import GMOCoinSymbols
from abundantia.schema.bitflyer import BitFlyerSymbols


def ingest_klines_to_sqlite_from_gmocoin_klines(
    symbol: GMOCoinSymbols, interval: int, start_date: datetime, end_date: datetime
) -> None:

    gmo = GMOCoinClient()
    sqlite = SQLiteClient()

    klines = gmo.get_klines(symbol, interval, start_date, end_date)
    sqlite.insert_common_klines(klines)


def ingest_klines_to_sqlite_from_bitflyer_klines(
    symbol: BitFlyerSymbols, interval: int, start_date: datetime, end_date: datetime
) -> None:

    bitflyer = BitFlyerClient()
    sqlite = SQLiteClient()

    klines = bitflyer.get_klines(symbol, interval, start_date, end_date)
    sqlite.insert_common_klines(klines)
