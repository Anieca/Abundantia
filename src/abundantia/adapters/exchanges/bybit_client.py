import time
from datetime import datetime

import pandas as pd
import pandera as pa
from pandera.typing import DataFrame

from abundantia.adapters.exchanges.base import BaseClient
from abundantia.logger import setup_logger
from abundantia.schema.bybit import BybitInversePerpetualSymbols, BybitKline
from abundantia.schema.common import CommonKlineSchema

logger = setup_logger(__name__)


class BybitInversePerpetualClient(BaseClient):
    NAME: str = "BybitInversePerpetual"
    HTTP_URL: str = "https://api.bybit.com"
    WS_URL: str = ""
    SYMBOLS = BybitInversePerpetualSymbols

    INTERVALS: dict[int, str] = {
        60: "1",
        180: "3",
        300: "5",
        900: "15",
        1800: "30",
        3600: "60",
        7200: "120",
        14400: "240",
        21600: "360",
        43200: "720",
        86400: "D",
        604800: "W",
    }
    SWAP_INTERVALS: dict[str, int] = {v: k for k, v in INTERVALS.items()}

    @classmethod
    def get_klines_by_http(
        cls, symbol: BybitInversePerpetualSymbols, interval: int, date: datetime
    ) -> list[BybitKline]:
        klines: list[BybitKline] = []
        params = {
            "symbol": symbol.name,
            "interval": cls.convert_interval_to_specific(interval),
            "from": int(date.timestamp()),
        }
        result = cls.get("/v2/public/kline/list", params)

        if result is None:
            return klines

        klines = [BybitKline(**r) for r in result.get("result", [])]
        return klines

    @classmethod
    @pa.check_types
    def convert_klines_to_common_klines(
        cls,
        symbol: BybitInversePerpetualSymbols,
        interval: int,
        start_date: datetime,
        end_date: datetime,
        bybit_klines: list[BybitKline],
    ) -> DataFrame[CommonKlineSchema]:

        sub_klines = pd.DataFrame(bybit_klines)
        sub_klines["time"] = pd.to_datetime(sub_klines["open_time"], unit="s", utc=True).dt.tz_convert(cls.TZ)
        sub_klines = sub_klines.set_index("time").sort_index()
        del sub_klines["open_time"]

        klines = cls._create_common_klines(symbol.name, interval, start_date, end_date, sub_klines)

        return klines

    @classmethod
    def convert_interval_to_specific(cls, interval: int) -> str:
        return cls.INTERVALS[interval]

    def get_klines(
        self, symbol: BybitInversePerpetualSymbols, interval: int, start_date: datetime, end_date: datetime
    ) -> DataFrame[CommonKlineSchema]:

        if interval < min(self.INTERVALS):
            logger.error(f"{interval} is too small. mininum is {min(self.INTERVALS)}")
            raise ValueError

        limit = 200
        date = start_date
        bybit_klines = []
        while date < end_date:
            klines_chunk = self.get_klines_by_http(symbol, interval, date)
            bybit_klines += klines_chunk

            if len(klines_chunk) != limit:
                break

            *_, latest_kline = klines_chunk
            date = datetime.fromtimestamp(latest_kline.open_time + interval)
            time.sleep(self.duration)

        klines = self.convert_klines_to_common_klines(symbol, interval, start_date, end_date, bybit_klines)

        return klines
