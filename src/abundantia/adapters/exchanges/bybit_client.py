import time
from datetime import datetime

import pandas as pd
import pandera as pa
from pandera.typing import DataFrame

from abundantia.adapters.exchanges.base import BaseClient
from abundantia.schema.bybit import BybitInversePerpetualSymbols, BybitKline
from abundantia.schema.common import CommonKlineSchema


class BybitInversePerpetualClient(BaseClient):
    name: str = "BybitInversePerpetual"
    http_url: str = "https://api.bybit.com"
    ws_url: str = ""
    symbols = BybitInversePerpetualSymbols

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

    def get_klines_by_http(
        self, symbol: BybitInversePerpetualSymbols, interval: int, date: datetime
    ) -> list[BybitKline]:
        klines: list[BybitKline] = []
        params = {
            "symbol": symbol.name,
            "interval": self.convert_interval_to_specific(interval),
            "from": int(date.timestamp()),
        }
        result = self.get(f"{self.http_url}/v2/public/kline/list", params)

        if result is None:
            return klines

        klines = [BybitKline(**r) for r in result.get("result", [])]
        return klines

    @classmethod
    @pa.check_types
    def convert_klines_to_common_klines(cls, bybit_klines: list[BybitKline]) -> DataFrame[CommonKlineSchema]:
        klines = pd.DataFrame(bybit_klines)
        klines.sort_values(by="open_time", inplace=True)

        klines["exchange"] = cls.name
        klines["interval"] = klines["interval"].map(cls.SWAP_INTERVALS)
        klines["open_time"] = klines["open_time"].mul(1000)
        del klines["turnover"]

        return klines

    @classmethod
    def convert_interval_to_specific(cls, interval: int) -> str:
        return cls.INTERVALS[interval]

    def get_klines(
        self, symbol: BybitInversePerpetualSymbols, interval: int, start_date: datetime, end_date: datetime
    ) -> DataFrame[CommonKlineSchema]:

        if interval < min(self.INTERVALS):
            self.logger.error(f"{interval} is too small. mininum is {min(self.INTERVALS)}")
            raise ValueError

        start_ts = start_date.timestamp() * 1000
        end_ts = end_date.timestamp() * 1000

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

        klines = self.convert_klines_to_common_klines(bybit_klines)

        cond = klines["open_time"] >= start_ts
        if len(klines) < cond.sum():
            self.logger.warning(
                f"No data from {start_date} to {datetime.fromtimestamp(klines['open_time'].div(1000).min())}"
            )
        klines = klines[cond].copy()

        cond = klines["open_time"] < end_ts
        if len(klines) <= cond.sum():
            self.logger.warning(
                f"No data from {datetime.fromtimestamp(klines['open_time'].div(1000).max())} to {end_date}"
            )
        klines = klines[cond].copy()

        return klines
