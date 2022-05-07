import time
from datetime import datetime

import pandas as pd
import pandera as pa
from pandera.typing import DataFrame

from abundantia.adapters.exchanges.base import BaseClient
from abundantia.schema.bybit import BybitInversePerpetualSymbols, BybitKline
from abundantia.schema.common import CommonKlineSchema
from abundantia.utils import convert_interval_to_freq


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
    def convert_klines_to_common_klines(
        cls,
        symbol: BybitInversePerpetualSymbols,
        interval: int,
        start_date: datetime,
        end_date: datetime,
        bybit_klines: list[BybitKline],
    ) -> DataFrame[CommonKlineSchema]:

        sub_klines = pd.DataFrame(bybit_klines)
        sub_klines["time"] = sub_klines["open_time"].map(datetime.fromtimestamp)
        sub_klines = sub_klines.set_index("time").sort_index()
        del sub_klines["open_time"]

        index = pd.date_range(
            start_date, end_date, freq=convert_interval_to_freq(interval), inclusive="left", name="time"
        )
        klines = pd.DataFrame(index=index).join(sub_klines).reset_index()
        klines["exchange"] = cls.name
        klines["symbol"] = symbol.name  # overwrite response
        klines["interval"] = interval  # overwrite response
        klines["open_time"] = klines["time"].map(datetime.timestamp).mul(1000).astype(int)
        del klines["time"], klines["turnover"]

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

        limit = 200
        date = start_date
        bybit_klines = []
        while date < end_date:
            klines_chunk = self.get_klines_by_http(symbol, interval, date)
            bybit_klines += klines_chunk

            if len(klines_chunk) != limit:
                break

            *_, latest_kline = klines_chunk
            date = datetime.fromtimestamp(latest_kline.open_time)
            time.sleep(self.duration)

        klines = self.convert_klines_to_common_klines(symbol, interval, start_date, end_date, bybit_klines)

        return klines
