import time
from datetime import datetime
from typing import Any

import pandas as pd
from pandera.typing import DataFrame

from abundantia.adapters.exchanges.base import BaseClient
from abundantia.schema.common import CommonKlineSchema
from abundantia.schema.ftx import FTXKline, FTXSymbols
from abundantia.utils import convert_interval_to_freq


class FTXClient(BaseClient):
    name: str = "FTX"
    http_url: str = "https://ftx.com/api"
    ws_url: str = ""
    symbols = FTXSymbols

    def get_klines_by_http(
        self, symbol: FTXSymbols, interval: int, start_time: datetime, end_time: datetime
    ) -> list[FTXKline]:
        """指定不可能だが最大 1500 件"""

        klines: list[FTXKline] = []
        if end_time > pd.Timestamp(datetime.now(), tzinfo=self.tz).ceil("D"):
            raise ValueError

        params = {
            "resolution": interval,
            "start_time": int(start_time.timestamp()),
            "end_time": int(end_time.timestamp()),
        }
        result = self.get(f"/markets/{symbol.value}/candles", params)

        if result is None:
            return klines

        if not result.get("success"):
            self.logger.error(result.get("error"))
            raise ValueError

        klines = [FTXKline(**r) for r in result.get("result", [])]
        return klines

    def get_klines(
        self, symbol: Any, interval: int, start_date: datetime, end_date: datetime
    ) -> DataFrame[CommonKlineSchema]:
        if interval < 60:
            raise NotImplementedError

        start_date = start_date.replace(tzinfo=self.tz)
        end_date = end_date.replace(tzinfo=self.tz)
        req_end_date = min(end_date, pd.Timestamp(datetime.now(), tzinfo=self.tz))
        ftx_klines = []

        current_date = req_end_date
        while current_date > start_date:
            klines_chunk = self.get_klines_by_http(symbol, interval, start_date, current_date)
            ftx_klines += klines_chunk

            oldest_kline, *_ = klines_chunk
            current_date = pd.Timestamp(oldest_kline.startTime).tz_convert(self.tz)
            time.sleep(self.duration)

        klines = self.convert_klines_to_common_klines(symbol, interval, start_date, end_date, ftx_klines)

        return klines

    @classmethod
    def convert_klines_to_common_klines(
        cls,
        symbol: FTXSymbols,
        interval: int,
        start_date: datetime,
        end_date: datetime,
        ftx_klines: list[FTXKline],
    ) -> DataFrame[CommonKlineSchema]:
        sub_klines = pd.DataFrame(ftx_klines)
        sub_klines["time"] = pd.to_datetime(sub_klines["startTime"]).dt.tz_convert(cls.tz)  # overwrite response
        sub_klines = sub_klines.set_index("time").sort_index()
        del sub_klines["startTime"]

        index = pd.date_range(
            start_date, end_date, freq=convert_interval_to_freq(interval), inclusive="left", name="time", tz=cls.tz
        )
        klines = pd.DataFrame(index=index).join(sub_klines).reset_index()
        klines["exchange"] = cls.name
        klines["symbol"] = symbol.value
        klines["interval"] = interval
        klines["open_time"] = klines["time"].map(datetime.timestamp).mul(1000).astype(int)
        del klines["time"]

        return klines
