import time
from datetime import datetime

import pandas as pd
import pandera as pa
from pandera.typing import DataFrame

from abundantia.exchanges.base import BaseClient, Symbols
from abundantia.logger import setup_logger
from abundantia.schema.common import CommonKlineSchema
from abundantia.schema.ftx import FTXKline, FTXSymbols

logger = setup_logger(__name__)


class FTXClient(BaseClient):
    NAME: str = "FTX"
    HTTP_URL: str = "https://ftx.com/api"
    WS_URL: str = ""
    SYMBOLS = FTXSymbols

    @classmethod
    def get_klines_by_http(
        cls, symbol: FTXSymbols, interval: int, start_time: datetime, end_time: datetime
    ) -> list[FTXKline]:
        """指定不可能だが最大 1500 件"""

        klines: list[FTXKline] = []
        if end_time > pd.Timestamp(datetime.now(), tzinfo=cls.TZ).ceil("D"):
            raise ValueError

        params = {
            "resolution": interval,
            "start_time": int(start_time.timestamp()),
            "end_time": int(end_time.timestamp()),
        }
        result = cls.get(f"/markets/{symbol.value}/candles", params)

        if result is None:
            return klines

        if not result.get("success"):
            logger.error(result.get("error"))
            raise ValueError

        klines = [FTXKline(**r) for r in result.get("result", [])]
        return klines

    def get_klines(
        self, symbol: Symbols, interval: int, start_date: datetime, end_date: datetime
    ) -> DataFrame[CommonKlineSchema]:

        self._check_invalid_datetime(start_date, end_date)

        if interval < 60:
            raise NotImplementedError

        start_date = self.convert_aware_datetime(start_date)
        end_date = self.convert_aware_datetime(end_date)
        req_end_date = min(end_date, pd.Timestamp(datetime.now(), tzinfo=self.TZ))
        ftx_klines = []

        current_date = req_end_date
        while current_date > start_date:
            klines_chunk = self.get_klines_by_http(symbol, interval, start_date, current_date)
            ftx_klines += klines_chunk

            oldest_kline, *_ = klines_chunk
            current_date = pd.Timestamp(oldest_kline.startTime, tz=self.TZ)
            time.sleep(self.duration)

        klines = self.convert_klines_to_common_klines(symbol, interval, start_date, end_date, ftx_klines)

        return klines

    @classmethod
    @pa.check_types
    def convert_klines_to_common_klines(
        cls,
        symbol: FTXSymbols,
        interval: int,
        start_date: datetime,
        end_date: datetime,
        ftx_klines: list[FTXKline],
    ) -> DataFrame[CommonKlineSchema]:

        start_date = cls.convert_aware_datetime(start_date)
        end_date = cls.convert_aware_datetime(end_date)

        sub_klines = pd.DataFrame(ftx_klines)
        sub_klines["time"] = pd.to_datetime(sub_klines["startTime"]).dt.tz_convert(cls.TZ)  # overwrite response
        sub_klines = sub_klines.set_index("time").sort_index()
        del sub_klines["startTime"]

        klines = cls._create_common_klines(symbol.value, interval, start_date, end_date, sub_klines)
        return klines
