import time
from datetime import datetime, timedelta
from typing import Literal

import pandas as pd
import pandera as pa
from pandera.typing import DataFrame

from abundantia.adapters.exchanges.base import BaseClient
from abundantia.schema.common import CommonKlineSchema
from abundantia.schema.gmocoin import GMOCoinExecution, GMOCoinKline, GMOCoinSymbols
from abundantia.utils import convert_interval_to_freq, setup_logger

logger = setup_logger(__name__)


class GMOCoinClient(BaseClient):
    name: str = "GMOCoin"
    http_url: str = "https://api.coin.z.com/public"
    ws_url: str = "wss://api.coin.z.com/ws/"
    symbols = GMOCoinSymbols

    INTERVALS: dict[int, str] = {
        60: "1min",
        300: "5min",
        600: "10min",
        900: "15min",
        1800: "30min",
        3600: "1hour",
        14400: "4hour",
        28800: "8hour",
        43200: "12hour",
        86400: "1day",
        604800: "1week",
    }

    def get_klines_by_http(self, symbol: GMOCoinSymbols, interval: int, date: datetime) -> list[GMOCoinKline]:
        """
        date の指定に対して日本時間 06:00 から取得
        2022/01/01 を指定すると 2022/01/01 06:00 ~ 2022/01/02 05:59:59 まで取得
        """

        klines: list[GMOCoinKline] = []
        params = {
            "symbol": symbol.name,
            "interval": self.convert_interval_to_specific(interval),
            "date": self.convert_datetime_to_specific(date),
        }

        result = self.get("/v1/klines", params)

        if result is None:
            return klines

        if result.get("status") != 0:
            logger.error(result)
            return klines

        klines = [GMOCoinKline(**d) for d in result.get("data", [])]
        return klines

    def get_executions_by_http(
        self, symbol: GMOCoinSymbols, page: int = 1, count: int = 100, max_executions: int = 500
    ) -> list[GMOCoinExecution]:

        count = min(count, max_executions)

        all_executions: list[GMOCoinExecution] = []
        params = {"symbol": symbol.name, "page": str(page), "count": str(count)}

        while len(all_executions) < max_executions:
            result = self.get("/v1/trades", params=params)

            if result is None:
                break

            if result.get("status") != 0:
                logger.error(result)
                break

            data = result.get("data", {})
            executions = data.get("list", [])
            current_page = data.get("pagination", {}).get("currentPage", None)
            all_executions += [GMOCoinExecution(**e) for e in executions]

            if current_page is None:
                logger.warn(f"pagination error. {result}")
                break

            if len(executions) != count:
                logger.warn(f"{len(executions)} != {count}.")
                break

            params["page"] = str(current_page + 1)
            logger.info(f"{all_executions[0].timestamp}, {all_executions[-1].timestamp}, {len(all_executions)}")
            time.sleep(self.duration)

        return all_executions

    @classmethod
    @pa.check_types
    def convert_executions_to_common_klines(
        cls,
        symbol: GMOCoinSymbols,
        interval: int,
        executions: list[GMOCoinExecution],
        inclusive: Literal["both", "neither"] = "both",
    ) -> DataFrame[CommonKlineSchema]:
        freq = convert_interval_to_freq(interval)

        df = pd.DataFrame(executions)

        df["time"] = pd.to_datetime(df["timestamp"])
        df.set_index("time", inplace=True)
        df.sort_index(inplace=True)

        date_range = cls.get_date_range(df.index, freq, inclusive)

        group = df.resample(freq)
        ohlc: pd.DataFrame = group["price"].ohlc()
        volume: pd.Series[float] = group["size"].sum().rename("volume")

        klines = pd.DataFrame(index=date_range)
        klines["exchange"] = cls.name
        klines["symbol"] = symbol.name
        klines["interval"] = interval
        klines = klines.join(ohlc).join(volume)

        klines = klines.reset_index()
        klines["open_time"] = klines["open_time"].map(datetime.timestamp).mul(1000).astype(int)
        return klines

    @classmethod
    @pa.check_types
    def convert_klines_to_common_klines(
        cls,
        symbol: GMOCoinSymbols,
        interval: int,
        start_date: datetime,
        end_date: datetime,
        gmo_klines: list[GMOCoinKline],
    ) -> DataFrame[CommonKlineSchema]:

        sub_klines = pd.DataFrame(gmo_klines).rename({"openTime": "open_time"}, axis=1)
        sub_klines["time"] = sub_klines["open_time"].div(1000).map(datetime.fromtimestamp)
        sub_klines = sub_klines.set_index("time").sort_index()
        del sub_klines["open_time"]

        index = pd.date_range(
            start_date, end_date, freq=convert_interval_to_freq(interval), inclusive="left", name="time"
        )
        klines = pd.DataFrame(index=index).join(sub_klines).reset_index()
        klines["exchange"] = cls.name
        klines["symbol"] = symbol.name
        klines["interval"] = interval
        klines["open_time"] = klines["time"].map(datetime.timestamp).mul(1000).astype(int)
        del klines["time"]

        return klines

    @classmethod
    def convert_interval_to_specific(cls, interval: int) -> str:
        return cls.INTERVALS[interval]

    @classmethod
    def convert_datetime_to_specific(cls, date: datetime) -> str:
        return datetime.strftime(date, "%Y%m%d")

    def get_klines(
        self, symbol: GMOCoinSymbols, interval: int, start_date: datetime, end_date: datetime
    ) -> DataFrame[CommonKlineSchema]:

        # 日本時間 06:00:00 が開始点のため指定日の1日前から取得する
        req_start_date = start_date - timedelta(days=1)

        if interval < min(self.INTERVALS):
            logger.error(f"{interval} is too small. mininum is {min(self.INTERVALS)}")
            raise ValueError

        if start_date < datetime(2021, 4, 16):
            logger.error(f"{start_date} is too small. mininum is {datetime(2021, 4, 16)}")
            raise ValueError

        date = req_start_date
        gmo_klines = []
        while date < end_date:
            gmo_klines += self.get_klines_by_http(symbol, interval, date)
            date += timedelta(days=1)
            time.sleep(self.duration)

        klines = self.convert_klines_to_common_klines(symbol, interval, start_date, end_date, gmo_klines)

        return klines
