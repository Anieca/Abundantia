from datetime import datetime
from time import sleep
from typing import Literal

import pandas as pd

from abundantia.adaptors import BaseClient
from abundantia.schema.gmocoin import GMOCoinExecution, GMOCoinKline
from abundantia.utils import convert_interval_to_freq


class GMOCoinClient(BaseClient):
    http_url: str = "https://api.coin.z.com/public"
    ws_url: str = "wss://api.coin.z.com/ws/"
    btc_jpy: str = "BTC_JPY"
    btc: str = "BTC"
    symbols: tuple[str, ...] = (btc_jpy, btc)

    INTERVALS: dict[int, str] = {
        60: "1min",
        300: "5min",
        600: "10min",
        900: " 15min",
        1800: " 30min",
        3600: " 1hour",
        14400: "4hour",
        28800: "8hour",
        43200: "12hour",
        86400: "1day",
        604800: "1week",
    }

    def get_klines_by_http(self, symbol: str, interval: int, date: str) -> list[GMOCoinKline]:
        klines: list[GMOCoinKline] = []
        params = {"symbol": symbol, "interval": self.convert_interval_to_specific(interval), "date": date}

        result = self.get(f"{self.http_url}/v1/klines", params)

        if result is None:
            return klines

        klines = [GMOCoinKline(**d) for d in result.get("data", [])]
        return klines

    def get_executions_by_http(
        self, symbol: str, page: int = 1, count: int = 100, max_executions: int = 100_000
    ) -> list[GMOCoinExecution]:

        assert symbol in self.symbols
        count = min(count, max_executions)

        all_executions: list[GMOCoinExecution] = []
        params = {"symbol": symbol, "page": str(page), "count": str(count)}

        while len(all_executions) < max_executions:
            result = self.get(f"{self.http_url}/v1/trades", params=params)

            if result is None:
                break

            data = result.get("data", {})
            executions = data.get("list", [])
            current_page = data.get("pagination", {}).get("currentPage", None)
            all_executions += [GMOCoinExecution(**e) for e in executions]

            if current_page is None:
                self.logger.warn(f"pagination error. {result}")
                break

            if len(executions) != count:
                self.logger.warn(f"{len(executions)} != {count}.")
                break

            params["page"] = str(current_page + 1)
            self.logger.info(f"{all_executions[0].timestamp}, {all_executions[-1].timestamp}, {len(all_executions)}")
            sleep(1)

        return all_executions

    @staticmethod
    def convert_executions_to_common_klines(
        symbol: str,
        executions: list[GMOCoinExecution],
        interval: int,
        inclusive: Literal["both", "neither"] = "neither",
    ) -> pd.DataFrame:
        freq = convert_interval_to_freq(interval)

        df = pd.DataFrame(executions)

        df["time"] = pd.to_datetime(df["timestamp"])
        df.set_index("time", inplace=True)
        df.sort_index(inplace=True)

        start: pd.Timestamp = df.index.min().round(freq=freq)
        end: pd.Timestamp = df.index.max().round(freq=freq)
        date_range = pd.date_range(start, end, name="open_time", freq=freq, inclusive=inclusive)

        group = df.resample(freq)
        ohlc: pd.DataFrame = group["price"].ohlc()
        volume: pd.Series[float] = group["size"].sum().rename("volume")

        klines = pd.DataFrame(index=date_range)
        klines["exchange"] = "GMOCoin"
        klines["symbol"] = symbol
        klines["interval"] = interval
        klines = klines.join(ohlc).join(volume)

        klines = klines.reset_index()
        klines["open_time"] = klines["open_time"].map(datetime.timestamp).mul(1000)

        return klines

    @staticmethod
    def convert_klines_to_common_klines(symbol: str, gmo_klines: list[GMOCoinKline], interval: int) -> pd.DataFrame:
        klines = pd.DataFrame(gmo_klines)
        klines.sort_values(by="openTime", inplace=True)

        klines["exchange"] = "GMOCoin"
        klines["symbol"] = symbol
        klines["interval"] = interval

        klines = klines.rename({"openTime": "open_time"}, axis=1)

        return klines

    @classmethod
    def convert_interval_to_specific(cls, interval: int) -> str:
        return cls.INTERVALS[interval]
