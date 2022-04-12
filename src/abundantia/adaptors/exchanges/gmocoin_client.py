import traceback

import pandas as pd
import requests

from abundantia.schema.gmocoin import GMOCoinExecution, GMOCoinKline
from abundantia.utils import setup_logger


class GMOCoinClient:
    http_url: str = "https://api.coin.z.com/public"
    ws_url: str = "wss://api.coin.z.com/ws/"
    btc_jpy: str = "BTC_JPY"
    btc: str = "BTC"
    symbols: tuple[str, ...] = (btc_jpy, btc)

    def __init__(self, log_level: str = "DEBUG") -> None:
        self.logger = setup_logger(__name__, log_level)

    def get_klines_by_http(self, symbol: str, interval: str, date: str) -> list[GMOCoinKline]:
        klines: list[GMOCoinKline] = []
        params = {"symbol": symbol, "interval": interval, "date": date}

        self.logger.info(params)

        try:
            response = requests.get(f"{self.http_url}/v1/klines", params=params)
            res_json = response.json()
        except Exception:
            self.logger.error(traceback.format_exc())
            return klines

        klines = [GMOCoinKline(**d) for d in res_json.get("data", [])]

        return klines

    def get_executions_by_http(
        self, symbol: str, page: int = 1, count: int = 100, max_executions: int = 100_000
    ) -> list[GMOCoinExecution]:

        assert symbol in self.symbols
        count = min(count, max_executions)

        all_executions: list[GMOCoinExecution] = []
        params = {"symbol": symbol, "page": str(page), "count": str(count)}

        while len(all_executions) < max_executions:
            self.logger.info(params)

            try:
                response = requests.get(f"{self.http_url}/v1/trades", params=params)
                res_json = response.json()
            except Exception:
                self.logger.error(traceback.format_exc())
                break

            data = res_json.get("data", {})
            executions = data.get("list", [])
            current_page = data.get("pagination", {}).get("currentPage", None)
            all_executions += [GMOCoinExecution(**e) for e in executions]

            if current_page is None:
                self.logger.warn(f"pagination error. {data}")
                break

            if len(executions) != count:
                self.logger.warn(f"{len(executions)} != {count}.")
                break

            params["page"] = str(current_page + 1)
            self.logger.info(f"{all_executions[0].timestamp}, {all_executions[-1].timestamp}, {len(all_executions)}")

        return all_executions

    @staticmethod
    def convert_executions_to_common_klines(
        executions: list[GMOCoinExecution], freq: str = "T", inclusive: str = "neither"
    ) -> pd.DataFrame:

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

        klines = pd.DataFrame(index=date_range).join(ohlc).join(volume)

        return klines
