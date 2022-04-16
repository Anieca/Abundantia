from __future__ import annotations

from datetime import datetime

import pandas as pd

from abundantia.adaptors import BaseClient
from abundantia.schema.bitflyer import BitFlyerExecution
from abundantia.utils import convert_interval_to_freq


class BitFlyerClient(BaseClient):
    name: str = "BitFlyer"
    http_url: str = "https://api.bitflyer.com"
    ws_url: str = "wss"
    fx_btc_jpy: str = "FX_BTC_JPY"
    btc_jpy: str = "BTC_JPY"
    symbols: tuple[str, ...] = (btc_jpy, fx_btc_jpy)

    def get_executions_by_http(
        self,
        product_code: str,
        before: int | str | None = None,
        after: int | str | None = None,
        count: int = 500,
        max_executions: int = 100_000,
    ) -> list[BitFlyerExecution]:
        """
        before は含まない
        """
        assert product_code in self.symbols
        count = min(count, max_executions)

        all_executions: list[BitFlyerExecution] = []
        params = {"product_code": product_code, "count": str(count)}

        if before is not None:
            params["before"] = str(before)
        if after is not None:
            params["after"] = str(after)

        while len(all_executions) < max_executions:

            result = self.get(f"{self.http_url}/v1/executions", params=params)

            if result is None:
                break

            executions = result
            all_executions += [BitFlyerExecution(**e) for e in executions]

            if len(executions) != count:
                self.logger.warn(f"{len(executions)} != {count}.")
                break

            params["before"] = str(all_executions[-1].id)
            self.logger.info(f"{all_executions[0].id}, {all_executions[-1].id}, {len(all_executions)}")

        return all_executions

    def convert_executions_to_common_klines(
        self, symbol: str, executions: list[BitFlyerExecution], interval: int, inclusive: str = "neither"
    ) -> pd.DataFrame:
        freq = convert_interval_to_freq(interval)

        df = pd.DataFrame(executions)

        df["time"] = pd.to_datetime(df["exec_date"])
        df.set_index("time", inplace=True)
        df.sort_index(inplace=True)

        start: pd.Timestamp = df.index.min().round(freq=freq)
        end: pd.Timestamp = df.index.max().round(freq=freq)
        date_range = pd.date_range(start, end, name="open_time", freq=freq, inclusive=inclusive)

        group = df.resample(freq)
        ohlc: pd.DataFrame = group["price"].ohlc()
        volume: pd.Series[float] = group["size"].sum().rename("volume")

        klines = pd.DataFrame(index=date_range)
        klines["exchange"] = self.name
        klines["symbol"] = symbol
        klines["interval"] = interval
        klines = klines.join(ohlc).join(volume)

        klines = klines.reset_index()
        klines["open_time"] = klines["open_time"].map(datetime.timestamp).mul(1000)

        return klines
