from __future__ import annotations

import time
from datetime import datetime, timedelta
from typing import Literal

import pandas as pd
import pandera as pa
from pandera.typing import DataFrame

from abundantia.adapters import BaseClient
from abundantia.schema import BitFlyerExecution, BitFlyerSymbols, CommonKlineSchema
from abundantia.utils import convert_interval_to_freq


class BitFlyerClient(BaseClient):
    name: str = "BitFlyer"
    http_url: str = "https://api.bitflyer.com"
    ws_url: str = "wss"
    symbols = BitFlyerSymbols

    def get_executions_by_http(
        self,
        product_code: BitFlyerSymbols,
        before: int | str | None = None,
        after: int | str | None = None,
        count: int = 500,
        max_executions: int = 500,
    ) -> list[BitFlyerExecution]:
        """
        before は含まない
        """
        count = min(count, max_executions)

        all_executions: list[BitFlyerExecution] = []
        params = {"product_code": product_code.name, "count": str(count)}

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
            self.logger.info(f"{all_executions[0].exec_date}, {all_executions[-1].exec_date}, {len(all_executions)}")
            time.sleep(self.duration)

        return all_executions

    @pa.check_types
    def convert_executions_to_common_klines(
        self,
        symbol: BitFlyerSymbols,
        interval: int,
        executions: list[BitFlyerExecution],
        inclusive: Literal["both", "neither"] = "both",
    ) -> DataFrame[CommonKlineSchema]:
        freq = convert_interval_to_freq(interval)

        df = pd.DataFrame(executions)

        df["time"] = pd.to_datetime(df["exec_date"], utc=True)
        df.set_index("time", inplace=True)
        df.sort_index(inplace=True)

        start: pd.Timestamp = df.index.min().floor(freq=freq)
        end: pd.Timestamp = df.index.max().floor(freq=freq)
        date_range = pd.date_range(start, end, name="open_time", freq=freq, inclusive=inclusive)

        group = df.resample(freq)
        ohlc: pd.DataFrame = group["price"].ohlc()
        volume: pd.Series[float] = group["size"].sum().rename("volume")

        klines = pd.DataFrame(index=date_range)
        klines["exchange"] = self.name
        klines["symbol"] = symbol.name
        klines["interval"] = interval
        klines = klines.join(ohlc).join(volume)

        klines = klines.reset_index()
        klines["open_time"] = klines["open_time"].map(datetime.timestamp).mul(1000).astype(int)
        return klines

    def get_klines(
        self, symbol: BitFlyerSymbols, interval: int, start_date: datetime, end_date: datetime
    ) -> DataFrame[CommonKlineSchema]:

        if start_date >= end_date:
            self.logger.error(f"Must be start_date < end_date. start_date={start_date}, end_date={end_date}.")
            raise ValueError

        if start_date < datetime.now() - timedelta(days=40):
            self.logger.error(f"{start_date} is too old.")
            raise ValueError

        if start_date.tzinfo is not None or end_date.tzinfo is not None:
            self.logger.error("Only tz_naive datetime object can be accepted.")
            raise ValueError

        start_ts = start_date.replace(tzinfo=self.tz).timestamp() * 1000
        end_ts = end_date.replace(tzinfo=self.tz).timestamp() * 1000
        current_ts = end_date.replace(tzinfo=self.tz).timestamp() * 1000

        before = None

        executions: list[BitFlyerExecution] = []
        while current_ts > start_ts:
            self.logger.info(f"{current_ts} {start_ts} {len(executions)}")
            executions_chunk = self.get_executions_by_http(symbol, before=before)
            executions += executions_chunk

            oldest_execution = executions_chunk[-1]
            before = oldest_execution.id
            current_ts = pd.Timestamp(oldest_execution.exec_date, tz="UTC").tz_convert(self.tz).timestamp() * 1000
            time.sleep(1)

        klines = self.convert_executions_to_common_klines(symbol, interval, executions)

        cond = klines["open_time"] >= start_ts
        if len(klines) <= cond.sum():
            self.logger.warning(f"lack: {start_date} {datetime.fromtimestamp(klines['open_time'].div(1000).min())}")
        klines = klines[cond].copy()

        cond = klines["open_time"] < end_ts
        if len(klines) <= cond.sum():
            self.logger.warning(f"lack: {datetime.fromtimestamp(klines['open_time'].div(1000).max())} {end_date}")
        klines = klines[cond].copy()

        return klines
