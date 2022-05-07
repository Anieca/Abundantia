from __future__ import annotations

import time
from datetime import datetime, timedelta

import pandas as pd
import pandera as pa
from pandera.typing import DataFrame

from abundantia.adapters.exchanges.base import BaseClient
from abundantia.schema.bitflyer import BitFlyerExecution, BitFlyerSymbols
from abundantia.schema.common import CommonKlineSchema
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

            if not isinstance(result, list):
                self.logger.error(result)
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

    @classmethod
    @pa.check_types
    def convert_executions_to_common_klines(
        cls,
        symbol: BitFlyerSymbols,
        interval: int,
        start_date: datetime,
        end_date: datetime,
        executions: list[BitFlyerExecution],
    ) -> DataFrame[CommonKlineSchema]:
        freq = convert_interval_to_freq(interval)

        df = pd.DataFrame(executions)
        df = df.sort_values(by="id").reset_index(drop=True)

        df["time"] = pd.to_datetime(df["exec_date"], utc=True).dt.tz_convert(cls.tz)
        df = df.set_index("time").sort_index()

        index = pd.date_range(
            start_date, end_date, freq=convert_interval_to_freq(interval), inclusive="left", name="time", tz=cls.tz
        )

        group = df.resample(freq)
        ohlc: pd.DataFrame = group["price"].ohlc()
        volume: pd.Series[float] = group["size"].sum().rename("volume")

        klines = pd.DataFrame(index=index).join(ohlc).join(volume).reset_index()
        klines["exchange"] = cls.name
        klines["symbol"] = symbol.name
        klines["interval"] = interval
        klines["open_time"] = klines["time"].map(datetime.timestamp).mul(1000).astype(int)
        del klines["time"]

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

        start_date = start_date.replace(tzinfo=self.tz)
        end_date = end_date.replace(tzinfo=self.tz)
        current_date = end_date.replace(tzinfo=self.tz)

        before = None
        max_executions = 500

        executions: list[BitFlyerExecution] = []
        while current_date > start_date:
            self.logger.info(f"{current_date} {start_date} {len(executions)}")
            executions_chunk = self.get_executions_by_http(symbol, before=before, max_executions=max_executions)
            executions += executions_chunk

            if len(executions_chunk) != max_executions:
                break

            *_, oldest_execution = executions_chunk
            before = oldest_execution.id
            current_date = pd.Timestamp(oldest_execution.exec_date, tz="UTC").tz_convert(self.tz)
            time.sleep(self.duration)

        klines = self.convert_executions_to_common_klines(symbol, interval, start_date, end_date, executions)
        return klines
