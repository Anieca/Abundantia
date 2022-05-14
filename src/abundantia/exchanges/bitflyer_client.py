from __future__ import annotations

import time
from datetime import datetime, timedelta

import pandas as pd
import pandera as pa
from pandera.typing import DataFrame

from abundantia.exchanges.base import BaseClient, Symbols
from abundantia.logger import setup_logger
from abundantia.schema.bitflyer import BitFlyerExecution, BitFlyerSymbols
from abundantia.schema.common import CommonKlineSchema

logger = setup_logger(__name__)


class BitFlyerClient(BaseClient):
    NAME: str = "BitFlyer"
    HTTP_URL: str = "https://api.bitflyer.com"
    WS_URL: str = ""
    SYMBOLS = BitFlyerSymbols

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
            result = self.get("/v1/executions", params=params)

            if result is None:
                break

            if not isinstance(result, list):
                logger.error(result)
                break

            executions = result
            all_executions += [BitFlyerExecution(**e) for e in executions]

            if len(executions) != count:
                logger.warn(f"{len(executions)} != {count}.")
                break

            params["before"] = str(all_executions[-1].id)
            logger.info(f"{all_executions[0].exec_date}, {all_executions[-1].exec_date}, {len(all_executions)}")
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
        freq = cls.convert_interval_to_freq(interval)

        execution_df = pd.DataFrame(executions)
        execution_df = execution_df.sort_values(by="id").reset_index(drop=True)

        execution_df["time"] = pd.to_datetime(execution_df["exec_date"], utc=True).dt.tz_convert(cls.TZ)
        execution_df = execution_df.set_index("time").sort_index()

        group = execution_df.resample(freq)
        ohlc: pd.DataFrame = group["price"].ohlc()
        volume: pd.Series[float] = group["size"].sum().rename("volume")
        sub_klines = pd.concat([ohlc, volume], axis=1)

        klines = cls._create_common_klines(symbol.name, interval, start_date, end_date, sub_klines)
        return klines

    def get_klines(
        self, symbol: Symbols, interval: int, start_date: datetime, end_date: datetime
    ) -> DataFrame[CommonKlineSchema]:

        if start_date >= end_date:
            logger.error(f"Must be start_date < end_date. start_date={start_date}, end_date={end_date}.")
            raise ValueError

        if start_date < datetime.now() - timedelta(days=40):
            logger.error(f"{start_date} is too old.")
            raise ValueError

        if start_date.tzinfo is not None or end_date.tzinfo is not None:
            logger.error("Only tz_naive datetime object can be accepted.")
            raise ValueError

        start_date = start_date.replace(tzinfo=self.TZ)
        end_date = end_date.replace(tzinfo=self.TZ)
        current_date = end_date.replace(tzinfo=self.TZ)

        before = None
        max_executions = 500

        executions: list[BitFlyerExecution] = []
        while current_date > start_date:
            logger.info(f"{current_date} {start_date} {len(executions)}")
            executions_chunk = self.get_executions_by_http(symbol, before=before, max_executions=max_executions)
            executions += executions_chunk

            if len(executions_chunk) != max_executions:
                break

            *_, oldest_execution = executions_chunk
            before = oldest_execution.id
            current_date = pd.Timestamp(oldest_execution.exec_date, tz="UTC").tz_convert(self.TZ)
            time.sleep(self.duration)

        klines = self.convert_executions_to_common_klines(symbol, interval, start_date, end_date, executions)
        return klines
