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

            executions = [BitFlyerExecution(**r) for r in result]
            all_executions += executions

            if len(executions) != count:
                logger.warn(f"{len(executions)} != {count}.")
                break

            latest_execution, *_, oldest_execution = executions
            params["before"] = str(oldest_execution.id)
            logger.info(f"{latest_execution.exec_date}, {oldest_execution.exec_date}, {len(all_executions)}")
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

        start_date = cls.convert_aware_datetime(start_date)
        end_date = cls.convert_aware_datetime(end_date)

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

        self._check_invalid_datetime(start_date, end_date)

        start_date = self.convert_aware_datetime(start_date)
        end_date = self.convert_aware_datetime(end_date)
        current_date = end_date

        before = None
        max_executions = 500

        executions: list[BitFlyerExecution] = []
        while current_date > start_date:
            logger.info(f"{start_date} {current_date}")
            executions_chunk = self.get_executions_by_http(symbol, before=before, max_executions=max_executions)
            executions += executions_chunk

            if len(executions_chunk) != max_executions:
                break

            *_, oldest_execution = executions_chunk
            before = oldest_execution.id
            current_date = pd.Timestamp(oldest_execution.exec_date, tz=self.TZ)
            time.sleep(self.duration)

        klines = self.convert_executions_to_common_klines(symbol, interval, start_date, end_date, executions)
        return klines

    @classmethod
    def _check_invalid_datetime(cls, start_date: datetime, end_date: datetime) -> None:
        super()._check_invalid_datetime(start_date, end_date)

        if start_date < datetime.now() - timedelta(days=40):
            logger.error(f"{start_date} is too old.")
            raise ValueError
