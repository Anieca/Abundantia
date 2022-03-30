from __future__ import annotations

import asyncio
import traceback

import pybotters

from abundantia.schema.bitflyer import BitFlyer, BitFlyerExecution
from abundantia.utils import setup_logger

logger = setup_logger(__name__)


class BitFlyerClient:
    bitflyer = BitFlyer()

    async def get_executions_by_http(
        self,
        before: int | str | None = None,
        after: int | str | None = None,
        count: int = 500,
        max_executions: int = 100_000,
    ) -> list[BitFlyerExecution]:
        """
        before は含まない
        """
        count = min(count, max_executions)

        all_executions: list[BitFlyerExecution] = []
        params = {"product_code": self.bitflyer.fx_btc_jpy, "count": count}

        if before is not None:
            params["before"] = before
        if after is not None:
            params["after"] = after

        async with pybotters.Client(base_url=self.bitflyer.http_url) as client:
            while len(all_executions) < max_executions:
                try:
                    response = await client.get("/v1/executions", params=params)
                    executions = await response.json()
                    all_executions += [BitFlyerExecution(**e) for e in executions]

                    params["before"] = all_executions[-1].id

                    logger.info(params)
                    logger.info(f"{all_executions[0].id}, {all_executions[-1].id}, {len(all_executions)}")
                    await asyncio.sleep(1)

                    if len(executions) != count:
                        logger.warn(f"{len(executions)} != {count}.")
                        break

                except Exception:
                    logger.error(traceback.format_exc())
                    break
        return all_executions
