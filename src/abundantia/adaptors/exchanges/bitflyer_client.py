from __future__ import annotations

import asyncio
import traceback

import pybotters

from abundantia.schema.bitflyer import BitFlyerExecution
from abundantia.utils import setup_logger

logger = setup_logger(__name__)


class BitFlyerClient:
    http_url: str = "https://api.bitflyer.com"
    ws_url: str = "wss"
    fx_btc_jpy: str = "FX_BTC_JPY"
    btc_jpy: str = "BTC_JPY"
    symbols: tuple[str, ...] = (btc_jpy, fx_btc_jpy)

    async def get_executions_by_http(
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

        async with pybotters.Client(base_url=self.http_url) as client:
            while len(all_executions) < max_executions:
                try:
                    response = await client.get("/v1/executions", params=params)
                    executions = await response.json()
                    all_executions += [BitFlyerExecution(**e) for e in executions]

                    if len(executions) != count:
                        logger.warn(f"{len(executions)} != {count}.")
                        break

                    params["before"] = str(all_executions[-1].id)

                    logger.info(params)
                    logger.info(f"{all_executions[0].id}, {all_executions[-1].id}, {len(all_executions)}")
                    await asyncio.sleep(1)

                except Exception:
                    logger.error(traceback.format_exc())
                    break
        return all_executions
