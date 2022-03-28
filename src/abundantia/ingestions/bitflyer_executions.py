import asyncio
import traceback
from logging import getLogger

import pybotters
from abundantia.adaptors.csv import CSVClient
from abundantia.schema.bitflyer import BitFlyer

logger = getLogger(__name__)


async def http_executions(
    output_fname: str,
    max_executions: int = 100_000,
    before: str | None = None,
    count: int = 500,
):
    """
    before は含まない
    """

    bitflyer = BitFlyer()
    all_executions: list[dict] = []
    params = {"product_code": bitflyer.fx_btc_jpy, "count": count}

    if before is not None:
        params["before"] = before

    async with pybotters.Client(base_url=bitflyer.http_url) as client:
        while len(all_executions) < max_executions:
            try:
                response = await client.get("/v1/executions", params=params)
                executions = await response.json()
                all_executions += executions

                oldest_execution = all_executions[-1]
                params["before"] = oldest_execution["id"]

                logger.info(f"{oldest_execution['id']}, {oldest_execution['exec_date']}, {len(all_executions)}")
                await asyncio.sleep(1)

            except Exception:
                logger.error(traceback.format_exc())
                break

    CSVClient.dump(output_fname, all_executions)


if __name__ == "__main__":
    asyncio.run(http_executions("./out1.csv"))
