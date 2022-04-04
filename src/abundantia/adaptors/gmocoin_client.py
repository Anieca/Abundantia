import asyncio
import traceback

import pybotters

from abundantia.schema.gmocoin import GMOCoinExecution
from abundantia.utils import setup_logger

logger = setup_logger(__name__)


class GMOCoinClient:
    http_url: str = "https://api.coin.z.com/public"
    ws_url: str = "wss://api.coin.z.com/ws/"
    btc_jpy: str = "BTC_JPY"
    btc: str = "BTC"
    symbols: tuple[str, ...] = (btc_jpy, btc)

    async def get_executions_by_http(
        self, symbol: str, page: int = 1, count: int = 100, max_executions: int = 100_000
    ) -> list[GMOCoinExecution]:

        assert symbol in self.symbols
        count = min(count, max_executions)

        all_executions: list[GMOCoinExecution] = []
        params = {"symbol": symbol, "page": page, "count": count}

        async with pybotters.Client(base_url=self.http_url) as client:
            while len(all_executions) < max_executions:
                try:
                    response = await client.get("/v1/trades", params=params)
                    response = await response.json()
                    data = response.get("data", {})
                    executions = data.get("list", [])
                    all_executions += [GMOCoinExecution(**e) for e in executions]
                    current_page = data.get("pagination", {}).get("currentPage", None)

                    if current_page is None:
                        logger.warn(f"pagination error. {data}")
                        break

                    if len(executions) != count:
                        logger.warn(f"{len(executions)} != {count}.")
                        break

                    params["page"] = current_page + 1

                    logger.info(params)
                    logger.info(f"{all_executions[0].timestamp}, {all_executions[-1].timestamp}, {len(all_executions)}")
                    await asyncio.sleep(1)

                except Exception:
                    logger.error(traceback.format_exc())
                    break
        return all_executions
