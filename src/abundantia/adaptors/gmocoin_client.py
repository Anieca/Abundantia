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

    async def get_executions_by_http(self, symbol: str, count: int = 500, max_executions: int = 100_000):
        assert symbol in self.symbols
        count = min(count, max_executions)

        all_executions: list[GMOCoinExecution] = []
        params = {"symbol": symbol}

        async with pybotters.Client(base_url=self.http_url) as client:

            while len(all_executions) < max_executions:
                try:
                    response = await client.get("/v1/trades", params=params)
                    executions = await response.json()
                    executions = executions.get("data", {}).get("list", [])
                    all_executions += [GMOCoinExecution(**e) for e in executions]

                    await asyncio.sleep(1)

                    if len(executions) != count:
                        logger.warn(f"{len(executions)} != {count}.")
                        break

                except Exception:
                    logger.error(traceback.format_exc())
                    break
        return all_executions
