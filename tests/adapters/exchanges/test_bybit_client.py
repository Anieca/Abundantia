from datetime import datetime

import pytest

from abundantia.adapters.exchanges.bybit_client import BybitInversePerpetualClient
from abundantia.schema.bybit import BybitKline


class TestBybitInversePerpetualClient:
    def setup_method(self):
        self.client = BybitInversePerpetualClient()

    @pytest.mark.exchange
    def test_get_klines_by_http(self):
        klines = self.client.get_klines_by_http(self.client.symbols.BTCUSD, 60, datetime(2022, 5, 6))
        assert len(klines) == 200

    def test_convert_klines_to_common_klines(self):
        bybit_klines = [
            BybitKline(
                symbol="BTCUSD",
                interval="D",
                open_time=1651795200,
                open=36511.0,
                high=36649.0,
                low=35204.5,
                close=35992.0,
                volume=2096797281.0,
                turnover=58289.36510664,
            ),
            BybitKline(
                symbol="BTCUSD",
                interval="D",
                open_time=1651881600,
                open=35992.0,
                high=36044.5,
                low=35714.0,
                close=35762.0,
                volume=83123018.0,
                turnover=2316.31668545,
            ),
        ]

        klines = self.client.convert_klines_to_common_klines(bybit_klines)
        assert len(klines) == len(bybit_klines)
