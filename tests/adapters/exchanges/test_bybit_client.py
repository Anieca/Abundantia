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
                interval="1",
                open_time=1651676400,
                open=38773.0,
                high=38789.0,
                low=38773.0,
                close=38779.5,
                volume=181631.0,
                turnover=4.68411369,
            ),
            BybitKline(
                symbol="BTCUSD",
                interval="1",
                open_time=1651676460,
                open=38779.5,
                high=38798.5,
                low=38777.5,
                close=38798.5,
                volume=82025.0,
                turnover=2.11500075,
            ),
            BybitKline(
                symbol="BTCUSD",
                interval="1",
                open_time=1651676520,
                open=38798.5,
                high=38798.5,
                low=38750.0,
                close=38764.5,
                volume=540133.0,
                turnover=13.92999025,
            ),
        ]

        symbol = self.client.symbols.BTCUSD
        interval = 60
        start_date = datetime(2022, 5, 5)
        end_date = datetime(2022, 5, 6)
        klines = self.client.convert_klines_to_common_klines(symbol, interval, start_date, end_date, bybit_klines)

        assert len(klines) == 1440
        assert len(klines.dropna()) == len(bybit_klines)
