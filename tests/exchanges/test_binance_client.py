from datetime import datetime

import pytest

from abundantia.exchanges.binance_client import BinanceClient
from abundantia.schema.binance import BinanceKline


class TestBinanceClient:
    def setup_method(self):
        self.client = BinanceClient()
        self.symbol = self.client.SYMBOLS.BTCUSDT

    @pytest.mark.exchange
    def test_get_klines_by_http_01(self):
        start_date = datetime(2022, 5, 5)
        end_date = datetime(2022, 5, 7)
        interval = 60

        klines = self.client.get_klines_by_http(self.symbol, interval, start_date, end_date)
        assert len(klines) == 1000

        oldest_kline, *_ = klines
        assert datetime.fromtimestamp(oldest_kline.open_time / 1000) == start_date

    def test_convert_klines_to_common_klines(self):
        binance_klines = [
            BinanceKline(
                open_time=1651676400000,
                open=38786.22,
                high=38798.87,
                low=38783.08,
                close=38785.57,
                volume=13.36026,
                close_time=1651676459999,
                quote_asset_volume=518265.7514185,
                number_of_trade=577,
                taker_buy_base_asset_volume=3.87814,
                taker_buy_quote_asset_volume=150429.3369944,
                ignore="0",
            ),
            BinanceKline(
                open_time=1651676460000,
                open=38785.57,
                high=38807.33,
                low=38778.57,
                close=38805.98,
                volume=17.70655,
                close_time=1651676519999,
                quote_asset_volume=686897.7865447,
                number_of_trade=942,
                taker_buy_base_asset_volume=7.95744,
                taker_buy_quote_asset_volume=308705.9118426,
                ignore="0",
            ),
            BinanceKline(
                open_time=1651676520000,
                open=38805.97,
                high=38805.98,
                low=38756.24,
                close=38766.73,
                volume=33.81048,
                close_time=1651676579999,
                quote_asset_volume=1310932.1289653,
                number_of_trade=1164,
                taker_buy_base_asset_volume=11.05837,
                taker_buy_quote_asset_volume=428702.8474994,
                ignore="0",
            ),
        ]

        start_date = datetime(2022, 5, 5)
        end_date = datetime(2022, 5, 6)
        interval = 60

        klines = self.client.convert_klines_to_common_klines(
            self.symbol, interval, start_date, end_date, binance_klines
        )
        print(klines)
        assert len(klines) == 1440
        assert len(klines.dropna()) == len(binance_klines)
