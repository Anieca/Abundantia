from datetime import datetime

from abundantia.adapters.exchanges.ftx_client import FTXClient
from abundantia.schema.ftx import FTXKline


class TestFTXClient:
    def setup_method(self):
        self.client = FTXClient()

    def test_convert_klines_to_common_klines(self):
        ftx_klines = [
            FTXKline(
                startTime="2022-05-06T14:00:00+00:00",
                time=1651845600000,
                open=35732.0,
                high=35816.0,
                low=35722.0,
                close=35808.0,
                volume=7212311.1769,
            ),
            FTXKline(
                startTime="2022-05-06T14:01:00+00:00",
                time=1651845660000,
                open=35812.0,
                high=35956.0,
                low=35796.0,
                close=35842.0,
                volume=11387116.6112,
            ),
            FTXKline(
                startTime="2022-05-06T14:02:00+00:00",
                time=1651845720000,
                open=35842.0,
                high=35842.0,
                low=35616.0,
                close=35631.0,
                volume=13397713.1163,
            ),
        ]
        symbol = self.client.symbols.BTC_PERP
        interval = 60
        start_date = datetime(2022, 5, 6)
        end_date = datetime(2022, 5, 7)
        klines = self.client.convert_klines_to_common_klines(symbol, interval, start_date, end_date, ftx_klines)
        assert len(klines) == 1440
        assert len(klines.dropna()) == len(ftx_klines)
