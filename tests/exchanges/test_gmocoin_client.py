from datetime import datetime

import pytest

from abundantia.exchanges.gmocoin_client import GMOCoinClient
from abundantia.schema.gmocoin import GMOCoinExecution, GMOCoinKline


class TestGMOCoinClient:
    def setup_method(self):
        self.client = GMOCoinClient()

    @pytest.mark.exchange
    def test_get_klines_by_http(self):
        klines = self.client.get_klines_by_http(GMOCoinClient.SYMBOLS.BTC_JPY, 60, datetime(2021, 4, 15))
        assert len(klines) == 60 * 24

    @pytest.mark.exchange
    def test_get_executions_by_http(self):
        count = 50
        executions = self.client.get_executions_by_http(GMOCoinClient.SYMBOLS.BTC_JPY, max_executions=count)
        assert len(executions) == count

    def test_convert_klines_to_common_klines(self):
        gmo_klines = [
            GMOCoinKline(
                openTime=1649980860000, open=5024175.0, high=5026584.0, low=5021974.0, close=5025210.0, volume=2.22
            ),
            GMOCoinKline(
                openTime=1649980920000, open=5026777.0, high=5027965.0, low=5023210.0, close=5024067.0, volume=1.18
            ),
            GMOCoinKline(
                openTime=1649980980000, open=5025477.0, high=5026676.0, low=5022626.0, close=5026179.0, volume=0.7
            ),
            GMOCoinKline(
                openTime=1649981040000, open=5025704.0, high=5025704.0, low=5024011.0, close=5024164.0, volume=0.5
            ),
            GMOCoinKline(
                openTime=1649981100000, open=5022637.0, high=5023759.0, low=5020401.0, close=5021018.0, volume=2.96
            ),
        ]
        symbol = self.client.SYMBOLS.BTC_JPY
        interval = 60
        start_date = datetime(2022, 4, 15)
        end_date = datetime(2022, 4, 16)
        klines = self.client.convert_klines_to_common_klines(symbol, interval, start_date, end_date, gmo_klines)
        assert len(klines) == 1440
        assert len(klines.dropna()) == len(gmo_klines)

    def test_convert_executions_to_common_klines(self):
        executions = [
            GMOCoinExecution(price=5010271.0, side="SELL", size=0.01, timestamp="2022-04-12T14:48:01.828Z"),
            GMOCoinExecution(price=5010271.0, side="BUY", size=0.01, timestamp="2022-04-12T14:46:01.828Z"),
        ]
        symbol = self.client.SYMBOLS.BTC_JPY
        interval = 60
        start_date = datetime(2022, 4, 12)
        end_date = datetime(2022, 4, 13)

        klines = self.client.convert_executions_to_common_klines(symbol, interval, start_date, end_date, executions)
        assert len(klines) == 1440
        assert len(klines.dropna() == 2)

    def test_convert_datetime_to_specific(self):
        assert "20220101" == self.client.convert_datetime_to_specific(datetime(2022, 1, 1))
        assert "20220101" == self.client.convert_datetime_to_specific(datetime(2022, 1, 1, 1, 1, 1))
