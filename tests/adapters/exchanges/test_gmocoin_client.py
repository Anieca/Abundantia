from datetime import datetime

import pytest

from abundantia.adapters.exchanges.gmocoin_client import GMOCoinClient
from abundantia.schema.gmocoin import GMOCoinExecution


class TestGMOCoinClient:
    def setup_method(self):
        self.client = GMOCoinClient(log_level="WARNING")

    @pytest.mark.exchange
    def test_get_klines_by_http(self):
        klines = self.client.get_klines_by_http(GMOCoinClient.symbols.BTC_JPY, 60, datetime(2021, 4, 15))
        assert len(klines) == 60 * 24

    @pytest.mark.exchange
    def test_get_executions_by_http(self):
        count = 50
        executions = self.client.get_executions_by_http(GMOCoinClient.symbols.BTC_JPY, max_executions=count)
        assert len(executions) == count

    def test_convert_executions_to_common_klines(self):
        executions = [
            GMOCoinExecution(price=5010271.0, side="SELL", size=0.01, timestamp="2022-04-12T14:48:01.828Z"),
            GMOCoinExecution(price=5010271.0, side="BUY", size=0.01, timestamp="2022-04-12T14:46:01.828Z"),
        ]
        symbol = self.client.symbols.BTC_JPY
        interval = 60

        klines = self.client.convert_executions_to_common_klines(symbol, interval, executions, inclusive="neither")
        assert len(klines) == 1

        klines = self.client.convert_executions_to_common_klines(symbol, interval, executions, inclusive="both")
        assert len(klines) == 3

    def test_convert_datetime_to_specific(self):
        assert "20220101" == self.client.convert_datetime_to_specific(datetime(2022, 1, 1))
        assert "20220101" == self.client.convert_datetime_to_specific(datetime(2022, 1, 1, 1, 1, 1))
