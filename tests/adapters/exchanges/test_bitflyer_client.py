import pytest

from abundantia.adapters import BitFlyerClient
from abundantia.schema.bitflyer import BitFlyerExecution


class TestBitflyerClient:
    def setup_method(self):
        self.client = BitFlyerClient(log_level="WARNING")

    @pytest.mark.exchange
    def test_get_executions_by_http(self):
        count = 50
        executions = self.client.get_executions_by_http(BitFlyerClient.symbols.FX_BTC_JPY, max_executions=count)
        assert len(executions) == count

    @pytest.mark.exchange
    def test_executions_to_kline(self):
        executions = [
            BitFlyerExecution(
                id=2325998351,
                side="BUY",
                price=4994488.0,
                size=0.16,
                exec_date="2022-04-12T14:29:22.087",
                buy_child_order_acceptance_id="JRF20220412-142921-030158",
                sell_child_order_acceptance_id="JRF20220412-142920-012835",
            ),
            BitFlyerExecution(
                id=2325998350,
                side="BUY",
                price=4994485.0,
                size=0.02,
                exec_date="2022-04-12T14:29:22.087",
                buy_child_order_acceptance_id="JRF20220412-142921-030158",
                sell_child_order_acceptance_id="JRF20220412-142921-012850",
            ),
            BitFlyerExecution(
                id=2325998349,
                side="BUY",
                price=4994425.0,
                size=0.02,
                exec_date="2022-04-12T14:29:02.087",
                buy_child_order_acceptance_id="JRF20220412-142921-030158",
                sell_child_order_acceptance_id="JRF20220412-142921-012850",
            ),
            BitFlyerExecution(
                id=2325998348,
                side="BUY",
                price=4994415.0,
                size=0.02,
                exec_date="2022-04-12T14:28:52.087",
                buy_child_order_acceptance_id="JRF20220412-142921-030158",
                sell_child_order_acceptance_id="JRF20220412-142921-012850",
            ),
        ]
        symbol = BitFlyerClient.symbols.FX_BTC_JPY
        interval = 60

        klines = self.client.convert_executions_to_common_klines(symbol, interval, executions, inclusive="neither")
        assert len(klines) == 0

        klines = self.client.convert_executions_to_common_klines(symbol, interval, executions, inclusive="both")
        assert len(klines) == 2
