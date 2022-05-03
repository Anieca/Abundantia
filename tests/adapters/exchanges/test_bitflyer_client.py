from abundantia.adapters import BitFlyerClient
from abundantia.schema.bitflyer import BitFlyerExecution

client = BitFlyerClient(log_level="WARNING")


def test_get_executions_by_http():
    count = 50
    executions = client.get_executions_by_http(BitFlyerClient.symbols.FX_BTC_JPY, max_executions=count)
    assert len(executions) == count


def test_executions_to_kline():
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
    ]
    symbol = BitFlyerClient.symbols.FX_BTC_JPY
    interval = 60

    klines = client.convert_executions_to_common_klines(symbol, interval, executions, inclusive="neither")
    assert len(klines) == 0

    klines = client.convert_executions_to_common_klines(symbol, interval, executions, inclusive="both")
    assert len(klines) == 1
