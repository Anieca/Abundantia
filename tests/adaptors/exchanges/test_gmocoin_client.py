from abundantia.adaptors import GMOCoinClient
from abundantia.schema.gmocoin import GMOCoinExecution

client = GMOCoinClient(log_level="WARNING")


def test_get_klines_by_http():
    klines = client.get_klines_by_http(GMOCoinClient.btc_jpy, "1min", "20210415")
    assert len(klines) == 60 * 24


def test_get_executions_by_http():
    count = 50
    executions = client.get_executions_by_http(GMOCoinClient.btc_jpy, max_executions=count)
    assert len(executions) == count


def test_convert_executions_to_common_klines():
    executions = [
        GMOCoinExecution(price=5010271.0, side="SELL", size=0.01, timestamp="2022-04-12T14:48:01.828Z"),
        GMOCoinExecution(price=5010271.0, side="BUY", size=0.01, timestamp="2022-04-12T14:48:01.828Z"),
    ]
    symbol = GMOCoinClient.btc_jpy

    klines = client.convert_executions_to_common_klines(symbol, executions, inclusive="neither")
    assert len(klines) == 0

    klines = client.convert_executions_to_common_klines(symbol, executions, inclusive="both")
    assert len(klines) == 1
