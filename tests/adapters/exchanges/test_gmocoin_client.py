from datetime import datetime

from abundantia.adapters import GMOCoinClient
from abundantia.schema.gmocoin import GMOCoinExecution

client = GMOCoinClient(log_level="WARNING")


def test_get_klines_by_http():
    klines = client.get_klines_by_http(GMOCoinClient.symbols.BTC_JPY, 60, datetime(2021, 4, 15))
    assert len(klines) == 60 * 24


def test_get_executions_by_http():
    count = 50
    executions = client.get_executions_by_http(GMOCoinClient.symbols.BTC_JPY, max_executions=count)
    assert len(executions) == count


def test_convert_executions_to_common_klines():
    executions = [
        GMOCoinExecution(price=5010271.0, side="SELL", size=0.01, timestamp="2022-04-12T14:48:01.828Z"),
        GMOCoinExecution(price=5010271.0, side="BUY", size=0.01, timestamp="2022-04-12T14:48:01.828Z"),
    ]
    symbol = GMOCoinClient.symbols.BTC_JPY
    interval = 60

    klines = client.convert_executions_to_common_klines(symbol, interval, executions, inclusive="neither")
    assert len(klines) == 0

    klines = client.convert_executions_to_common_klines(symbol, interval, executions, inclusive="both")
    assert len(klines) == 1


def test_convert_datetime_to_specific():
    assert "20220101" == GMOCoinClient.convert_datetime_to_specific(datetime(2022, 1, 1))
    assert "20220101" == GMOCoinClient.convert_datetime_to_specific(datetime(2022, 1, 1, 1, 1, 1))
