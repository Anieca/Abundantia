from abundantia.adaptors import GMOCoinClient


def test_get_klines_by_http():
    client = GMOCoinClient()
    klines = client.get_klines_by_http(GMOCoinClient.btc_jpy, "1min", "20210415")
    assert len(klines) == 60 * 24


def test_get_executions_by_http():
    client = GMOCoinClient()
    count = 50
    executions = client.get_executions_by_http(GMOCoinClient.btc_jpy, max_executions=count)
    assert len(executions) == count
