from abundantia.adaptors import BitFlyerClient


def test_get_executions_by_http():
    client = BitFlyerClient()
    count = 50
    executions = client.get_executions_by_http(BitFlyerClient.btc_jpy, max_executions=count)
    assert len(executions) == count
