from abundantia.adaptors.databases.sqlite_client import SQLiteClient
from abundantia.schema.common import CommonKlineModel


def test_select():
    sqlite = SQLiteClient()
    results = sqlite.select_common_klines()

    for result in results:
        assert isinstance(result, CommonKlineModel)
