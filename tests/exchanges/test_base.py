from datetime import datetime

from dateutil.tz import gettz

from abundantia.exchanges.base import BaseClient


class TestBaseClient:
    def test_convert_freq_to_interval(self):
        for freq, interval in [("S", 1), ("T", 60), ("H", 3600)]:
            assert interval == BaseClient.convert_freq_to_interval(freq)

    def test_convert_interval_to_freq(self):
        for interval, freq in [(1, "1S"), (15, "15S"), (60, "1T"), (1800, "30T"), (3600, "1H"), (43200, "12H")]:
            assert freq == BaseClient.convert_interval_to_freq(interval)

    def test_is_aware(self):
        assert BaseClient.is_aware(datetime(2022, 5, 5, tzinfo=gettz()))
        assert not BaseClient.is_aware(datetime(2022, 5, 5))

    def test_check_invalid_datetime(self):
        start_date = datetime(2022, 5, 7)
        end_date = datetime(2022, 5, 6)
        try:
            BaseClient._check_invalid_datetime(start_date, end_date)
        except ValueError:
            assert True
        except Exception:
            assert False
