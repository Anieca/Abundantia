from abundantia.exchanges.base import BaseClient


class TestBaseClient:
    def test_convert_freq_to_interval(self):
        for freq, interval in [("S", 1), ("T", 60), ("H", 3600)]:
            assert interval == BaseClient.convert_freq_to_interval(freq)

    def test_convert_interval_to_freq(self):
        for interval, freq in [(1, "1S"), (15, "15S"), (60, "1T"), (1800, "30T"), (3600, "1H"), (43200, "12H")]:
            assert freq == BaseClient.convert_interval_to_freq(interval)
