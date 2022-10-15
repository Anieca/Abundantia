import os
from datetime import datetime, timedelta, timezone

import pytest

from abundantia.ftx_client import FTXClient, FTXOrderRequestParams


class TestFTXClient:
    @pytest.fixture
    def client(self) -> FTXClient:
        return FTXClient()

    @pytest.fixture
    def authed_client(self) -> FTXClient:
        api_key = os.environ.get("FTX_API_KEY")
        api_secret = os.environ.get("FTX_API_SECRET")
        return FTXClient(api_key, api_secret)

    def test_get(self, client: FTXClient) -> None:
        assert len(client.get("/markets")) > 0

    @pytest.mark.parametrize(
        "start,end,interval",
        (
            (datetime(2022, 1, 1, tzinfo=timezone.utc), datetime(2022, 2, 1, tzinfo=timezone.utc), 60),
            (datetime(2022, 1, 1, tzinfo=timezone.utc), datetime(2022, 2, 1, tzinfo=timezone.utc), 3600),
        ),
    )
    @pytest.mark.slow
    def test_get_klines(self, client: FTXClient, start: datetime, end: datetime, interval: int) -> None:
        klines = client.get_klines("BTC-PERP", interval, start, end)
        s, *_, e = klines

        assert s.startTime == start
        assert e.startTime == end - timedelta(seconds=interval)

    @pytest.mark.parametrize(
        "input,expected",
        (
            (datetime(2022, 1, 1), False),
            (datetime(2022, 1, 1, tzinfo=None), False),
            (datetime(2022, 1, 1, tzinfo=timezone.utc), True),
            (datetime(2022, 1, 1).astimezone(timezone.utc), True),
        ),
    )
    def test_is_aware(self, input: datetime, expected: bool) -> None:
        assert FTXClient.is_aware(input) == expected

    @pytest.mark.parametrize(
        "start,end,interval,expected",
        (
            (datetime(2022, 1, 1), datetime(2022, 2, 1), 60, 30),
            (datetime(2022, 1, 1), datetime(2022, 2, 1), 3600, 1),
        ),
    )
    def test_estimate_num_request(self, start: datetime, end: datetime, interval: int, expected: int) -> None:
        assert FTXClient._estimate_num_request(start, end, interval) == expected  # type: ignore

    @pytest.mark.auth
    def test_get_open_orders(self, authed_client: FTXClient) -> None:
        assert isinstance(authed_client.get_open_orders(), tuple)

    @pytest.mark.auth
    def test_place_order(self, authed_client: FTXClient) -> None:
        params = FTXOrderRequestParams(market="XRP-PERP", side="sell", price=0.6, type="limit", size=1.0)
        order = authed_client.place_order(params)
        assert order.id

    @pytest.mark.auth
    def test_cancel_all_orders(self, authed_client: FTXClient) -> None:
        authed_client.cancel_all_orders("XRP-PERP")
