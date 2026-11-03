from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

import awswrangler as wr
from awswrangler import exceptions


def _mock_logs_client(statuses: list[str]) -> MagicMock:
    client = MagicMock()
    client.get_query_results.side_effect = [{"status": s, "results": []} for s in statuses]
    return client


@pytest.mark.parametrize(
    "status,expected_exception",
    [
        ("Failed", exceptions.QueryFailed),
        ("Cancelled", exceptions.QueryCancelled),
        ("Timeout", exceptions.QueryFailed),
    ],
)
def test_wait_query_raises_on_final_error_state(status: str, expected_exception: type[Exception]) -> None:
    client = _mock_logs_client(["Scheduled", "Running", status])
    with patch("awswrangler.cloudwatch._utils.client", return_value=client):
        with pytest.raises(expected_exception):
            wr.cloudwatch.wait_query(query_id="query-id", cloudwatch_query_wait_polling_delay=0)
    assert client.get_query_results.call_count == 3


def test_wait_query_returns_on_complete() -> None:
    client = _mock_logs_client(["Running", "Complete"])
    with patch("awswrangler.cloudwatch._utils.client", return_value=client):
        response = wr.cloudwatch.wait_query(query_id="query-id", cloudwatch_query_wait_polling_delay=0)
    assert response["status"] == "Complete"
    assert client.get_query_results.call_count == 2
