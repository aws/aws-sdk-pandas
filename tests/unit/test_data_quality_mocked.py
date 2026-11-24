from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

import awswrangler as wr
from awswrangler import exceptions


@pytest.fixture(autouse=True)
def _no_polling_delay(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("awswrangler.data_quality._utils._RULESET_EVALUATION_WAIT_POLLING_DELAY", 0)


def _mock_glue_client(statuses: list[str]) -> MagicMock:
    client = MagicMock()
    client.start_data_quality_ruleset_evaluation_run.return_value = {"RunId": "run-id"}
    client.start_data_quality_rule_recommendation_run.return_value = {"RunId": "run-id"}
    runs = [{"RunId": "run-id", "Status": s} for s in statuses]
    client.get_data_quality_ruleset_evaluation_run.side_effect = runs
    client.get_data_quality_rule_recommendation_run.side_effect = runs
    return client


@pytest.mark.parametrize(
    "status,expected_exception",
    [
        ("FAILED", exceptions.QueryFailed),
        ("STOPPED", exceptions.QueryCancelled),
        ("TIMEOUT", exceptions.QueryFailed),
    ],
)
def test_evaluate_ruleset_raises_on_final_error_status(status: str, expected_exception: type[Exception]) -> None:
    client = _mock_glue_client(["STARTING", "RUNNING", status])
    with patch("awswrangler._utils.client", return_value=client):
        with pytest.raises(expected_exception):
            wr.data_quality.evaluate_ruleset(
                name="ruleset", iam_role_arn="arn:aws:iam::123456789012:role/role", database="db", table="tbl"
            )
    assert client.get_data_quality_ruleset_evaluation_run.call_count == 3


def test_create_recommendation_ruleset_raises_on_timeout() -> None:
    client = _mock_glue_client(["RUNNING", "TIMEOUT"])
    with patch("awswrangler._utils.client", return_value=client):
        with pytest.raises(exceptions.QueryFailed, match="timed out"):
            wr.data_quality.create_recommendation_ruleset(
                database="db", table="tbl", iam_role_arn="arn:aws:iam::123456789012:role/role"
            )
    assert client.get_data_quality_rule_recommendation_run.call_count == 2
