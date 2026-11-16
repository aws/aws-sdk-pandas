from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from awswrangler.data_api import redshift


def _waiter(*responses: dict[str, Any]) -> redshift.RedshiftDataApiWaiter:
    client = MagicMock()
    client.describe_statement.side_effect = list(responses)
    return redshift.RedshiftDataApiWaiter(client, sleep=0, backoff=1, retries=5)


def test_wait_returns_true_when_finished() -> None:
    waiter = _waiter({"Id": "id", "Status": "STARTED"}, {"Id": "id", "Status": "FINISHED"})

    with patch("time.sleep"):
        assert waiter.wait("id") is True


def test_wait_raises_with_error_message_when_failed() -> None:
    waiter = _waiter({"Id": "id", "Status": "FAILED", "Error": "ERROR: relation does not exist"})

    with pytest.raises(redshift.RedshiftDataApiFailedException, match="FAILED.*relation does not exist"):
        waiter.wait("id")


def test_wait_raises_failed_exception_when_aborted_without_error() -> None:
    # A statement cancelled with CancelStatement ends as ABORTED and the
    # DescribeStatement response does not have to include `Error`.
    waiter = _waiter({"Id": "id", "Status": "ABORTED"})

    with pytest.raises(redshift.RedshiftDataApiFailedException, match="ABORTED"):
        waiter.wait("id")
