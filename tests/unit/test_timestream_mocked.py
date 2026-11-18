"""Mocked unit tests for awswrangler.timestream batch load waiting."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

import awswrangler as wr
from awswrangler import exceptions


def test_wait_batch_load_task_failed_raises_with_error_message() -> None:
    # ErrorMessage is a member of BatchLoadTaskDescription, not of the top-level response.
    client = MagicMock()
    client.describe_batch_load_task.return_value = {
        "BatchLoadTaskDescription": {
            "TaskId": "task-id",
            "TaskStatus": "FAILED",
            "ErrorMessage": "Data source bucket not found",
        }
    }
    with patch("awswrangler._utils.client", return_value=client):
        with pytest.raises(exceptions.TimestreamLoadError, match="Data source bucket not found"):
            wr.timestream.wait_batch_load_task(task_id="task-id", timestream_batch_load_wait_polling_delay=0)
