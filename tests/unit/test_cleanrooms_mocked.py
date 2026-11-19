from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

import awswrangler as wr
from awswrangler import exceptions


@patch("awswrangler.cleanrooms._utils.time.sleep")
@patch("awswrangler._utils.client")
def test_wait_query_failed_surfaces_error_message(mock_client: MagicMock, _sleep: MagicMock) -> None:
    mock_client.return_value.get_protected_query.return_value = {
        "protectedQuery": {
            "id": "query-id",
            "status": "FAILED",
            "error": {"message": "Column does not exist", "code": "INVALID_INPUT"},
        }
    }

    with pytest.raises(exceptions.QueryFailed, match="Column does not exist"):
        wr.cleanrooms.wait_query(membership_id="membership-id", query_id="query-id")


@patch("awswrangler.cleanrooms._utils.time.sleep")
@patch("awswrangler._utils.client")
def test_wait_query_failed_without_error_block(mock_client: MagicMock, _sleep: MagicMock) -> None:
    mock_client.return_value.get_protected_query.return_value = {
        "protectedQuery": {"id": "query-id", "status": "CANCELLED"}
    }

    with pytest.raises(exceptions.QueryFailed):
        wr.cleanrooms.wait_query(membership_id="membership-id", query_id="query-id")
