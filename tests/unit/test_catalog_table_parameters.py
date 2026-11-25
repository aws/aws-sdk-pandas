from __future__ import annotations

from unittest.mock import MagicMock, patch

import awswrangler as wr


def test_get_table_parameters_without_parameters() -> None:
    # Table.Parameters is optional in the Glue model, so a table created without any must not raise a KeyError.
    client = MagicMock()
    client.get_table.return_value = {"Table": {"Name": "t", "DatabaseName": "d"}}
    with patch("awswrangler._utils.client", return_value=client):
        assert wr.catalog.get_table_parameters(database="d", table="t") == {}


def test_get_table_parameters_with_parameters() -> None:
    client = MagicMock()
    client.get_table.return_value = {"Table": {"Name": "t", "Parameters": {"classification": "csv"}}}
    with patch("awswrangler._utils.client", return_value=client):
        assert wr.catalog.get_table_parameters(database="d", table="t") == {"classification": "csv"}
