from __future__ import annotations

import re
from unittest.mock import MagicMock

import pandas as pd

import awswrangler as wr
from awswrangler.data_api.rds import RdsDataApi


def test_rds_to_sql_binds_columns_with_spaces_and_hyphens() -> None:
    con = MagicMock(spec=RdsDataApi)
    con.execute.return_value = pd.DataFrame({"TABLE_NAME": ["t"]})  # table exists, so no CREATE TABLE path

    wr.data_api.rds.to_sql(
        df=pd.DataFrame({"first name": ["Ada"], "last-name": ["Lovelace"]}),
        con=con,
        table="t",
        database="db",
    )

    batch = con.batch_execute.call_args.kwargs
    placeholder_names = re.findall(r":(\w+)", batch["sql"])
    parameters = {parameter["name"]: parameter["value"] for parameter in batch["parameter_sets"][0]}
    assert [parameters.get(name) for name in placeholder_names] == [
        {"stringValue": "Ada"},
        {"stringValue": "Lovelace"},
    ]
