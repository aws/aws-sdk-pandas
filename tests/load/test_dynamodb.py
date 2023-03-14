from typing import Any, Dict

import modin.pandas as pd
import pytest

import awswrangler as wr

from .._utils import ExecutionTimer


@pytest.mark.parametrize(
    "params",
    [
        {
            "KeySchema": [{"AttributeName": "value", "KeyType": "HASH"}],
            "AttributeDefinitions": [
                {"AttributeName": "value", "AttributeType": "N"},
            ],
        }
    ],
)
def test_dynamodb_write(
    params: Dict[str, Any], dynamodb_table: str, big_modin_df: pd.DataFrame, request: pytest.FixtureRequest
) -> None:
    benchmark_time = 30

    with ExecutionTimer(request) as timer:
        wr.dynamodb.put_df(df=big_modin_df, table_name=dynamodb_table)

    assert timer.elapsed_time < benchmark_time
