import random
from typing import Any, Dict

import boto3
import modin.pandas as pd
import pytest

import awswrangler as wr

from .._utils import ExecutionTimer


def _generate_item(id: int) -> Dict[str, Any]:
    return {
        "id": str(id),
        "year": random.randint(1923, 2023),
        "title": f"{random.randrange(16**6):06x}",
    }


def _fill_dynamodb_table(table_name: str, num_objects: int) -> None:
    dynamodb_resource = boto3.resource("dynamodb")
    table = dynamodb_resource.Table(table_name)

    with table.batch_writer() as writer:
        for i in range(num_objects):
            item = _generate_item(i)
            writer.put_item(Item=item)


@pytest.mark.parametrize(
    "params",
    [
        {
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}, {"AttributeName": "year", "KeyType": "RANGE"}],
            "AttributeDefinitions": [
                {"AttributeName": "id", "AttributeType": "S"},
                {"AttributeName": "year", "AttributeType": "N"},
            ],
        }
    ],
)
def test_dynamodb_read(params: Dict[str, Any], dynamodb_table: str, request: pytest.FixtureRequest) -> None:
    benchmark_time = 30
    num_objects = 50_000

    _fill_dynamodb_table(dynamodb_table, num_objects)

    with ExecutionTimer(request) as timer:
        frame = wr.dynamodb.read_items(table_name=dynamodb_table, allow_full_scan=True)

    assert len(frame) == num_objects
    assert timer.elapsed_time < benchmark_time


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
    wr.config.log_to_driver = True

    with ExecutionTimer(request) as timer:
        wr.dynamodb.put_df(df=big_modin_df, table_name=dynamodb_table)

    assert timer.elapsed_time < benchmark_time
