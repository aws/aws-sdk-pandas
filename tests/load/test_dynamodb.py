import random
from typing import Any, Dict, Optional

import boto3
import modin.pandas as pd
import pytest
import ray

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


def create_big_modin_df(table_size: int, num_blocks: Optional[int]) -> pd.DataFrame:
    pandas_refs = ray.data.range(table_size).to_pandas_refs()
    dataset = ray.data.from_pandas_refs(pandas_refs)

    if num_blocks:
        dataset = dataset.repartition(num_blocks=num_blocks)

    frame = dataset.to_modin()
    frame["foo"] = frame.id * 2
    frame["bar"] = frame.id % 2

    return frame


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
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
            "AttributeDefinitions": [
                {"AttributeName": "id", "AttributeType": "N"},
            ],
        }
    ],
)
@pytest.mark.parametrize("num_blocks", [2, 4, 8, None])
def test_dynamodb_write(
    params: Dict[str, Any],
    num_blocks: int,
    dynamodb_table: str,
    request: pytest.FixtureRequest,
) -> None:
    benchmark_time = 30
    big_modin_df = create_big_modin_df(25_000, num_blocks)

    with ExecutionTimer(request) as timer:
        wr.dynamodb.put_df(df=big_modin_df, table_name=dynamodb_table, use_threads=4)

    assert timer.elapsed_time < benchmark_time

    df_out = wr.dynamodb.read_items(dynamodb_table, allow_full_scan=True)
    assert len(df_out) == len(big_modin_df)
