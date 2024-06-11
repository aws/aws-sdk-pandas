from __future__ import annotations

from typing import Any, Iterator

import boto3
import pytest

import awswrangler as wr
import awswrangler.pandas as pd

from .._utils import (
    get_time_str_with_random_suffix,
)


@pytest.fixture(scope="session")
def lock_dynamodb_table() -> Iterator[str]:
    name = f"deltalake_lock_{get_time_str_with_random_suffix()}"
    print(f"Table name: {name}")

    dynamodb_client = boto3.client("dynamodb")
    dynamodb_client.create_table(
        TableName=name,
        BillingMode="PAY_PER_REQUEST",
        KeySchema=[
            {"AttributeName": "tablePath", "KeyType": "HASH"},
            {"AttributeName": "fileName", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "tablePath", "AttributeType": "S"},
            {"AttributeName": "fileName", "AttributeType": "S"},
        ],
    )

    dynamodb_client.get_waiter("table_exists").wait(TableName=name)

    yield name

    dynamodb_client.delete_table(TableName=name)
    dynamodb_client.get_waiter("table_not_exists").wait(TableName=name)
    print(f"Table {name} deleted.")


@pytest.fixture(params=["no_lock", "dynamodb_lock"], scope="session")
def lock_settings(request: pytest.FixtureRequest) -> dict[str, Any]:
    if request.param == "no_lock":
        return dict(s3_allow_unsafe_rename=True)
    else:
        return dict(lock_dynamodb_table=request.getfixturevalue("lock_dynamodb_table"))


@pytest.mark.parametrize("s3_additional_kwargs", [None, {"ServerSideEncryption": "AES256"}])
@pytest.mark.parametrize(
    "pyarrow_additional_kwargs", [{"safe": True, "deduplicate_objects": False, "types_mapper": None}]
)
def test_read_deltalake(
    path: str,
    lock_settings: dict[str, Any],
    s3_additional_kwargs: dict[str, Any] | None,
    pyarrow_additional_kwargs: dict[str, Any],
) -> None:
    df = pd.DataFrame({"c0": [1, 2, 3], "c1": ["foo", None, "bar"], "c2": [3.0, 4.0, 5.0], "c3": [True, False, None]})
    wr.s3.to_deltalake(path=path, df=df, s3_additional_kwargs=s3_additional_kwargs, **lock_settings)

    df2 = wr.s3.read_deltalake(
        path=path, s3_additional_kwargs=s3_additional_kwargs, pyarrow_additional_kwargs=pyarrow_additional_kwargs
    )
    assert df2.equals(df)


@pytest.mark.parametrize("pyarrow_additional_kwargs", [{"types_mapper": None}])
def test_read_deltalake_versioned(
    path: str, lock_settings: dict[str, Any], pyarrow_additional_kwargs: dict[str, Any]
) -> None:
    df = pd.DataFrame({"c0": [1, 2, 3], "c1": ["foo", "baz", "bar"]})
    wr.s3.to_deltalake(path=path, df=df, **lock_settings)

    df2 = wr.s3.read_deltalake(path=path, pyarrow_additional_kwargs=pyarrow_additional_kwargs)
    assert df2.equals(df)

    df["c2"] = [True, False, True]
    wr.s3.to_deltalake(path=path, df=df, mode="overwrite", schema_mode="overwrite", **lock_settings)

    df3 = wr.s3.read_deltalake(path=path, version=0, pyarrow_additional_kwargs=pyarrow_additional_kwargs)
    assert df3.equals(df.drop("c2", axis=1))

    df4 = wr.s3.read_deltalake(path=path, version=1, pyarrow_additional_kwargs=pyarrow_additional_kwargs)
    assert df4.equals(df)


def test_read_deltalake_partitions(path: str, lock_settings: dict[str, Any]) -> None:
    df = pd.DataFrame({"c0": [1, 2, 3], "c1": [True, False, True], "par0": ["foo", "foo", "bar"], "par1": [1, 2, 2]})
    wr.s3.to_deltalake(path=path, df=df, partition_cols=["par0", "par1"], **lock_settings)

    df2 = wr.s3.read_deltalake(path=path, columns=["c0"], partitions=[("par0", "=", "foo"), ("par1", "=", "1")])
    assert df2.shape == (1, 1)
