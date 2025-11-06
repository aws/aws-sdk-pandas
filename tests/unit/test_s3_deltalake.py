from __future__ import annotations

from typing import Any, Iterator, Iterable

import boto3
import pyarrow as pa
import pytest
from pandas.testing import assert_frame_equal

import awswrangler as wr
import awswrangler.pandas as pd
from awswrangler.s3._write_deltalake import _df_iter_to_record_batch_reader

from .._utils import (
    get_time_str_with_random_suffix,
)


def assert_df_equal_unordered(left: pd.DataFrame, right: pd.DataFrame, by: list[str]) -> None:
    """Compare two dataframes ignoring row order and dtypes."""
    l2 = left.sort_values(by).reset_index(drop=True)
    r2 = right.sort_values(by).reset_index(drop=True)

    assert_frame_equal(l2, r2, check_dtype=False, check_like=True)


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


@pytest.mark.parametrize("chunksize", [2, 10])
def test_to_deltalake_streaming_single_commit_overwrite(
    path: str,
    lock_settings: dict[str, Any],
    chunksize: int,
) -> None:
    df1 = pd.DataFrame({"c0": [1, 1], "c1": [10, 11], "v": [100, 200]})
    df2 = pd.DataFrame({"c0": [2, 2], "c1": [12, 13], "v": [300, 400]})

    def dfs() -> Iterable[pd.DataFrame]:
        yield df1
        yield df2

    wr.s3.to_deltalake_streaming(
        dfs=dfs(),
        path=path,
        mode="overwrite",
        partition_cols=["c0", "c1"],
        **lock_settings,
    )

    out = wr.s3.read_deltalake(path=path)

    expected = pd.concat([df1, df2], ignore_index=True)
    assert_df_equal_unordered(expected, out, by=["c0", "c1", "v"])


def test_to_deltalake_streaming_creates_one_version_per_run(
    path: str,
    lock_settings: dict[str, Any],
) -> None:
    df_run1_a = pd.DataFrame({"c0": [1], "c1": [10], "v": [111]})
    df_run1_b = pd.DataFrame({"c0": [1], "c1": [11], "v": [112]})

    wr.s3.to_deltalake_streaming(
        dfs=[df_run1_a, df_run1_b],
        path=path,
        mode="overwrite",
        partition_cols=["c0", "c1"],
        **lock_settings,
    )

    run1_expected = pd.concat([df_run1_a, df_run1_b], ignore_index=True)
    latest_v0 = wr.s3.read_deltalake(path=path)
    assert_df_equal_unordered(run1_expected, latest_v0, by=["c0", "c1", "v"])

    df_run2_a = pd.DataFrame({"c0": [2], "c1": [12], "v": [221]})
    df_run2_b = pd.DataFrame({"c0": [2], "c1": [13], "v": [222]})

    wr.s3.to_deltalake_streaming(
        dfs=[df_run2_a, df_run2_b],
        path=path,
        mode="overwrite",
        partition_cols=["c0", "c1"],
        **lock_settings,
    )

    v0 = wr.s3.read_deltalake(path=path, version=0)
    v1 = wr.s3.read_deltalake(path=path, version=1)
    run2_expected = pd.concat([df_run2_a, df_run2_b], ignore_index=True)

    assert_df_equal_unordered(run1_expected, v0, by=["c0", "c1", "v"])
    assert_df_equal_unordered(run2_expected, v1, by=["c0", "c1", "v"])


def test_to_deltalake_streaming_partitions_and_filters(
    path: str,
    lock_settings: dict[str, Any],
) -> None:
    df1 = pd.DataFrame({"c0": [1, 1, 2], "c1": [10, 11, 12], "v": [1, 2, 3]})
    df2 = pd.DataFrame({"c0": [2, 3, 3], "c1": [13, 14, 15], "v": [4, 5, 6]})

    wr.s3.to_deltalake_streaming(
        dfs=[df1, df2],
        path=path,
        mode="overwrite",
        partition_cols=["c0", "c1"],
        **lock_settings,
    )

    only_c02 = wr.s3.read_deltalake(
        path=path,
        partitions=[("c0", "=", "2")],
        columns=["v", "c1"],
    )
    assert set(only_c02["c1"].tolist()) == {12, 13}
    assert sorted(only_c02["v"].tolist()) == [3, 4]


def test_to_deltalake_streaming_empty_iterator_is_noop(
    path: str,
    lock_settings: dict[str, Any],
) -> None:
    wr.s3.to_deltalake_streaming(
        dfs=[pd.DataFrame({"c0": [1], "c1": [1], "v": [1]})],
        path=path,
        mode="overwrite",
        partition_cols=["c0", "c1"],
        **lock_settings,
    )
    baseline = wr.s3.read_deltalake(path=path)

    def empty() -> Iterator[pd.DataFrame]:
        if False:
            yield pd.DataFrame()  # pragma: no cover

    wr.s3.to_deltalake_streaming(
        dfs=empty(),
        path=path,
        mode="overwrite",
        partition_cols=["c0", "c1"],
        **lock_settings,
    )
    after = wr.s3.read_deltalake(path=path)
    assert after.equals(baseline)


def test_df_iter_to_record_batch_reader_schema_and_rows() -> None:
    df_empty = pd.DataFrame({"a": [], "b": []})
    df1 = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    df2 = pd.DataFrame({"a": [3], "b": ["z"]})

    reader, schema = _df_iter_to_record_batch_reader(
        df_iter=[df_empty, df1, df2],
        index=False,
        dtype={},
        target_schema=None,
        batch_size=None,
    )

    assert isinstance(schema, pa.Schema)
    assert {f.name for f in schema} == {"a", "b"}

    table: pa.Table = reader.read_all()
    pdf = table.to_pandas()
    assert len(pdf) == 3
    assert sorted(pdf["a"].tolist()) == [1, 2, 3]
    assert set(pdf["b"].tolist()) == {"x", "y", "z"}


def test_df_iter_to_record_batch_reader_respects_batch_size() -> None:
    df1 = pd.DataFrame({"a": list(range(5)), "b": ["x"] * 5})
    df2 = pd.DataFrame({"a": list(range(5, 9)), "b": ["y"] * 4})

    reader, _ = _df_iter_to_record_batch_reader(
        df_iter=[df1, df2],
        index=False,
        dtype={},
        target_schema=None,
        batch_size=3,
    )

    batch_count = 0
    row_count = 0
    for batch in reader:
        batch_count += 1
        row_count += batch.num_rows

    assert batch_count >= 3
    assert row_count == 9
