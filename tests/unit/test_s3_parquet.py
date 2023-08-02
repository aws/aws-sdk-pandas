# mypy: disable-error-code=no-untyped-def

import itertools
import logging
import math
from datetime import date, datetime, timedelta, timezone
from typing import List, Optional, Union

import boto3
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import awswrangler as wr
import awswrangler.pandas as pd
from awswrangler._distributed import MemoryFormatEnum

from .._utils import (
    assert_pandas_equals,
    ensure_data_types,
    get_df_list,
    is_ray_modin,
    to_pandas,
)

logging.getLogger("awswrangler").setLevel(logging.DEBUG)

pytestmark = pytest.mark.distributed


@pytest.mark.parametrize("partition_cols", [None, ["c2"], ["c1", "c2"]])
def test_parquet_metadata_partitions_dataset(path, partition_cols):
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [3, 4, 5], "c2": [6, 7, 8]})
    wr.s3.to_parquet(df=df, path=path, dataset=True, partition_cols=partition_cols)
    columns_types, partitions_types = wr.s3.read_parquet_metadata(path=path, dataset=True)
    partitions_types = partitions_types if partitions_types is not None else {}
    assert len(columns_types) + len(partitions_types) == len(df.columns)
    assert columns_types.get("c0") == "bigint"
    assert (columns_types.get("c1") == "bigint") or (partitions_types.get("c1") == "string")
    assert (columns_types.get("c1") == "bigint") or (partitions_types.get("c1") == "string")


def test_read_parquet_metadata_nulls(path):
    df = pd.DataFrame({"c0": [None, None, None], "c1": [1, 2, 3], "c2": ["a", "b", "c"]})
    path = f"{path}df.parquet"
    wr.s3.to_parquet(df, path)
    with pytest.raises(wr.exceptions.UndetectedType):
        wr.s3.read_parquet_metadata(path)
    columns_types, _ = wr.s3.read_parquet_metadata(path, ignore_null=True)
    assert len(columns_types) == len(df.columns)
    assert columns_types.get("c0") == ""
    assert columns_types.get("c1") == "bigint"
    assert columns_types.get("c2") == "string"


@pytest.mark.parametrize(
    "partition_cols",
    [
        ["c2"],
        ["value", "c2"],
        pytest.param(
            None,
            marks=pytest.mark.xfail(
                is_ray_modin,
                raises=TypeError,
                reason="Broken sort_values in Modin",
            ),
        ),
    ],
)
def test_parquet_cast_string_dataset(path, partition_cols):
    df = pd.DataFrame({"id": [1, 2, 3], "value": ["foo", "boo", "bar"], "c2": [4, 5, 6], "c3": [7.0, 8.0, 9.0]})
    wr.s3.to_parquet(df, path, dataset=True, partition_cols=partition_cols, dtype={"id": "string", "c3": "string"})
    df2 = wr.s3.read_parquet(path, dataset=True).sort_values("id", ignore_index=True)
    assert str(df2.id.dtypes) == "string"
    assert str(df2.c3.dtypes) == "string"
    assert df.shape == df2.shape
    for col, row in tuple(itertools.product(df.columns, range(3))):
        assert str(df[col].iloc[row]) == str(df2[col].iloc[row])


@pytest.mark.parametrize("use_threads", [True, False, 2])
def test_read_parquet_filter_partitions(path, use_threads):
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [0, 1, 2], "c2": [0, 0, 1]})
    wr.s3.to_parquet(df, path, dataset=True, partition_cols=["c1", "c2"], use_threads=use_threads)
    df2 = wr.s3.read_parquet(
        path, dataset=True, partition_filter=lambda x: True if x["c1"] == "0" else False, use_threads=use_threads
    )
    assert df2.shape == (1, 3)
    assert df2.c0.iloc[0] == 0
    assert df2.c1.astype(int).iloc[0] == 0
    assert df2.c2.astype(int).iloc[0] == 0
    df2 = wr.s3.read_parquet(
        path,
        dataset=True,
        partition_filter=lambda x: True if x["c1"] == "1" and x["c2"] == "0" else False,
        use_threads=use_threads,
    )
    assert df2.shape == (1, 3)
    assert df2.c0.iloc[0] == 1
    assert df2.c1.astype(int).iloc[0] == 1
    assert df2.c2.astype(int).iloc[0] == 0
    df2 = wr.s3.read_parquet(
        path, dataset=True, partition_filter=lambda x: True if x["c2"] == "0" else False, use_threads=use_threads
    )
    assert df2.shape == (2, 3)
    assert df2.c0.astype(int).sum() == 1
    assert df2.c1.astype(int).sum() == 1
    assert df2.c2.astype(int).sum() == 0


def test_read_parquet_table(path, glue_database, glue_table):
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [0, 1, 2], "c2": [0, 0, 1]})
    wr.s3.to_parquet(
        df,
        path,
        dataset=True,
        database=glue_database,
        table=glue_table,
    )
    df_out = wr.s3.read_parquet_table(table=glue_table, database=glue_database)
    assert df_out.shape == (3, 3)


def test_read_parquet_table_filter_partitions(path, glue_database, glue_table):
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [0, 1, 2], "c2": [0, 0, 1]})
    wr.s3.to_parquet(
        df,
        path,
        dataset=True,
        partition_cols=["c1", "c2"],
        database=glue_database,
        table=glue_table,
    )
    df_out = wr.s3.read_parquet_table(
        table=glue_table,
        database=glue_database,
        partition_filter=lambda x: True if x["c1"] == "0" else False,
    )
    assert df_out.shape == (1, 3)
    assert df_out.c0.astype(int).sum() == 0
    with pytest.raises(wr.exceptions.NoFilesFound):
        wr.s3.read_parquet_table(
            table=glue_table,
            database=glue_database,
            partition_filter=lambda x: True if x["c1"] == "3" else False,
        )


@pytest.mark.xfail(
    is_ray_modin, raises=wr.exceptions.InvalidArgument, reason="kwargs not supported in distributed mode"
)
def test_parquet(path):
    df_file = pd.DataFrame({"id": [1, 2, 3]})
    path_file = f"{path}test_parquet_file.parquet"
    df_dataset = pd.DataFrame({"id": [1, 2, 3], "partition": ["A", "A", "B"]})
    df_dataset["partition"] = df_dataset["partition"].astype("category")
    path_dataset = f"{path}test_parquet_dataset"
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_parquet(df=df_file, path=path_file, mode="append")
    with pytest.raises(wr.exceptions.InvalidCompression):
        wr.s3.to_parquet(df=df_file, path=path_file, compression="WRONG")
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_parquet(df=df_dataset, path=path_dataset, partition_cols=["col2"])
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_parquet(df=df_dataset, path=path_dataset, glue_table_settings={"description": "foo"})
    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.s3.to_parquet(df=df_dataset, path=path_dataset, partition_cols=["col2"], dataset=True, mode="WRONG")
    wr.s3.to_parquet(df=df_file, path=path_file)
    assert len(wr.s3.read_parquet(path=path_file, use_threads=True, boto3_session=None).index) == 3
    assert len(wr.s3.read_parquet(path=[path_file], use_threads=False, boto3_session=boto3.DEFAULT_SESSION).index) == 3
    paths = wr.s3.to_parquet(df=df_dataset, path=path_dataset, dataset=True)["paths"]
    with pytest.raises(wr.exceptions.InvalidArgument):
        assert wr.s3.read_parquet(path=paths, dataset=True)
    assert len(wr.s3.read_parquet(path=path_dataset, use_threads=True, boto3_session=boto3.DEFAULT_SESSION).index) == 3
    dataset_paths = wr.s3.to_parquet(
        df=df_dataset, path=path_dataset, dataset=True, partition_cols=["partition"], mode="overwrite"
    )["paths"]
    assert len(wr.s3.read_parquet(path=path_dataset, use_threads=True, boto3_session=None).index) == 3
    assert len(wr.s3.read_parquet(path=dataset_paths, use_threads=True).index) == 3
    assert len(wr.s3.read_parquet(path=path_dataset, dataset=True, use_threads=True).index) == 3
    wr.s3.to_parquet(df=df_dataset, path=path_dataset, dataset=True, partition_cols=["partition"], mode="overwrite")
    wr.s3.to_parquet(
        df=df_dataset, path=path_dataset, dataset=True, partition_cols=["partition"], mode="overwrite_partitions"
    )


@pytest.mark.parametrize("columns", [None, ["val"]])
def test_parquet_bulk_read(path: str, columns: Optional[List[str]]) -> None:
    df = pd.DataFrame({"id": [1, 2, 3], "val": ["foo", "boo", "bar"]})
    num_files = 10

    for i in range(num_files):
        wr.s3.to_parquet(df=df, path=f"{path}{i}.parquet")

    df2 = wr.s3.read_parquet(path=path, columns=columns, ray_args={"bulk_read": True})
    assert len(df2) == num_files * len(df)

    expected_num_columns = len(df.columns) if columns is None else len(columns)
    assert len(df2.columns) == expected_num_columns


@pytest.mark.xfail(
    raises=AssertionError,
    condition=is_ray_modin,
    reason="Validate schema is necessary to merge schemas in distributed mode",
)
def test_parquet_validate_schema(path):
    df = pd.DataFrame({"id": [1, 2, 3]})
    path_file = f"{path}0.parquet"
    wr.s3.to_parquet(df=df, path=path_file)
    df2 = pd.DataFrame({"id2": [1, 2, 3], "val": ["foo", "boo", "bar"]})
    path_file2 = f"{path}1.parquet"
    wr.s3.to_parquet(df=df2, path=path_file2)
    df3 = wr.s3.read_parquet(path=path, validate_schema=False)
    assert len(df3.index) == 6
    assert len(df3.columns) == 3
    with pytest.raises(wr.exceptions.InvalidSchemaConvergence):
        wr.s3.read_parquet(path=path, validate_schema=True)


def test_parquet_uint64(path):
    df = pd.DataFrame(
        {
            "c0": [0, 0, (2**8) - 1],
            "c1": [0, 0, (2**16) - 1],
            "c2": [0, 0, (2**32) - 1],
            "c3": [0, 0, (2**64) - 1],
            "c4": [0, 1, 2],
        }
    )
    df["c0"] = df.c0.astype("uint8")
    df["c1"] = df.c1.astype("uint16")
    df["c2"] = df.c2.astype("uint32")
    df["c3"] = df.c3.astype("uint64")
    wr.s3.to_parquet(df=df, path=path, dataset=True, mode="overwrite", partition_cols=["c4"])
    df = wr.s3.read_parquet(path=path, dataset=True)
    assert len(df.index) == 3
    assert len(df.columns) == 5
    assert df.c0.max() == (2**8) - 1
    assert df.c1.max() == (2**16) - 1
    assert df.c2.max() == (2**32) - 1
    assert df.c3.max() == (2**64) - 1
    assert df.c4.astype("uint8").sum() == 3


def test_parquet_metadata_partitions(path):
    path = f"{path}0.parquet"
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": ["3", "4", "5"], "c2": [6.0, 7.0, 8.0]})
    wr.s3.to_parquet(df=df, path=path, dataset=False)
    columns_types, _ = wr.s3.read_parquet_metadata(path=path, dataset=False)
    assert len(columns_types) == len(df.columns)
    assert columns_types.get("c0") == "bigint"
    assert columns_types.get("c1") == "string"
    assert columns_types.get("c2") == "double"


def test_parquet_cast_string(path):
    df = pd.DataFrame({"id": [1, 2, 3], "value": ["foo", "boo", "bar"]})
    path_file = f"{path}0.parquet"
    wr.s3.to_parquet(df, path_file, dtype={"id": "string"}, sanitize_columns=False)
    df2 = wr.s3.read_parquet(path_file)
    assert str(df2.id.dtypes) == "string"
    assert df.shape == df2.shape
    for col, row in tuple(itertools.product(df.columns, range(3))):
        assert df[col].iloc[row] == df2[col].iloc[row]


def test_to_parquet_file_sanitize(path):
    df = pd.DataFrame({"C0": [0, 1], "camelCase": [2, 3], "c**--2": [4, 5]})
    path_file = f"{path}0.parquet"
    wr.s3.to_parquet(df, path_file, sanitize_columns=True)
    df2 = wr.s3.read_parquet(path_file)
    assert df.shape == df2.shape
    assert list(df2.columns) == ["c0", "camelcase", "c_2"]
    assert df2.c0.sum() == 1
    assert df2.camelcase.sum() == 5
    assert df2.c_2.sum() == 9


@pytest.mark.parametrize("use_threads", [True, False, 2])
def test_to_parquet_file_dtype(path, use_threads):
    df = pd.DataFrame({"c0": [1.0, None, 2.0], "c1": [pd.NA, pd.NA, pd.NA]})
    file_path = f"{path}0.parquet"
    wr.s3.to_parquet(df, file_path, dtype={"c0": "bigint", "c1": "string"}, use_threads=use_threads)
    df2 = wr.s3.read_parquet(file_path, use_threads=use_threads)
    assert df2.shape == df.shape
    assert df2.c0.sum() == 3
    assert str(df2.c0.dtype) == "Int64"
    assert str(df2.c1.dtype) == "string"


@pytest.mark.parametrize("filename_prefix", [None, "my_prefix"])
@pytest.mark.parametrize("use_threads", [True, False])
def test_to_parquet_filename_prefix(compare_filename_prefix, path, filename_prefix, use_threads):
    test_prefix = "my_prefix"
    df = pd.DataFrame({"col": [1, 2, 3], "col2": ["A", "A", "B"]})
    file_path = f"{path}0.parquet"

    # If Dataset is False, parquet file should never start with prefix
    filename = wr.s3.to_parquet(
        df=df, path=file_path, dataset=False, filename_prefix=filename_prefix, use_threads=use_threads
    )["paths"][0].split("/")[-1]
    assert not filename.startswith(test_prefix)

    # If Dataset is True, parquet file starts with prefix if one is supplied
    filename = wr.s3.to_parquet(
        df=df, path=path, dataset=True, filename_prefix=filename_prefix, use_threads=use_threads
    )["paths"][0].split("/")[-1]
    compare_filename_prefix(filename, filename_prefix, test_prefix)

    # Partitioned
    filename = wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        filename_prefix=filename_prefix,
        partition_cols=["col2"],
        use_threads=use_threads,
    )["paths"][0].split("/")[-1]
    compare_filename_prefix(filename, filename_prefix, test_prefix)

    # Bucketing
    filename = wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        filename_prefix=filename_prefix,
        bucketing_info=(["col2"], 2),
        use_threads=use_threads,
    )["paths"][0].split("/")[-1]
    compare_filename_prefix(filename, filename_prefix, test_prefix)
    assert filename.endswith("bucket-00000.snappy.parquet")


def test_read_parquet_map_types(path):
    df = pd.DataFrame({"c0": [0, 1, 1, 2]}, dtype=np.int8)
    file_path = f"{path}0.parquet"
    wr.s3.to_parquet(df, file_path)
    df2 = wr.s3.read_parquet(file_path)
    assert str(df2.c0.dtype) == "Int8"
    df3 = wr.s3.read_parquet(file_path, pyarrow_additional_kwargs={"types_mapper": None})
    assert str(df3.c0.dtype) == "int8"


@pytest.mark.parametrize("use_threads", [True, False, 2])
@pytest.mark.parametrize("max_rows_by_file", [None, 0, 40, 250, 1000])
def test_parquet_with_size(path, use_threads, max_rows_by_file):
    df = get_df_list()
    df = pd.concat([df for _ in range(100)])
    paths = wr.s3.to_parquet(
        df=df,
        path=path + "x.parquet",
        index=False,
        dataset=False,
        max_rows_by_file=max_rows_by_file,
        use_threads=use_threads,
    )["paths"]
    if max_rows_by_file is not None and max_rows_by_file > 0:
        assert len(paths) >= math.floor(300 / max_rows_by_file)
        assert len(paths) <= math.ceil(300 / max_rows_by_file)
    df2 = wr.s3.read_parquet(path=path, dataset=False, use_threads=use_threads)
    ensure_data_types(df2, has_list=True)
    assert df2.shape == (300, 19)
    assert df.iint8.sum() == df2.iint8.sum()


@pytest.mark.parametrize("use_threads", [True, False, 2])
def test_index_and_timezone(path, use_threads):
    df = pd.DataFrame({"c0": [datetime.utcnow(), datetime.utcnow()], "par": ["a", "b"]}, index=["foo", "boo"])
    df["c1"] = pd.DatetimeIndex(df.c0).tz_localize(tz="US/Eastern")
    df.index = df.index.astype("string")
    wr.s3.to_parquet(df, path, index=True, use_threads=use_threads, dataset=True, partition_cols=["par"])
    df2 = wr.s3.read_parquet(path, use_threads=use_threads, dataset=True)
    assert_pandas_equals(df[["c0", "c1"]], df2[["c0", "c1"]])


@pytest.mark.xfail(
    is_ray_modin, raises=wr.exceptions.InvalidArgumentCombination, reason="Index not working with `max_rows_by_file`"
)
@pytest.mark.parametrize("use_threads", [True, False, 2])
def test_index_recovery_simple_int(path, use_threads):
    df = pd.DataFrame({"c0": np.arange(10, 1_010, 1)}, dtype="Int64")
    df.index = df.index.astype("Int64")
    paths = wr.s3.to_parquet(df, path, index=True, use_threads=use_threads, dataset=True, max_rows_by_file=300)["paths"]
    assert len(paths) == 4
    df2 = wr.s3.read_parquet(f"{path}*.parquet", use_threads=use_threads)
    assert_pandas_equals(df, df2)


@pytest.mark.xfail(
    is_ray_modin, raises=wr.exceptions.InvalidArgumentCombination, reason="Index not working with `max_rows_by_file`"
)
@pytest.mark.parametrize("use_threads", [True, False, 2])
def test_index_recovery_simple_str(path, use_threads):
    df = pd.DataFrame({"c0": [0, 1, 2, 3, 4]}, index=["a", "b", "c", "d", "e"], dtype="Int64")
    df.index = df.index.astype("string")
    paths = wr.s3.to_parquet(df, path, index=True, use_threads=use_threads, dataset=True, max_rows_by_file=1)["paths"]
    assert len(paths) == 5
    df2 = wr.s3.read_parquet(f"{path}*.parquet", use_threads=use_threads)
    assert_pandas_equals(df, df2)


@pytest.mark.parametrize("use_threads", [True, False, 2])
def test_index_recovery_partitioned_str(path, use_threads):
    df = pd.DataFrame(
        {"c0": [0, 1, 2, 3, 4], "par": ["foo", "boo", "bar", "foo", "boo"]}, index=["a", "b", "c", "d", "e"]
    )
    df.index = df.index.astype("string")
    df["c0"] = df["c0"].astype("Int64")
    df["par"] = df["c0"].astype("category")
    paths = wr.s3.to_parquet(
        df, path, index=True, use_threads=use_threads, dataset=True, partition_cols=["par"], max_rows_by_file=1
    )["paths"]
    assert len(paths) == 5
    df2 = wr.s3.read_parquet(f"{path}*.parquet", use_threads=use_threads, dataset=True)
    assert df.shape == df2.shape
    assert df.c0.equals(df2.c0)
    assert df.index.equals(df2.index)


@pytest.mark.xfail(
    is_ray_modin, raises=wr.exceptions.InvalidArgumentCombination, reason="Index not working with `max_rows_by_file`"
)
@pytest.mark.parametrize("use_threads", [True, False, 2])
def test_range_index_recovery_simple(path, use_threads):
    df = pd.DataFrame({"c0": np.arange(10, 15, 1)}, dtype="Int64", index=pd.RangeIndex(start=5, stop=30, step=5))
    df.index = df.index.astype("Int64")
    paths = wr.s3.to_parquet(df, path, index=True, use_threads=use_threads, dataset=True, max_rows_by_file=3)["paths"]
    assert len(paths) == 2
    df2 = wr.s3.read_parquet(f"{path}*.parquet", use_threads=use_threads)
    assert_pandas_equals(df.reset_index(level=0), df2.reset_index(level=0))


@pytest.mark.modin_index
@pytest.mark.xfail(
    raises=AssertionError,
    reason="https://github.com/ray-project/ray/issues/37771",
    condition=is_ray_modin,
)
@pytest.mark.parametrize("use_threads", [True, False, 2])
@pytest.mark.parametrize("name", [None, "foo"])
def test_range_index_recovery_pandas(path, use_threads, name):
    # Import pandas because modin.to_parquet does not preserve index.name when writing parquet
    import pandas as pd

    df = pd.DataFrame({"c0": np.arange(10, 15, 1)}, dtype="Int64", index=pd.RangeIndex(start=5, stop=30, step=5))
    df.index.name = name
    path_file = f"{path}0.parquet"
    df.to_parquet(path_file)
    df2 = wr.s3.read_parquet(path_file, use_threads=use_threads, pyarrow_additional_kwargs={"ignore_metadata": False})
    assert_pandas_equals(df.reset_index(level=0), df2.reset_index(level=0))


@pytest.mark.xfail(
    is_ray_modin, raises=wr.exceptions.InvalidArgumentCombination, reason="Index not working with `max_rows_by_file`"
)
@pytest.mark.parametrize("use_threads", [True, False, 2])
def test_multi_index_recovery_simple(path, use_threads):
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": ["a", "b", "c"], "c2": [True, False, True], "c3": [0, 1, 2]})
    df["c0"] = df["c0"].astype("Int64")
    df["c1"] = df["c1"].astype("string")
    df["c2"] = df["c2"].astype("boolean")
    df["c3"] = df["c3"].astype("Int64")

    df = df.set_index(["c0", "c1", "c2"])
    paths = wr.s3.to_parquet(df, path, index=True, use_threads=use_threads, dataset=True, max_rows_by_file=1)["paths"]
    assert len(paths) == 3
    df2 = wr.s3.read_parquet(f"{path}*.parquet", use_threads=use_threads)
    assert_pandas_equals(df.reset_index(), df2.reset_index())


@pytest.mark.xfail(
    is_ray_modin, raises=wr.exceptions.InvalidArgumentCombination, reason="Index not working with `max_rows_by_file`"
)
@pytest.mark.parametrize("use_threads", [True, False, 2])
def test_multi_index_recovery_nameless(path, use_threads):
    df = pd.DataFrame({"c0": np.arange(10, 13, 1)}, dtype="Int64")
    df = df.set_index([pd.Index([1, 2, 3], dtype="Int64"), pd.Index([1, 2, 3], dtype="Int64")])
    paths = wr.s3.to_parquet(df, path, index=True, use_threads=use_threads, dataset=True, max_rows_by_file=1)["paths"]
    assert len(paths) == 3
    df2 = wr.s3.read_parquet(f"{path}*.parquet", use_threads=use_threads)
    assert_pandas_equals(df.reset_index(), df2.reset_index())


@pytest.mark.modin_index
@pytest.mark.xfail(
    raises=(wr.exceptions.InvalidArgumentCombination, AssertionError),
    reason="Named index not working when partitioning to a single file",
    condition=is_ray_modin,
)
@pytest.mark.parametrize("use_threads", [True, False, 2])
@pytest.mark.parametrize("name", [None, "foo"])
@pytest.mark.parametrize("pandas", [True, False])
def test_index_columns(path, use_threads, name, pandas):
    df = pd.DataFrame({"c0": [0, 1], "c1": [2, 3]}, dtype="Int64")
    df.index.name = name
    path_file = f"{path}0.parquet"
    if pandas:
        df.to_parquet(path_file, index=True)
    else:
        wr.s3.to_parquet(df, path_file, index=True)
    df2 = wr.s3.read_parquet(path_file, columns=["c0"], use_threads=use_threads)
    assert df[["c0"]].equals(df2)


@pytest.mark.parametrize("use_threads", [True, False, 2])
@pytest.mark.parametrize("name", [None, "foo"])
@pytest.mark.parametrize("pandas", [True, False])
@pytest.mark.parametrize("drop", [True, False])
def test_range_index_columns(path, use_threads, name, pandas, drop):
    if wr.memory_format.get() == MemoryFormatEnum.MODIN and not pandas:
        pytest.skip("Skip due to Modin data frame index not saved as a named column")
    df = pd.DataFrame({"c0": [0, 1], "c1": [2, 3]}, dtype="Int64", index=pd.RangeIndex(start=5, stop=7, step=1))
    df.index.name = name
    df.index = df.index.astype("Int64")

    path_file = f"{path}0.parquet"
    if pandas:
        # Convert to pandas because modin.to_parquet() does not preserve index name
        df = to_pandas(df)
        df.to_parquet(path_file, index=True)
    else:
        wr.s3.to_parquet(df, path_file, index=True)

    name = "__index_level_0__" if name is None else name
    columns = ["c0"] if drop else [name, "c0"]
    df2 = wr.s3.read_parquet(path_file, columns=columns, use_threads=use_threads)
    assert_pandas_equals(df[["c0"]].reset_index(level=0, drop=drop), df2.reset_index(level=0, drop=drop))


def test_to_parquet_dataset_sanitize(path):
    df = pd.DataFrame({"C0": [0, 1], "camelCase": [2, 3], "c**--2": [4, 5], "Par": ["a", "b"]})

    wr.s3.to_parquet(df, path, dataset=True, partition_cols=["Par"], sanitize_columns=False)
    df2 = wr.s3.read_parquet(path, dataset=True)
    assert df.shape == df2.shape
    assert list(df2.columns) == ["C0", "camelCase", "c**--2", "Par"]
    assert df2.C0.sum() == 1
    assert df2.camelCase.sum() == 5
    assert df2["c**--2"].sum() == 9
    assert df2.Par.to_list() == ["a", "b"]
    wr.s3.to_parquet(df, path, dataset=True, partition_cols=["par"], sanitize_columns=True, mode="overwrite")
    df2 = wr.s3.read_parquet(path, dataset=True)
    assert df.shape == df2.shape
    assert list(df2.columns) == ["c0", "camelcase", "c_2", "par"]
    assert df2.c0.sum() == 1
    assert df2.camelcase.sum() == 5
    assert df2.c_2.sum() == 9
    assert df2.par.to_list() == ["a", "b"]


@pytest.mark.modin_index
@pytest.mark.parametrize("use_threads", [False, True, 2])
def test_timezone_file(path, use_threads):
    file_path = f"{path}0.parquet"
    df = pd.DataFrame({"c0": [datetime.utcnow(), datetime.utcnow()]})
    df["c0"] = pd.DatetimeIndex(df.c0).tz_localize(tz="US/Eastern")
    df.to_parquet(file_path)
    df2 = wr.s3.read_parquet(path, use_threads=use_threads, pyarrow_additional_kwargs={"ignore_metadata": True})
    assert_pandas_equals(df, df2)


@pytest.mark.modin_index
@pytest.mark.parametrize("use_threads", [True, False, 2])
def test_timezone_file_columns(path, use_threads):
    file_path = f"{path}0.parquet"
    df = pd.DataFrame({"c0": [datetime.utcnow(), datetime.utcnow()], "c1": [1.1, 2.2]})
    df["c0"] = pd.DatetimeIndex(df.c0).tz_localize(tz="US/Eastern")
    df.to_parquet(file_path)
    df2 = wr.s3.read_parquet(
        path, columns=["c1"], use_threads=use_threads, pyarrow_additional_kwargs={"ignore_metadata": True}
    )
    assert_pandas_equals(df[["c1"]], df2)


def test_timezone_raw_values(path):
    df = pd.DataFrame({"c0": [1.1, 2.2], "par": ["a", "b"]})
    df["c1"] = pd.to_datetime(datetime.now(timezone.utc))
    df["c2"] = pd.to_datetime(datetime(2011, 11, 4, 0, 5, 23, tzinfo=timezone(timedelta(seconds=14400))))
    df["c3"] = pd.to_datetime(datetime(2011, 11, 4, 0, 5, 23, tzinfo=timezone(-timedelta(seconds=14400))))
    df["c4"] = pd.to_datetime(datetime(2011, 11, 4, 0, 5, 23, tzinfo=timezone(timedelta(hours=-8))))
    wr.s3.to_parquet(partition_cols=["par"], df=df, path=path, dataset=True, sanitize_columns=False)
    df2 = wr.s3.read_parquet(path, dataset=True, use_threads=False, pyarrow_additional_kwargs={"ignore_metadata": True})
    # Use pandas to read because of Modin "Internal Error: Internal and external indices on axis 1 do not match."
    import pandas

    df3 = pandas.read_parquet(path)
    df2["par"] = df2["par"].astype("string")
    df3["par"] = df3["par"].astype("string")
    assert_pandas_equals(df2, df3)


@pytest.mark.parametrize(
    "partition_cols",
    [
        None,
        ["a"],
        pytest.param(
            ["a", "b"],
            marks=pytest.mark.xfail(
                reason="Empty file cannot be read by Ray", raises=AssertionError, condition=is_ray_modin
            ),
        ),
    ],
)
def test_validate_columns(path, partition_cols) -> None:
    wr.s3.to_parquet(pd.DataFrame({"a": [1], "b": [2]}), path, dataset=True, partition_cols=partition_cols)
    wr.s3.read_parquet(path, columns=["a", "b"], dataset=True, validate_schema=True)
    with pytest.raises(KeyError):
        wr.s3.read_parquet(path, columns=["a", "b", "c"], dataset=True, validate_schema=True)


@pytest.mark.parametrize("use_threads", [True, False, 2])
def test_empty_column(path, use_threads):
    df = pd.DataFrame({"c0": [1, 2, 3], "c1": [None, None, None], "par": ["a", "b", "c"]})
    df["c0"] = df["c0"].astype("Int64")
    df["par"] = df["par"].astype("string")
    wr.s3.to_parquet(df, path, dataset=True, partition_cols=["par"])
    df2 = wr.s3.read_parquet(path, dataset=True, use_threads=use_threads).reset_index(drop=True)
    df2["par"] = df2["par"].astype("string")
    assert_pandas_equals(df, df2)


def test_mixed_types_column(path) -> None:
    df = pd.DataFrame({"c0": [1, 2, 3], "c1": [1, 2, "foo"], "par": ["a", "b", "c"]})
    df["c0"] = df["c0"].astype("Int64")
    df["par"] = df["par"].astype("string")
    with pytest.raises(pa.ArrowInvalid):
        wr.s3.to_parquet(df, path, dataset=True, partition_cols=["par"])


@pytest.mark.modin_index
@pytest.mark.parametrize("compression", [None, "snappy", "gzip", "zstd"])
def test_parquet_compression(path, compression) -> None:
    df = pd.DataFrame({"id": [1, 2, 3]}, dtype="Int64")
    path_file = f"{path}0.parquet"
    wr.s3.to_parquet(df=df, path=path_file, compression=compression)
    df2 = wr.s3.read_parquet([path_file], pyarrow_additional_kwargs={"ignore_metadata": True})
    assert_pandas_equals(df, df2)


@pytest.mark.parametrize("use_threads", [True, False, 2])
@pytest.mark.parametrize(
    "schema", [None, pa.schema([pa.field("c0", pa.int64()), pa.field("c1", pa.int64()), pa.field("par", pa.string())])]
)
def test_empty_file(path, use_threads, schema):
    df = pd.DataFrame({"c0": [1, 2, 3], "c1": [None, None, None], "par": ["a", "b", "c"]})
    df.index = df.index.astype("Int64")
    df["c0"] = df["c0"].astype("Int64")
    df["par"] = df["par"].astype("string")
    wr.s3.to_parquet(df, path, index=True, dataset=True, partition_cols=["par"])
    bucket, key = wr._utils.parse_path(f"{path}test.csv")
    boto3.client("s3").put_object(Body=b"", Bucket=bucket, Key=key)
    with pytest.raises(wr.exceptions.InvalidFile):
        wr.s3.read_parquet(path, use_threads=use_threads, ignore_empty=False, schema=schema)
    df2 = wr.s3.read_parquet(path, dataset=True, use_threads=use_threads)
    df2["par"] = df2["par"].astype("string")
    assert_pandas_equals(df, df2)


@pytest.mark.parametrize("use_threads", [True, False, 2])
def test_ignore_files(path: str, use_threads: Union[bool, int]) -> None:
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [0, 1, 2], "c2": [0, 0, 1]})

    wr.s3.to_parquet(df, f"{path}data.parquet", index=False)
    wr.s3.to_parquet(df, f"{path}data.parquet2", index=False)
    wr.s3.to_parquet(df, f"{path}data.parquet3", index=False)

    df2 = wr.s3.read_parquet(
        path,
        use_threads=use_threads,
        path_ignore_suffix=[".parquet2", ".parquet3"],
        dataset=True,
    )

    assert df.shape == df2.shape


@pytest.mark.xfail(
    is_ray_modin,
    raises=AssertionError,
    reason=(
        "Ray currently ignores empty blocks when fetching dataset schema:"
        "(ExecutionPlan)[https://github.com/ray-project/ray/blob/ray-2.0.1/python/ray/data/_internal/plan.py#L253]"
    ),
)
@pytest.mark.parametrize("chunked", [True, False])
def test_empty_parquet(path, chunked):
    path = f"{path}file.parquet"
    s = pa.schema([pa.field("a", pa.int64())])
    pq.write_table(s.empty_table(), path)

    df = wr.s3.read_parquet(path, chunked=chunked)

    if chunked:
        df = pd.concat(list(df))

    assert len(df) == 0
    assert len(df.columns) > 0


def test_read_chunked(path):
    path = f"{path}file.parquet"
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [None, None, None]})
    wr.s3.to_parquet(df, path)
    df2 = next(wr.s3.read_parquet(path, chunked=True))
    assert df.shape == df2.shape


def test_read_chunked_validation_exception2(path):
    df = pd.DataFrame({"c0": [0, 1, 2]})
    wr.s3.to_parquet(df, f"{path}file0.parquet")
    df = pd.DataFrame({"c1": [0, 1, 2]})
    wr.s3.to_parquet(df, f"{path}file1.parquet")
    with pytest.raises(wr.exceptions.InvalidSchemaConvergence):
        for _ in wr.s3.read_parquet(path, dataset=True, chunked=True, validate_schema=True):
            pass


@pytest.mark.xfail(
    is_ray_modin, raises=wr.exceptions.InvalidArgument, reason="kwargs not supported in distributed mode"
)
def test_read_parquet_versioned(path) -> None:
    path_file = f"{path}0.parquet"
    dfs = [pd.DataFrame({"id": [1, 2, 3]}, dtype="Int64"), pd.DataFrame({"id": [4, 5, 6]}, dtype="Int64")]
    for df in dfs:
        wr.s3.to_parquet(df=df, path=path_file)
        version_id = wr.s3.describe_objects(path=path_file)[path_file]["VersionId"]
        df_temp = wr.s3.read_parquet(path_file, version_id=version_id)
        assert_pandas_equals(df_temp, df)
        assert version_id == wr.s3.describe_objects(path=path_file, version_id=version_id)[path_file]["VersionId"]


def test_parquet_schema_evolution(path, glue_database, glue_table):
    df = pd.DataFrame(
        {
            "id": [1, 2],
            "value": ["foo", "boo"],
        }
    )
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode="overwrite",
        database=glue_database,
        table=glue_table,
    )

    df2 = pd.DataFrame(
        {"id": [3, 4], "value": ["bar", None], "date": [date(2020, 1, 3), date(2020, 1, 4)], "flag": [True, False]}
    )
    wr.s3.to_parquet(
        df=df2,
        path=path,
        dataset=True,
        mode="append",
        database=glue_database,
        table=glue_table,
        schema_evolution=True,
        catalog_versioning=True,
    )

    column_types = wr.catalog.get_table_types(glue_database, glue_table)
    assert len(column_types) == len(df2.columns)


@pytest.mark.xfail(
    reason="Schema resolution is not as consistent in distributed mode", condition=is_ray_modin, raises=AssertionError
)
def test_to_parquet_schema_evolution_out_of_order(path, glue_database, glue_table) -> None:
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": ["a", "b", "c"]})
    wr.s3.to_parquet(df=df, path=path, dataset=True, database=glue_database, table=glue_table)

    df2 = df.copy()
    df2["c2"] = ["x", "y", "z"]

    wr.s3.to_parquet(
        df=df2,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
        mode="append",
        schema_evolution=True,
        catalog_versioning=True,
    )

    df_out = wr.s3.read_parquet(path=path, dataset=True)
    df_expected = pd.concat([df, df2], ignore_index=True)

    assert len(df_out) == len(df_expected)
    assert list(df_out.columns) == list(df_expected.columns)


# TODO https://github.com/aws/aws-sdk-pandas/issues/1775
@pytest.mark.xfail(reason="The `ignore_index` is not implemented")
def test_read_parquet_schema_validation_with_index_column(path) -> None:
    path_file = f"{path}file.parquet"
    df = pd.DataFrame({"idx": [1], "col": [2]})
    df0 = df.set_index("idx")
    wr.s3.to_parquet(
        df=df0,
        path=path_file,
        index=True,
    )
    df1 = wr.s3.read_parquet(
        path=path_file,
        ignore_index=False,
        columns=["idx", "col"],
        validate_schema=True,
    )
    assert df0.shape == df1.shape
