# mypy: disable-error-code=no-untyped-def

import itertools
import logging
import math
from datetime import date, datetime
from typing import Union

import boto3
import numpy as np
import pyarrow as pa
import pytest

import awswrangler as wr
import awswrangler.pandas as pd

from .._utils import (
    assert_pandas_equals,
    ensure_data_types,
    get_df_list,
    is_ray_modin,
)

logging.getLogger("awswrangler").setLevel(logging.DEBUG)

pytestmark = pytest.mark.distributed


@pytest.mark.parametrize("partition_cols", [None, ["c2"], ["c1", "c2"]])
def test_orc_metadata_partitions_dataset(path, partition_cols):
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [3, 4, 5], "c2": [6, 7, 8]})
    wr.s3.to_orc(df=df, path=path, dataset=True, partition_cols=partition_cols)
    columns_types, partitions_types = wr.s3.read_orc_metadata(path=path, dataset=True)
    partitions_types = partitions_types if partitions_types is not None else {}
    assert len(columns_types) + len(partitions_types) == len(df.columns)
    assert columns_types.get("c0") == "bigint"
    assert (columns_types.get("c1") == "bigint") or (partitions_types.get("c1") == "string")
    assert (columns_types.get("c1") == "bigint") or (partitions_types.get("c1") == "string")


def test_read_orc_metadata_nulls(path):
    df = pd.DataFrame({"c0": [1.0, 1.1, 1.2], "c1": [1, 2, 3], "c2": ["a", "b", "c"]})
    path = f"{path}df.orc"
    wr.s3.to_orc(df, path)
    columns_types, _ = wr.s3.read_orc_metadata(path)
    assert len(columns_types) == len(df.columns)
    assert columns_types.get("c0") == "double"
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
def test_orc_cast_string_dataset(path, partition_cols):
    df = pd.DataFrame({"id": [1, 2, 3], "value": ["foo", "boo", "bar"], "c2": [4, 5, 6], "c3": [7.0, 8.0, 9.0]})
    wr.s3.to_orc(df, path, dataset=True, partition_cols=partition_cols, dtype={"id": "string", "c3": "string"})
    df2 = wr.s3.read_orc(path, dataset=True).sort_values("id", ignore_index=True)
    assert str(df2.id.dtypes) == "string"
    assert str(df2.c3.dtypes) == "string"
    assert df.shape == df2.shape
    for col, row in tuple(itertools.product(df.columns, range(3))):
        assert str(df[col].iloc[row]) == str(df2[col].iloc[row])


@pytest.mark.parametrize("use_threads", [True, False, 2])
def test_read_orc_filter_partitions(path, use_threads):
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [0, 1, 2], "c2": [0, 0, 1]})
    wr.s3.to_orc(df, path, dataset=True, partition_cols=["c1", "c2"], use_threads=use_threads)
    df2 = wr.s3.read_orc(
        path, dataset=True, partition_filter=lambda x: True if x["c1"] == "0" else False, use_threads=use_threads
    )
    assert df2.shape == (1, 3)
    assert df2.c0.iloc[0] == 0
    assert df2.c1.astype(int).iloc[0] == 0
    assert df2.c2.astype(int).iloc[0] == 0
    df2 = wr.s3.read_orc(
        path,
        dataset=True,
        partition_filter=lambda x: True if x["c1"] == "1" and x["c2"] == "0" else False,
        use_threads=use_threads,
    )
    assert df2.shape == (1, 3)
    assert df2.c0.iloc[0] == 1
    assert df2.c1.astype(int).iloc[0] == 1
    assert df2.c2.astype(int).iloc[0] == 0
    df2 = wr.s3.read_orc(
        path, dataset=True, partition_filter=lambda x: True if x["c2"] == "0" else False, use_threads=use_threads
    )
    assert df2.shape == (2, 3)
    assert df2.c0.astype(int).sum() == 1
    assert df2.c1.astype(int).sum() == 1
    assert df2.c2.astype(int).sum() == 0


def test_read_orc_table(path, glue_database, glue_table):
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [0, 1, 2], "c2": [0, 0, 1]})
    wr.s3.to_orc(
        df,
        path,
        dataset=True,
        database=glue_database,
        table=glue_table,
    )
    df_out = wr.s3.read_orc_table(table=glue_table, database=glue_database)
    assert df_out.shape == (3, 3)


def test_read_orc_table_filter_partitions(path, glue_database, glue_table):
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [0, 1, 2], "c2": [0, 0, 1]})
    wr.s3.to_orc(
        df,
        path,
        dataset=True,
        partition_cols=["c1", "c2"],
        database=glue_database,
        table=glue_table,
    )
    df_out = wr.s3.read_orc_table(
        table=glue_table,
        database=glue_database,
        partition_filter=lambda x: True if x["c1"] == "0" else False,
    )
    assert df_out.shape == (1, 3)
    assert df_out.c0.astype(int).sum() == 0
    with pytest.raises(wr.exceptions.NoFilesFound):
        wr.s3.read_orc_table(
            table=glue_table,
            database=glue_database,
            partition_filter=lambda x: True if x["c1"] == "3" else False,
        )


@pytest.mark.xfail(
    is_ray_modin, raises=wr.exceptions.InvalidArgument, reason="kwargs not supported in distributed mode"
)
def test_orc(path):
    df_file = pd.DataFrame({"id": [1, 2, 3]})
    path_file = f"{path}test_orc_file.orc"
    df_dataset = pd.DataFrame({"id": [1, 2, 3], "partition": ["A", "A", "B"]})
    path_dataset = f"{path}test_orc_dataset"
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_orc(df=df_file, path=path_file, mode="append")
    with pytest.raises(wr.exceptions.InvalidCompression):
        wr.s3.to_orc(df=df_file, path=path_file, compression="WRONG")
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_orc(df=df_dataset, path=path_dataset, partition_cols=["col2"])
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_orc(df=df_dataset, path=path_dataset, glue_table_settings={"description": "foo"})
    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.s3.to_orc(df=df_dataset, path=path_dataset, partition_cols=["col2"], dataset=True, mode="WRONG")
    wr.s3.to_orc(df=df_file, path=path_file)
    assert len(wr.s3.read_orc(path=path_file, use_threads=True, boto3_session=None).index) == 3
    assert len(wr.s3.read_orc(path=[path_file], use_threads=False, boto3_session=boto3.DEFAULT_SESSION).index) == 3
    paths = wr.s3.to_orc(df=df_dataset, path=path_dataset, dataset=True)["paths"]
    with pytest.raises(wr.exceptions.InvalidArgument):
        assert wr.s3.read_orc(path=paths, dataset=True)
    assert len(wr.s3.read_orc(path=path_dataset, use_threads=True, boto3_session=boto3.DEFAULT_SESSION).index) == 3
    dataset_paths = wr.s3.to_orc(
        df=df_dataset, path=path_dataset, dataset=True, partition_cols=["partition"], mode="overwrite"
    )["paths"]
    assert len(wr.s3.read_orc(path=path_dataset, use_threads=True, boto3_session=None).index) == 3
    assert len(wr.s3.read_orc(path=dataset_paths, use_threads=True).index) == 3
    assert len(wr.s3.read_orc(path=path_dataset, dataset=True, use_threads=True).index) == 3
    wr.s3.to_orc(df=df_dataset, path=path_dataset, dataset=True, partition_cols=["partition"], mode="overwrite")
    wr.s3.to_orc(
        df=df_dataset, path=path_dataset, dataset=True, partition_cols=["partition"], mode="overwrite_partitions"
    )


@pytest.mark.xfail(
    raises=AssertionError,
    condition=is_ray_modin,
    reason="Validate schema is necessary to merge schemas in distributed mode",
)
def test_orc_validate_schema(path):
    df = pd.DataFrame({"id": [1, 2, 3]})
    path_file = f"{path}0.orc"
    wr.s3.to_orc(df=df, path=path_file)
    df2 = pd.DataFrame({"id2": [1, 2, 3], "val": ["foo", "boo", "bar"]})
    path_file2 = f"{path}1.orc"
    wr.s3.to_orc(df=df2, path=path_file2)
    df3 = wr.s3.read_orc(path=path, validate_schema=False)
    assert len(df3.index) == 6
    assert len(df3.columns) == 3
    with pytest.raises(wr.exceptions.InvalidSchemaConvergence):
        wr.s3.read_orc(path=path, validate_schema=True)


def test_orc_metadata_partitions(path):
    path = f"{path}0.orc"
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": ["3", "4", "5"], "c2": [6.0, 7.0, 8.0]})
    wr.s3.to_orc(df=df, path=path, dataset=False)
    columns_types, _ = wr.s3.read_orc_metadata(path=path, dataset=False)
    assert len(columns_types) == len(df.columns)
    assert columns_types.get("c0") == "bigint"
    assert columns_types.get("c1") == "string"
    assert columns_types.get("c2") == "double"


def test_orc_cast_string(path):
    df = pd.DataFrame({"id": [1, 2, 3], "value": ["foo", "boo", "bar"]})
    path_file = f"{path}0.orc"
    wr.s3.to_orc(df, path_file, dtype={"id": "string"}, sanitize_columns=False)
    df2 = wr.s3.read_orc(path_file)
    assert str(df2.id.dtypes) == "string"
    assert df.shape == df2.shape
    for col, row in tuple(itertools.product(df.columns, range(3))):
        assert df[col].iloc[row] == df2[col].iloc[row]


def test_to_orc_file_sanitize(path):
    df = pd.DataFrame({"C0": [0, 1], "camelCase": [2, 3], "c**--2": [4, 5]})
    path_file = f"{path}0.orc"
    wr.s3.to_orc(df, path_file, sanitize_columns=True)
    df2 = wr.s3.read_orc(path_file)
    assert df.shape == df2.shape
    assert list(df2.columns) == ["c0", "camelcase", "c_2"]
    assert df2.c0.sum() == 1
    assert df2.camelcase.sum() == 5
    assert df2.c_2.sum() == 9


@pytest.mark.parametrize("use_threads", [True, False, 2])
def test_to_orc_file_dtype(path, use_threads):
    df = pd.DataFrame({"c0": [1.0, None, 2.0], "c1": [pd.NA, pd.NA, pd.NA]})
    file_path = f"{path}0.orc"
    wr.s3.to_orc(df, file_path, dtype={"c0": "bigint", "c1": "string"}, use_threads=use_threads)
    df2 = wr.s3.read_orc(file_path, use_threads=use_threads)
    assert df2.shape == df.shape
    assert df2.c0.sum() == 3
    assert str(df2.c0.dtype) == "Int64"
    assert str(df2.c1.dtype) == "string"


@pytest.mark.parametrize("filename_prefix", [None, "my_prefix"])
@pytest.mark.parametrize("use_threads", [True, False])
def test_to_orc_filename_prefix(compare_filename_prefix, path, filename_prefix, use_threads):
    test_prefix = "my_prefix"
    df = pd.DataFrame({"col": [1, 2, 3], "col2": ["A", "A", "B"]})
    file_path = f"{path}0.orc"

    # If Dataset is False, ORC file should never start with prefix
    filename = wr.s3.to_orc(
        df=df, path=file_path, dataset=False, filename_prefix=filename_prefix, use_threads=use_threads
    )["paths"][0].split("/")[-1]
    assert not filename.startswith(test_prefix)

    # If Dataset is True, ORC file starts with prefix if one is supplied
    filename = wr.s3.to_orc(df=df, path=path, dataset=True, filename_prefix=filename_prefix, use_threads=use_threads)[
        "paths"
    ][0].split("/")[-1]
    compare_filename_prefix(filename, filename_prefix, test_prefix)

    # Partitioned
    filename = wr.s3.to_orc(
        df=df,
        path=path,
        dataset=True,
        filename_prefix=filename_prefix,
        partition_cols=["col2"],
        use_threads=use_threads,
    )["paths"][0].split("/")[-1]
    compare_filename_prefix(filename, filename_prefix, test_prefix)

    # Bucketing
    filename = wr.s3.to_orc(
        df=df,
        path=path,
        dataset=True,
        filename_prefix=filename_prefix,
        bucketing_info=(["col2"], 2),
        use_threads=use_threads,
    )["paths"][0].split("/")[-1]
    compare_filename_prefix(filename, filename_prefix, test_prefix)
    assert filename.endswith("bucket-00000.orc")


def test_read_orc_map_types(path):
    df = pd.DataFrame({"c0": [0, 1, 1, 2]}, dtype=np.int8)
    file_path = f"{path}0.orc"
    wr.s3.to_orc(df, file_path)
    df2 = wr.s3.read_orc(file_path)
    assert str(df2.c0.dtype) == "Int8"
    df3 = wr.s3.read_orc(file_path, pyarrow_additional_kwargs={"types_mapper": None})
    assert str(df3.c0.dtype) == "int8"


@pytest.mark.parametrize("use_threads", [True, False, 2])
@pytest.mark.parametrize("max_rows_by_file", [None, 0, 40, 250, 1000])
def test_orc_with_size(path, use_threads, max_rows_by_file):
    df = get_df_list().drop(["category"], axis=1)  # category not supported
    df = pd.concat([df for _ in range(100)])
    paths = wr.s3.to_orc(
        df=df,
        path=path + "x.orc",
        index=False,
        dataset=False,
        max_rows_by_file=max_rows_by_file,
        use_threads=use_threads,
    )["paths"]
    if max_rows_by_file is not None and max_rows_by_file > 0:
        assert len(paths) >= math.floor(300 / max_rows_by_file)
        assert len(paths) <= math.ceil(300 / max_rows_by_file)
    df2 = wr.s3.read_orc(path=path, dataset=False, use_threads=use_threads)
    ensure_data_types(df2, has_list=True, has_category=False)
    assert df2.shape == (300, 18)
    assert df.iint8.sum() == df2.iint8.sum()


@pytest.mark.xfail(
    raises=wr.exceptions.InvalidArgumentCombination,
    reason="Named index not working when partitioning to a single file",
    condition=is_ray_modin,
)
@pytest.mark.parametrize("use_threads", [True, False, 2])
@pytest.mark.parametrize("name", [None, "foo"])
@pytest.mark.parametrize("pandas", [True, False])
def test_index_columns(path, use_threads, name, pandas):
    df = pd.DataFrame({"c0": [0, 1], "c1": [2, 3]}, dtype="Int64")
    df.index.name = name
    path_file = f"{path}0.orc"
    if pandas:
        df.to_orc(path_file, index=True)
    else:
        wr.s3.to_orc(df, path_file, index=True)
    df2 = wr.s3.read_orc(path_file, columns=["c0"], use_threads=use_threads)
    assert df[["c0"]].equals(df2)


def test_to_orc_dataset_sanitize(path):
    df = pd.DataFrame({"C0": [0, 1], "camelCase": [2, 3], "c**--2": [4, 5], "Par": ["a", "b"]})

    wr.s3.to_orc(df, path, dataset=True, partition_cols=["Par"], sanitize_columns=False)
    df2 = wr.s3.read_orc(path, dataset=True)
    assert df.shape == df2.shape
    assert list(df2.columns) == ["C0", "camelCase", "c**--2", "Par"]
    assert df2.C0.sum() == 1
    assert df2.camelCase.sum() == 5
    assert df2["c**--2"].sum() == 9
    assert df2.Par.to_list() == ["a", "b"]
    wr.s3.to_orc(df, path, dataset=True, partition_cols=["par"], sanitize_columns=True, mode="overwrite")
    df2 = wr.s3.read_orc(path, dataset=True)
    assert df.shape == df2.shape
    assert list(df2.columns) == ["c0", "camelcase", "c_2", "par"]
    assert df2.c0.sum() == 1
    assert df2.camelcase.sum() == 5
    assert df2.c_2.sum() == 9
    assert df2.par.to_list() == ["a", "b"]


@pytest.mark.parametrize("use_threads", [True, False, 2])
def test_timezone_file_columns(path, use_threads):
    file_path = f"{path}0.orc"
    df = pd.DataFrame({"c0": [datetime.utcnow(), datetime.utcnow()], "c1": [1.1, 2.2]})
    df["c0"] = pd.DatetimeIndex(df.c0).tz_localize(tz="US/Eastern")
    df.to_orc(file_path)
    df2 = wr.s3.read_orc(path, columns=["c1"], use_threads=use_threads)
    assert_pandas_equals(df[["c1"]], df2)


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
    wr.s3.to_orc(pd.DataFrame({"a": [1], "b": [2]}), path, dataset=True, partition_cols=partition_cols)
    wr.s3.read_orc(path, dataset=True, validate_schema=True)
    with pytest.raises(KeyError):
        wr.s3.read_orc(path, columns=["a", "b", "c"], dataset=True, validate_schema=True)


def test_mixed_types_column(path) -> None:
    df = pd.DataFrame({"c0": [1, 2, 3], "c1": [1, 2, "foo"], "par": ["a", "b", "c"]})
    df["c0"] = df["c0"].astype("Int64")
    df["par"] = df["par"].astype("string")
    with pytest.raises(pa.ArrowInvalid):
        wr.s3.to_orc(df, path, dataset=True, partition_cols=["par"])


@pytest.mark.parametrize("compression", [None, "snappy", "zlib", "lz4", "zstd"])
def test_orc_compression(path, compression) -> None:
    df = pd.DataFrame({"id": [1, 2, 3]}, dtype="Int64")
    path_file = f"{path}0.orc"
    wr.s3.to_orc(df=df, path=path_file, compression=compression)
    df2 = wr.s3.read_orc([path_file])
    assert_pandas_equals(df, df2)


@pytest.mark.parametrize("use_threads", [True, False, 2])
def test_empty_file(path, use_threads):
    df = pd.DataFrame({"c0": [1, 2, 3], "par": ["a", "b", "c"]})
    df["c0"] = df["c0"].astype("Int64")
    df["par"] = df["par"].astype("string")
    wr.s3.to_orc(df, path, dataset=True, partition_cols=["par"])
    bucket, key = wr._utils.parse_path(f"{path}test.csv")
    boto3.client("s3").put_object(Body=b"", Bucket=bucket, Key=key)
    df2 = wr.s3.read_orc(path, dataset=True, use_threads=use_threads)
    df2["par"] = df2["par"].astype("string")
    assert_pandas_equals(df, df2)


@pytest.mark.parametrize("use_threads", [True, False, 2])
def test_ignore_files(path: str, use_threads: Union[bool, int]) -> None:
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [0, 1, 2], "c2": [0, 0, 1]})

    wr.s3.to_orc(df, f"{path}data.orc", index=False)
    wr.s3.to_orc(df, f"{path}data.orc2", index=False)
    wr.s3.to_orc(df, f"{path}data.orc3", index=False)

    df2 = wr.s3.read_orc(
        path,
        use_threads=use_threads,
        path_ignore_suffix=[".orc2", ".orc3"],
        dataset=True,
    )

    assert df.shape == df2.shape


@pytest.mark.xfail(
    is_ray_modin, raises=wr.exceptions.InvalidArgument, reason="kwargs not supported in distributed mode"
)
def test_read_orc_versioned(path) -> None:
    path_file = f"{path}0.orc"
    dfs = [pd.DataFrame({"id": [1, 2, 3]}, dtype="Int64"), pd.DataFrame({"id": [4, 5, 6]}, dtype="Int64")]
    for df in dfs:
        wr.s3.to_orc(df=df, path=path_file)
        version_id = wr.s3.describe_objects(path=path_file)[path_file]["VersionId"]
        df_temp = wr.s3.read_orc(path_file, version_id=version_id)
        assert_pandas_equals(df_temp, df)
        assert version_id == wr.s3.describe_objects(path=path_file, version_id=version_id)[path_file]["VersionId"]


def test_orc_schema_evolution(path, glue_database, glue_table):
    df = pd.DataFrame(
        {
            "id": [1, 2],
            "value": ["foo", "boo"],
        }
    )
    wr.s3.to_orc(
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
    wr.s3.to_orc(
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
def test_to_orc_schema_evolution_out_of_order(path, glue_database, glue_table) -> None:
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": ["a", "b", "c"]})
    wr.s3.to_orc(df=df, path=path, dataset=True, database=glue_database, table=glue_table)

    df2 = df.copy()
    df2["c2"] = ["x", "y", "z"]

    wr.s3.to_orc(
        df=df2,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
        mode="append",
        schema_evolution=True,
        catalog_versioning=True,
    )

    df_out = wr.s3.read_orc(path=path, dataset=True)
    df_expected = pd.concat([df, df2], ignore_index=True)

    assert len(df_out) == len(df_expected)
    assert list(df_out.columns) == list(df_expected.columns)


# TODO https://github.com/aws/aws-sdk-pandas/issues/1775
@pytest.mark.xfail(reason="The `ignore_index` is not implemented")
def test_read_orc_schema_validation_with_index_column(path) -> None:
    path_file = f"{path}file.orc"
    df = pd.DataFrame({"idx": [1], "col": [2]})
    df0 = df.set_index("idx")
    wr.s3.to_orc(
        df=df0,
        path=path_file,
        index=True,
    )
    df1 = wr.s3.read_orc(
        path=path_file,
        ignore_index=False,
        columns=["idx", "col"],
        validate_schema=True,
    )
    assert df0.shape == df1.shape
