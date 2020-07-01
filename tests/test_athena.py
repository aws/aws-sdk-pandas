import datetime
import logging
import math
from unittest.mock import patch

import boto3
import pandas as pd
import pytest

import awswrangler as wr

from ._utils import (
    dt,
    ensure_data_types,
    ensure_data_types_category,
    ensure_data_types_csv,
    extract_cloudformation_outputs,
    get_df,
    get_df_cast,
    get_df_category,
    get_df_csv,
    get_df_list,
    get_query_long,
    get_time_str_with_random_suffix,
    path_generator,
    ts,
)

logging.basicConfig(level=logging.INFO, format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)
logging.getLogger("botocore.credentials").setLevel(logging.CRITICAL)


@pytest.fixture(scope="module")
def cloudformation_outputs():
    yield extract_cloudformation_outputs()


@pytest.fixture(scope="module")
def bucket(cloudformation_outputs):
    yield cloudformation_outputs["BucketName"]


@pytest.fixture(scope="module")
def database(cloudformation_outputs):
    yield cloudformation_outputs["GlueDatabaseName"]


@pytest.fixture(scope="module")
def kms_key(cloudformation_outputs):
    yield cloudformation_outputs["KmsKeyArn"]


@pytest.fixture(scope="module")
def workgroup0(bucket):
    wkg_name = "aws_data_wrangler_0"
    client = boto3.client("athena")
    wkgs = client.list_work_groups()
    wkgs = [x["Name"] for x in wkgs["WorkGroups"]]
    if wkg_name not in wkgs:
        client.create_work_group(
            Name=wkg_name,
            Configuration={
                "ResultConfiguration": {"OutputLocation": f"s3://{bucket}/athena_workgroup0/"},
                "EnforceWorkGroupConfiguration": True,
                "PublishCloudWatchMetricsEnabled": True,
                "BytesScannedCutoffPerQuery": 100_000_000,
                "RequesterPaysEnabled": False,
            },
            Description="AWS Data Wrangler Test WorkGroup Number 0",
        )
    yield wkg_name


@pytest.fixture(scope="module")
def workgroup1(bucket):
    wkg_name = "aws_data_wrangler_1"
    client = boto3.client("athena")
    wkgs = client.list_work_groups()
    wkgs = [x["Name"] for x in wkgs["WorkGroups"]]
    if wkg_name not in wkgs:
        client.create_work_group(
            Name=wkg_name,
            Configuration={
                "ResultConfiguration": {
                    "OutputLocation": f"s3://{bucket}/athena_workgroup1/",
                    "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
                },
                "EnforceWorkGroupConfiguration": True,
                "PublishCloudWatchMetricsEnabled": True,
                "BytesScannedCutoffPerQuery": 100_000_000,
                "RequesterPaysEnabled": False,
            },
            Description="AWS Data Wrangler Test WorkGroup Number 1",
        )
    yield wkg_name


@pytest.fixture(scope="module")
def workgroup2(bucket, kms_key):
    wkg_name = "aws_data_wrangler_2"
    client = boto3.client("athena")
    wkgs = client.list_work_groups()
    wkgs = [x["Name"] for x in wkgs["WorkGroups"]]
    if wkg_name not in wkgs:
        client.create_work_group(
            Name=wkg_name,
            Configuration={
                "ResultConfiguration": {
                    "OutputLocation": f"s3://{bucket}/athena_workgroup2/",
                    "EncryptionConfiguration": {"EncryptionOption": "SSE_KMS", "KmsKey": kms_key},
                },
                "EnforceWorkGroupConfiguration": False,
                "PublishCloudWatchMetricsEnabled": True,
                "BytesScannedCutoffPerQuery": 100_000_000,
                "RequesterPaysEnabled": False,
            },
            Description="AWS Data Wrangler Test WorkGroup Number 2",
        )
    yield wkg_name


@pytest.fixture(scope="module")
def workgroup3(bucket, kms_key):
    wkg_name = "aws_data_wrangler_3"
    client = boto3.client("athena")
    wkgs = client.list_work_groups()
    wkgs = [x["Name"] for x in wkgs["WorkGroups"]]
    if wkg_name not in wkgs:
        client.create_work_group(
            Name=wkg_name,
            Configuration={
                "ResultConfiguration": {
                    "OutputLocation": f"s3://{bucket}/athena_workgroup3/",
                    "EncryptionConfiguration": {"EncryptionOption": "SSE_KMS", "KmsKey": kms_key},
                },
                "EnforceWorkGroupConfiguration": True,
                "PublishCloudWatchMetricsEnabled": True,
                "BytesScannedCutoffPerQuery": 100_000_000,
                "RequesterPaysEnabled": False,
            },
            Description="AWS Data Wrangler Test WorkGroup Number 3",
        )
    yield wkg_name


@pytest.fixture(scope="function")
def table(database):
    name = f"tbl_{get_time_str_with_random_suffix()}"
    print(f"Table name: {name}")
    wr.catalog.delete_table_if_exists(database=database, table=name)
    yield name
    wr.catalog.delete_table_if_exists(database=database, table=name)


@pytest.fixture(scope="function")
def table2(database):
    name = f"tbl_{get_time_str_with_random_suffix()}"
    print(f"Table name: {name}")
    wr.catalog.delete_table_if_exists(database=database, table=name)
    yield name
    wr.catalog.delete_table_if_exists(database=database, table=name)


@pytest.fixture(scope="function")
def path(bucket):
    yield from path_generator(bucket)


@pytest.fixture(scope="function")
def path2(bucket):
    yield from path_generator(bucket)


@pytest.fixture(scope="function")
def path3(bucket):
    yield from path_generator(bucket)


def test_to_parquet_modes(database, table, path):

    # Round 1 - Warm up
    df = pd.DataFrame({"c0": [0, None]}, dtype="Int64")
    paths = wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode="overwrite",
        database=database,
        table=table,
        description="c0",
        parameters={"num_cols": str(len(df.columns)), "num_rows": str(len(df.index))},
        columns_comments={"c0": "0"},
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df2 = wr.athena.read_sql_table(table, database)
    assert df.shape == df2.shape
    assert df.c0.sum() == df2.c0.sum()
    parameters = wr.catalog.get_table_parameters(database, table)
    assert len(parameters) >= 5
    assert parameters["num_cols"] == str(len(df2.columns))
    assert parameters["num_rows"] == str(len(df2.index))
    assert wr.catalog.get_table_description(database, table) == "c0"
    comments = wr.catalog.get_columns_comments(database, table)
    assert len(comments) == len(df.columns)
    assert comments["c0"] == "0"

    # Round 2 - Overwrite
    df = pd.DataFrame({"c1": [None, 1, None]}, dtype="Int16")
    paths = wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode="overwrite",
        database=database,
        table=table,
        description="c1",
        parameters={"num_cols": str(len(df.columns)), "num_rows": str(len(df.index))},
        columns_comments={"c1": "1"},
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df2 = wr.athena.read_sql_table(table, database)
    assert df.shape == df2.shape
    assert df.c1.sum() == df2.c1.sum()
    parameters = wr.catalog.get_table_parameters(database, table)
    assert len(parameters) >= 5
    assert parameters["num_cols"] == str(len(df2.columns))
    assert parameters["num_rows"] == str(len(df2.index))
    assert wr.catalog.get_table_description(database, table) == "c1"
    comments = wr.catalog.get_columns_comments(database, table)
    assert len(comments) == len(df.columns)
    assert comments["c1"] == "1"

    # Round 3 - Append
    df = pd.DataFrame({"c1": [None, 2, None]}, dtype="Int8")
    paths = wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode="append",
        database=database,
        table=table,
        description="c1",
        parameters={"num_cols": str(len(df.columns)), "num_rows": str(len(df.index) * 2)},
        columns_comments={"c1": "1"},
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df2 = wr.athena.read_sql_table(table, database)
    assert len(df.columns) == len(df2.columns)
    assert len(df.index) * 2 == len(df2.index)
    assert df.c1.sum() + 1 == df2.c1.sum()
    parameters = wr.catalog.get_table_parameters(database, table)
    assert len(parameters) >= 5
    assert parameters["num_cols"] == str(len(df2.columns))
    assert parameters["num_rows"] == str(len(df2.index))
    assert wr.catalog.get_table_description(database, table) == "c1"
    comments = wr.catalog.get_columns_comments(database, table)
    assert len(comments) == len(df.columns)
    assert comments["c1"] == "1"

    # Round 4 - Append + New Column
    df = pd.DataFrame({"c2": ["a", None, "b"], "c1": [None, None, None]})
    paths = wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode="append",
        database=database,
        table=table,
        description="c1+c2",
        parameters={"num_cols": "2", "num_rows": "9"},
        columns_comments={"c1": "1", "c2": "2"},
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    df2 = wr.athena.read_sql_table(table, database)
    assert len(df2.columns) == 2
    assert len(df2.index) == 9
    assert df2.c1.sum() == 3
    parameters = wr.catalog.get_table_parameters(database, table)
    assert len(parameters) >= 5
    assert parameters["num_cols"] == "2"
    assert parameters["num_rows"] == "9"
    assert wr.catalog.get_table_description(database, table) == "c1+c2"
    comments = wr.catalog.get_columns_comments(database, table)
    assert len(comments) == len(df.columns)
    assert comments["c1"] == "1"
    assert comments["c2"] == "2"

    # Round 5 - Append + New Column + Wrong Types
    df = pd.DataFrame({"c2": [1], "c3": [True], "c1": ["1"]})
    paths = wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode="append",
        database=database,
        table=table,
        description="c1+c2+c3",
        parameters={"num_cols": "3", "num_rows": "10"},
        columns_comments={"c1": "1!", "c2": "2!", "c3": "3"},
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df2 = wr.athena.read_sql_table(table, database)
    assert len(df2.columns) == 3
    assert len(df2.index) == 10
    assert df2.c1.sum() == 4
    parameters = wr.catalog.get_table_parameters(database, table)
    assert len(parameters) >= 5
    assert parameters["num_cols"] == "3"
    assert parameters["num_rows"] == "10"
    assert wr.catalog.get_table_description(database, table) == "c1+c2+c3"
    comments = wr.catalog.get_columns_comments(database, table)
    assert len(comments) == len(df.columns)
    assert comments["c1"] == "1!"
    assert comments["c2"] == "2!"
    assert comments["c3"] == "3"

    # Round 6 - Overwrite Partitioned
    df = pd.DataFrame({"c0": ["foo", None], "c1": [0, 1]})
    paths = wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode="overwrite",
        database=database,
        table=table,
        partition_cols=["c1"],
        description="c0+c1",
        parameters={"num_cols": "2", "num_rows": "2"},
        columns_comments={"c0": "zero", "c1": "one"},
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    df2 = wr.athena.read_sql_table(table, database)
    assert df.shape == df2.shape
    assert df.c1.sum() == df2.c1.sum()
    parameters = wr.catalog.get_table_parameters(database, table)
    assert len(parameters) >= 5
    assert parameters["num_cols"] == "2"
    assert parameters["num_rows"] == "2"
    assert wr.catalog.get_table_description(database, table) == "c0+c1"
    comments = wr.catalog.get_columns_comments(database, table)
    assert len(comments) == len(df.columns)
    assert comments["c0"] == "zero"
    assert comments["c1"] == "one"

    # Round 7 - Overwrite Partitions
    df = pd.DataFrame({"c0": [None, None], "c1": [0, 2]})
    paths = wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode="overwrite_partitions",
        database=database,
        table=table,
        partition_cols=["c1"],
        description="c0+c1",
        parameters={"num_cols": "2", "num_rows": "3"},
        columns_comments={"c0": "zero", "c1": "one"},
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df2 = wr.athena.read_sql_table(table, database)
    assert len(df2.columns) == 2
    assert len(df2.index) == 3
    assert df2.c1.sum() == 3
    parameters = wr.catalog.get_table_parameters(database, table)
    assert len(parameters) >= 5
    assert parameters["num_cols"] == "2"
    assert parameters["num_rows"] == "3"
    assert wr.catalog.get_table_description(database, table) == "c0+c1"
    comments = wr.catalog.get_columns_comments(database, table)
    assert len(comments) == len(df.columns)
    assert comments["c0"] == "zero"
    assert comments["c1"] == "one"

    # Round 8 - Overwrite Partitions + New Column + Wrong Type
    df = pd.DataFrame({"c0": [1, 2], "c1": ["1", "3"], "c2": [True, False]})
    paths = wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode="overwrite_partitions",
        database=database,
        table=table,
        partition_cols=["c1"],
        description="c0+c1+c2",
        parameters={"num_cols": "3", "num_rows": "4"},
        columns_comments={"c0": "zero", "c1": "one", "c2": "two"},
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df2 = wr.athena.read_sql_table(table, database)
    assert len(df2.columns) == 3
    assert len(df2.index) == 4
    assert df2.c1.sum() == 6
    parameters = wr.catalog.get_table_parameters(database, table)
    assert len(parameters) >= 5
    assert parameters["num_cols"] == "3"
    assert parameters["num_rows"] == "4"
    assert wr.catalog.get_table_description(database, table) == "c0+c1+c2"
    comments = wr.catalog.get_columns_comments(database, table)
    assert len(comments) == len(df.columns)
    assert comments["c0"] == "zero"
    assert comments["c1"] == "one"
    assert comments["c2"] == "two"


def test_store_parquet_metadata_modes(database, table, path):

    # Round 1 - Warm up
    df = pd.DataFrame({"c0": [0, None]}, dtype="Int64")
    paths = wr.s3.to_parquet(df=df, path=path, dataset=True, mode="overwrite")["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    wr.s3.store_parquet_metadata(
        path=path,
        dataset=True,
        mode="overwrite",
        database=database,
        table=table,
        description="c0",
        parameters={"num_cols": str(len(df.columns)), "num_rows": str(len(df.index))},
        columns_comments={"c0": "0"},
    )
    df2 = wr.athena.read_sql_table(table, database)
    assert df.shape == df2.shape
    assert df.c0.sum() == df2.c0.sum()
    parameters = wr.catalog.get_table_parameters(database, table)
    assert len(parameters) >= 5
    assert parameters["num_cols"] == str(len(df2.columns))
    assert parameters["num_rows"] == str(len(df2.index))
    assert wr.catalog.get_table_description(database, table) == "c0"
    comments = wr.catalog.get_columns_comments(database, table)
    assert len(comments) == len(df.columns)
    assert comments["c0"] == "0"

    # Round 2 - Overwrite
    df = pd.DataFrame({"c1": [None, 1, None]}, dtype="Int16")
    paths = wr.s3.to_parquet(df=df, path=path, dataset=True, mode="overwrite")["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    wr.s3.store_parquet_metadata(
        path=path,
        dataset=True,
        mode="overwrite",
        database=database,
        table=table,
        description="c1",
        parameters={"num_cols": str(len(df.columns)), "num_rows": str(len(df.index))},
        columns_comments={"c1": "1"},
    )
    df2 = wr.athena.read_sql_table(table, database)
    assert df.shape == df2.shape
    assert df.c1.sum() == df2.c1.sum()
    parameters = wr.catalog.get_table_parameters(database, table)
    assert len(parameters) >= 5
    assert parameters["num_cols"] == str(len(df2.columns))
    assert parameters["num_rows"] == str(len(df2.index))
    assert wr.catalog.get_table_description(database, table) == "c1"
    comments = wr.catalog.get_columns_comments(database, table)
    assert len(comments) == len(df.columns)
    assert comments["c1"] == "1"

    # Round 3 - Append
    df = pd.DataFrame({"c1": [None, 2, None]}, dtype="Int16")
    paths = wr.s3.to_parquet(df=df, path=path, dataset=True, mode="append")["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    wr.s3.store_parquet_metadata(
        path=path,
        dataset=True,
        mode="append",
        database=database,
        table=table,
        description="c1",
        parameters={"num_cols": str(len(df.columns)), "num_rows": str(len(df.index) * 2)},
        columns_comments={"c1": "1"},
    )
    df2 = wr.athena.read_sql_table(table, database)
    assert len(df.columns) == len(df2.columns)
    assert len(df.index) * 2 == len(df2.index)
    assert df.c1.sum() + 1 == df2.c1.sum()
    parameters = wr.catalog.get_table_parameters(database, table)
    assert len(parameters) >= 5
    assert parameters["num_cols"] == str(len(df2.columns))
    assert parameters["num_rows"] == str(len(df2.index))
    assert wr.catalog.get_table_description(database, table) == "c1"
    comments = wr.catalog.get_columns_comments(database, table)
    assert len(comments) == len(df.columns)
    assert comments["c1"] == "1"

    # Round 4 - Append + New Column
    df = pd.DataFrame({"c2": ["a", None, "b"], "c1": [None, 1, None]})
    df["c1"] = df["c1"].astype("Int16")
    paths = wr.s3.to_parquet(df=df, path=path, dataset=True, mode="append")["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    wr.s3.store_parquet_metadata(
        path=path,
        dataset=True,
        mode="append",
        database=database,
        table=table,
        description="c1+c2",
        parameters={"num_cols": "2", "num_rows": "9"},
        columns_comments={"c1": "1", "c2": "2"},
    )
    df2 = wr.athena.read_sql_table(table, database)
    assert len(df2.columns) == 2
    assert len(df2.index) == 9
    assert df2.c1.sum() == 4
    parameters = wr.catalog.get_table_parameters(database, table)
    assert len(parameters) >= 5
    assert parameters["num_cols"] == "2"
    assert parameters["num_rows"] == "9"
    assert wr.catalog.get_table_description(database, table) == "c1+c2"
    comments = wr.catalog.get_columns_comments(database, table)
    assert len(comments) == len(df.columns)
    assert comments["c1"] == "1"
    assert comments["c2"] == "2"

    # Round 5 - Overwrite Partitioned
    df = pd.DataFrame({"c0": ["foo", None], "c1": [0, 1]})
    paths = wr.s3.to_parquet(df=df, path=path, dataset=True, mode="overwrite", partition_cols=["c1"])["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    wr.s3.store_parquet_metadata(
        path=path,
        dataset=True,
        mode="overwrite",
        database=database,
        table=table,
        description="c0+c1",
        parameters={"num_cols": "2", "num_rows": "2"},
        columns_comments={"c0": "zero", "c1": "one"},
    )
    df2 = wr.athena.read_sql_table(table, database)
    assert df.shape == df2.shape
    assert df.c1.sum() == df2.c1.astype(int).sum()
    parameters = wr.catalog.get_table_parameters(database, table)
    assert len(parameters) >= 5
    assert parameters["num_cols"] == "2"
    assert parameters["num_rows"] == "2"
    assert wr.catalog.get_table_description(database, table) == "c0+c1"
    comments = wr.catalog.get_columns_comments(database, table)
    assert len(comments) == len(df.columns)
    assert comments["c0"] == "zero"
    assert comments["c1"] == "one"

    # Round 6 - Overwrite Partitions
    df = pd.DataFrame({"c0": [None, "boo"], "c1": [0, 2]})
    paths = wr.s3.to_parquet(df=df, path=path, dataset=True, mode="overwrite_partitions", partition_cols=["c1"])[
        "paths"
    ]
    wr.s3.wait_objects_exist(paths=paths)
    wr.s3.store_parquet_metadata(
        path=path,
        dataset=True,
        mode="append",
        database=database,
        table=table,
        description="c0+c1",
        parameters={"num_cols": "2", "num_rows": "3"},
        columns_comments={"c0": "zero", "c1": "one"},
    )
    df2 = wr.athena.read_sql_table(table, database)
    assert len(df2.columns) == 2
    assert len(df2.index) == 3
    assert df2.c1.astype(int).sum() == 3
    parameters = wr.catalog.get_table_parameters(database, table)
    assert len(parameters) >= 5
    assert parameters["num_cols"] == "2"
    assert parameters["num_rows"] == "3"
    assert wr.catalog.get_table_description(database, table) == "c0+c1"
    comments = wr.catalog.get_columns_comments(database, table)
    assert len(comments) == len(df.columns)
    assert comments["c0"] == "zero"
    assert comments["c1"] == "one"

    # Round 7 - Overwrite Partitions + New Column
    df = pd.DataFrame({"c0": ["bar", None], "c1": [1, 3], "c2": [True, False]})
    paths = wr.s3.to_parquet(df=df, path=path, dataset=True, mode="overwrite_partitions", partition_cols=["c1"])[
        "paths"
    ]
    wr.s3.wait_objects_exist(paths=paths)
    wr.s3.store_parquet_metadata(
        path=path,
        dataset=True,
        mode="append",
        database=database,
        table=table,
        description="c0+c1+c2",
        parameters={"num_cols": "3", "num_rows": "4"},
        columns_comments={"c0": "zero", "c1": "one", "c2": "two"},
    )
    df2 = wr.athena.read_sql_table(table, database)
    assert len(df2.columns) == 3
    assert len(df2.index) == 4
    assert df2.c1.astype(int).sum() == 6
    parameters = wr.catalog.get_table_parameters(database, table)
    assert len(parameters) >= 5
    assert parameters["num_cols"] == "3"
    assert parameters["num_rows"] == "4"
    assert wr.catalog.get_table_description(database, table) == "c0+c1+c2"
    comments = wr.catalog.get_columns_comments(database, table)
    assert len(comments) == len(df.columns)
    assert comments["c0"] == "zero"
    assert comments["c1"] == "one"
    assert comments["c2"] == "two"


def test_athena_ctas(path, path2, path3, table, table2, database, kms_key):
    df = get_df_list()
    columns_types, partitions_types = wr.catalog.extract_athena_types(df=df, partition_cols=["par0", "par1"])
    assert len(columns_types) == 17
    assert len(partitions_types) == 2
    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.catalog.extract_athena_types(df=df, file_format="avro")
    paths = wr.s3.to_parquet(
        df=get_df_list(),
        path=path,
        index=True,
        use_threads=True,
        dataset=True,
        mode="overwrite",
        database=database,
        table=table,
        partition_cols=["par0", "par1"],
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    dirs = wr.s3.list_directories(path=path)
    for d in dirs:
        assert d.startswith(f"{path}par0=")
    df = wr.s3.read_parquet_table(table=table, database=database)
    assert len(df.index) == 3
    ensure_data_types(df=df, has_list=True)
    df = wr.athena.read_sql_table(
        table=table,
        database=database,
        ctas_approach=True,
        encryption="SSE_KMS",
        kms_key=kms_key,
        s3_output=path2,
        keep_files=False,
    )
    assert len(df.index) == 3
    ensure_data_types(df=df, has_list=True)
    final_destination = f"{path3}{table2}/"

    # keep_files=False
    wr.s3.delete_objects(path=path3)
    dfs = wr.athena.read_sql_query(
        sql=f"SELECT * FROM {table}",
        database=database,
        ctas_approach=True,
        chunksize=1,
        keep_files=False,
        ctas_temp_table_name=table2,
        s3_output=path3,
    )
    assert wr.catalog.does_table_exist(database=database, table=table2) is False
    assert len(wr.s3.list_objects(path=path3)) > 2
    assert len(wr.s3.list_objects(path=final_destination)) > 0
    for df in dfs:
        ensure_data_types(df=df, has_list=True)
    assert len(wr.s3.list_objects(path=path3)) == 0

    # keep_files=True
    wr.s3.delete_objects(path=path3)
    dfs = wr.athena.read_sql_query(
        sql=f"SELECT * FROM {table}",
        database=database,
        ctas_approach=True,
        chunksize=2,
        keep_files=True,
        ctas_temp_table_name=table2,
        s3_output=path3,
    )
    assert wr.catalog.does_table_exist(database=database, table=table2) is False
    assert len(wr.s3.list_objects(path=path3)) > 2
    assert len(wr.s3.list_objects(path=final_destination)) > 0
    for df in dfs:
        ensure_data_types(df=df, has_list=True)
    assert len(wr.s3.list_objects(path=path3)) > 2


def test_athena(path, database, kms_key, workgroup0, workgroup1):
    wr.catalog.delete_table_if_exists(database=database, table="__test_athena")
    paths = wr.s3.to_parquet(
        df=get_df(),
        path=path,
        index=True,
        use_threads=True,
        dataset=True,
        mode="overwrite",
        database=database,
        table="__test_athena",
        partition_cols=["par0", "par1"],
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    dfs = wr.athena.read_sql_query(
        sql="SELECT * FROM __test_athena",
        database=database,
        ctas_approach=False,
        chunksize=1,
        encryption="SSE_KMS",
        kms_key=kms_key,
        workgroup=workgroup0,
        keep_files=False,
    )
    for df2 in dfs:
        ensure_data_types(df=df2)
    df = wr.athena.read_sql_query(
        sql="SELECT * FROM __test_athena",
        database=database,
        ctas_approach=False,
        workgroup=workgroup1,
        keep_files=False,
    )
    assert len(df.index) == 3
    ensure_data_types(df=df)
    wr.athena.repair_table(table="__test_athena", database=database)
    wr.catalog.delete_table_if_exists(database=database, table="__test_athena")


def test_parquet_catalog(bucket, database):
    with pytest.raises(wr.exceptions.UndetectedType):
        wr.s3.to_parquet(
            df=pd.DataFrame({"A": [None]}),
            path=f"s3://{bucket}/test_parquet_catalog",
            dataset=True,
            database=database,
            table="test_parquet_catalog",
        )
    df = get_df_list()
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_parquet(
            df=df,
            path=f"s3://{bucket}/test_parquet_catalog",
            use_threads=True,
            dataset=False,
            mode="overwrite",
            database=database,
            table="test_parquet_catalog",
        )
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_parquet(
            df=df,
            path=f"s3://{bucket}/test_parquet_catalog",
            use_threads=True,
            dataset=False,
            table="test_parquet_catalog",
        )
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_parquet(
            df=df,
            path=f"s3://{bucket}/test_parquet_catalog",
            use_threads=True,
            dataset=True,
            mode="overwrite",
            database=database,
        )
    wr.s3.to_parquet(
        df=df,
        path=f"s3://{bucket}/test_parquet_catalog",
        use_threads=True,
        dataset=True,
        mode="overwrite",
        database=database,
        table="test_parquet_catalog",
    )
    wr.s3.to_parquet(
        df=df,
        path=f"s3://{bucket}/test_parquet_catalog2",
        index=True,
        use_threads=True,
        dataset=True,
        mode="overwrite",
        database=database,
        table="test_parquet_catalog2",
        partition_cols=["iint8", "iint16"],
    )
    columns_types, partitions_types = wr.s3.read_parquet_metadata(
        path=f"s3://{bucket}/test_parquet_catalog2", dataset=True
    )
    assert len(columns_types) == 18
    assert len(partitions_types) == 2
    columns_types, partitions_types, partitions_values = wr.s3.store_parquet_metadata(
        path=f"s3://{bucket}/test_parquet_catalog2", database=database, table="test_parquet_catalog2", dataset=True
    )
    assert len(columns_types) == 18
    assert len(partitions_types) == 2
    assert len(partitions_values) == 2
    wr.s3.delete_objects(path=f"s3://{bucket}/test_parquet_catalog/")
    wr.s3.delete_objects(path=f"s3://{bucket}/test_parquet_catalog2/")
    assert wr.catalog.delete_table_if_exists(database=database, table="test_parquet_catalog") is True
    assert wr.catalog.delete_table_if_exists(database=database, table="test_parquet_catalog2") is True


def test_parquet_catalog_duplicated(path, table, database):
    df = pd.DataFrame({"A": [1], "a": [1]})
    wr.s3.to_parquet(df=df, path=path, index=False, dataset=True, mode="overwrite", database=database, table=table)
    df = wr.s3.read_parquet(path=path)
    assert df.shape == (1, 1)


def test_parquet_catalog_casting(path, database):
    paths = wr.s3.to_parquet(
        df=get_df_cast(),
        path=path,
        index=False,
        dataset=True,
        mode="overwrite",
        database=database,
        table="__test_parquet_catalog_casting",
        dtype={
            "iint8": "tinyint",
            "iint16": "smallint",
            "iint32": "int",
            "iint64": "bigint",
            "float": "float",
            "double": "double",
            "decimal": "decimal(3,2)",
            "string": "string",
            "date": "date",
            "timestamp": "timestamp",
            "bool": "boolean",
            "binary": "binary",
            "category": "double",
            "par0": "bigint",
            "par1": "string",
        },
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    df = wr.s3.read_parquet(path=path)
    assert df.shape == (3, 16)
    ensure_data_types(df=df, has_list=False)
    df = wr.athena.read_sql_table(table="__test_parquet_catalog_casting", database=database, ctas_approach=True)
    assert df.shape == (3, 16)
    ensure_data_types(df=df, has_list=False)
    df = wr.athena.read_sql_table(table="__test_parquet_catalog_casting", database=database, ctas_approach=False)
    assert df.shape == (3, 16)
    ensure_data_types(df=df, has_list=False)
    wr.s3.delete_objects(path=path)
    assert wr.catalog.delete_table_if_exists(database=database, table="__test_parquet_catalog_casting") is True


def test_catalog(path, database, table):
    account_id = boto3.client("sts").get_caller_identity().get("Account")
    assert wr.catalog.does_table_exist(database=database, table=table) is False
    wr.catalog.create_parquet_table(
        database=database,
        table=table,
        path=path,
        columns_types={"col0": "int", "col1": "double"},
        partitions_types={"y": "int", "m": "int"},
        compression="snappy",
    )
    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.catalog.create_parquet_table(
            database=database, table=table, path=path, columns_types={"col0": "string"}, mode="append"
        )
    assert wr.catalog.does_table_exist(database=database, table=table) is True
    assert wr.catalog.delete_table_if_exists(database=database, table=table) is True
    assert wr.catalog.delete_table_if_exists(database=database, table=table) is False
    wr.catalog.create_parquet_table(
        database=database,
        table=table,
        path=path,
        columns_types={"col0": "int", "col1": "double"},
        partitions_types={"y": "int", "m": "int"},
        compression="snappy",
        description="Foo boo bar",
        parameters={"tag": "test"},
        columns_comments={"col0": "my int", "y": "year"},
        mode="overwrite",
    )
    wr.catalog.add_parquet_partitions(
        database=database,
        table=table,
        partitions_values={f"{path}y=2020/m=1/": ["2020", "1"], f"{path}y=2021/m=2/": ["2021", "2"]},
        compression="snappy",
    )
    assert wr.catalog.get_table_location(database=database, table=table) == path
    partitions_values = wr.catalog.get_parquet_partitions(database=database, table=table)
    assert len(partitions_values) == 2
    partitions_values = wr.catalog.get_parquet_partitions(
        database=database, table=table, catalog_id=account_id, expression="y = 2021 AND m = 2"
    )
    assert len(partitions_values) == 1
    assert len(set(partitions_values[f"{path}y=2021/m=2/"]) & {"2021", "2"}) == 2
    dtypes = wr.catalog.get_table_types(database=database, table=table)
    assert dtypes["col0"] == "int"
    assert dtypes["col1"] == "double"
    assert dtypes["y"] == "int"
    assert dtypes["m"] == "int"
    df_dbs = wr.catalog.databases()
    assert len(wr.catalog.databases(catalog_id=account_id)) == len(df_dbs)
    assert database in df_dbs["Database"].to_list()
    tables = list(wr.catalog.get_tables())
    assert len(tables) > 0
    for tbl in tables:
        if tbl["Name"] == table:
            assert tbl["TableType"] == "EXTERNAL_TABLE"
    tables = list(wr.catalog.get_tables(database=database))
    assert len(tables) > 0
    for tbl in tables:
        assert tbl["DatabaseName"] == database
    # search
    tables = list(wr.catalog.search_tables(text="parquet", catalog_id=account_id))
    assert len(tables) > 0
    for tbl in tables:
        if tbl["Name"] == table:
            assert tbl["TableType"] == "EXTERNAL_TABLE"
    # prefix
    tables = list(wr.catalog.get_tables(name_prefix=table[:4], catalog_id=account_id))
    assert len(tables) > 0
    for tbl in tables:
        if tbl["Name"] == table:
            assert tbl["TableType"] == "EXTERNAL_TABLE"
    # suffix
    tables = list(wr.catalog.get_tables(name_suffix=table[-4:], catalog_id=account_id))
    assert len(tables) > 0
    for tbl in tables:
        if tbl["Name"] == table:
            assert tbl["TableType"] == "EXTERNAL_TABLE"
    # name_contains
    tables = list(wr.catalog.get_tables(name_contains=table[4:-4], catalog_id=account_id))
    assert len(tables) > 0
    for tbl in tables:
        if tbl["Name"] == table:
            assert tbl["TableType"] == "EXTERNAL_TABLE"
    # prefix & suffix & name_contains
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        list(
            wr.catalog.get_tables(
                name_prefix=table[0], name_contains=table[3], name_suffix=table[-1], catalog_id=account_id
            )
        )
    # prefix & suffix
    tables = list(wr.catalog.get_tables(name_prefix=table[0], name_suffix=table[-1], catalog_id=account_id))
    assert len(tables) > 0
    for tbl in tables:
        if tbl["Name"] == table:
            assert tbl["TableType"] == "EXTERNAL_TABLE"
    # DataFrames
    assert len(wr.catalog.databases().index) > 0
    assert len(wr.catalog.tables().index) > 0
    assert (
        len(
            wr.catalog.tables(
                database=database,
                search_text="parquet",
                name_prefix=table[0],
                name_contains=table[3],
                name_suffix=table[-1],
                catalog_id=account_id,
            ).index
        )
        > 0
    )
    assert len(wr.catalog.table(database=database, table=table).index) > 0
    assert len(wr.catalog.table(database=database, table=table, catalog_id=account_id).index) > 0
    with pytest.raises(wr.exceptions.InvalidTable):
        wr.catalog.overwrite_table_parameters({"foo": "boo"}, database, "fake_table")


def test_catalog_get_databases(database):
    dbs = list(wr.catalog.get_databases())
    assert len(dbs) > 0
    for db in dbs:
        if db["Name"] == database:
            assert db["Description"] == "AWS Data Wrangler Test Arena - Glue Database"


def test_athena_query_cancelled(database):
    session = boto3.DEFAULT_SESSION
    query_execution_id = wr.athena.start_query_execution(sql=get_query_long(), database=database, boto3_session=session)
    wr.athena.stop_query_execution(query_execution_id=query_execution_id, boto3_session=session)
    with pytest.raises(wr.exceptions.QueryCancelled):
        assert wr.athena.wait_query(query_execution_id=query_execution_id)


def test_athena_query_failed(database):
    query_execution_id = wr.athena.start_query_execution(sql="SELECT random(-1)", database=database)
    with pytest.raises(wr.exceptions.QueryFailed):
        assert wr.athena.wait_query(query_execution_id=query_execution_id)


def test_athena_read_list(database):
    with pytest.raises(wr.exceptions.UnsupportedType):
        wr.athena.read_sql_query(sql="SELECT ARRAY[1, 2, 3]", database=database, ctas_approach=False)


def test_sanitize_names():
    assert wr.catalog.sanitize_column_name("CamelCase") == "camel_case"
    assert wr.catalog.sanitize_column_name("CamelCase2") == "camel_case2"
    assert wr.catalog.sanitize_column_name("Camel_Case3") == "camel_case3"
    assert wr.catalog.sanitize_column_name("Cámël_Casë4仮") == "camel_case4_"
    assert wr.catalog.sanitize_column_name("Camel__Case5") == "camel__case5"
    assert wr.catalog.sanitize_column_name("Camel{}Case6") == "camel_case6"
    assert wr.catalog.sanitize_column_name("Camel.Case7") == "camel_case7"
    assert wr.catalog.sanitize_column_name("xyz_cd") == "xyz_cd"
    assert wr.catalog.sanitize_column_name("xyz_Cd") == "xyz_cd"
    assert wr.catalog.sanitize_table_name("CamelCase") == "camel_case"
    assert wr.catalog.sanitize_table_name("CamelCase2") == "camel_case2"
    assert wr.catalog.sanitize_table_name("Camel_Case3") == "camel_case3"
    assert wr.catalog.sanitize_table_name("Cámël_Casë4仮") == "camel_case4_"
    assert wr.catalog.sanitize_table_name("Camel__Case5") == "camel__case5"
    assert wr.catalog.sanitize_table_name("Camel{}Case6") == "camel_case6"
    assert wr.catalog.sanitize_table_name("Camel.Case7") == "camel_case7"
    assert wr.catalog.sanitize_table_name("xyz_cd") == "xyz_cd"
    assert wr.catalog.sanitize_table_name("xyz_Cd") == "xyz_cd"


def test_athena_ctas_empty(database):
    sql = """
        WITH dataset AS (
          SELECT 0 AS id
        )
        SELECT id
        FROM dataset
        WHERE id != 0
    """
    assert wr.athena.read_sql_query(sql=sql, database=database).empty is True
    assert len(list(wr.athena.read_sql_query(sql=sql, database=database, chunksize=1))) == 0


def test_athena_struct(database):
    sql = "SELECT CAST(ROW(1, 'foo') AS ROW(id BIGINT, value VARCHAR)) AS col0"
    with pytest.raises(wr.exceptions.UnsupportedType):
        wr.athena.read_sql_query(sql=sql, database=database, ctas_approach=False)
    df = wr.athena.read_sql_query(sql=sql, database=database, ctas_approach=True)
    assert len(df.index) == 1
    assert len(df.columns) == 1
    assert df["col0"].iloc[0]["id"] == 1
    assert df["col0"].iloc[0]["value"] == "foo"
    sql = "SELECT ROW(1, ROW(2, ROW(3, '4'))) AS col0"
    df = wr.athena.read_sql_query(sql=sql, database=database, ctas_approach=True)
    assert len(df.index) == 1
    assert len(df.columns) == 1
    assert df["col0"].iloc[0]["field0"] == 1
    assert df["col0"].iloc[0]["field1"]["field0"] == 2
    assert df["col0"].iloc[0]["field1"]["field1"]["field0"] == 3
    assert df["col0"].iloc[0]["field1"]["field1"]["field1"] == "4"


def test_athena_time_zone(database):
    sql = "SELECT current_timestamp AS value, typeof(current_timestamp) AS type"
    df = wr.athena.read_sql_query(sql=sql, database=database, ctas_approach=False)
    assert len(df.index) == 1
    assert len(df.columns) == 2
    assert df["type"][0] == "timestamp with time zone"
    assert df["value"][0].year == datetime.datetime.utcnow().year


def test_category(bucket, database):
    df = get_df_category()
    path = f"s3://{bucket}/test_category/"
    paths = wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        database=database,
        table="test_category",
        mode="overwrite",
        partition_cols=["par0", "par1"],
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df2 = wr.s3.read_parquet(path=path, dataset=True, categories=[c for c in df.columns if c not in ["par0", "par1"]])
    ensure_data_types_category(df2)
    df2 = wr.athena.read_sql_query("SELECT * FROM test_category", database=database, categories=list(df.columns))
    ensure_data_types_category(df2)
    df2 = wr.athena.read_sql_table(table="test_category", database=database, categories=list(df.columns))
    ensure_data_types_category(df2)
    df2 = wr.athena.read_sql_query(
        "SELECT * FROM test_category", database=database, categories=list(df.columns), ctas_approach=False
    )
    ensure_data_types_category(df2)
    dfs = wr.athena.read_sql_query(
        "SELECT * FROM test_category", database=database, categories=list(df.columns), ctas_approach=False, chunksize=1
    )
    for df2 in dfs:
        ensure_data_types_category(df2)
    dfs = wr.athena.read_sql_query(
        "SELECT * FROM test_category", database=database, categories=list(df.columns), ctas_approach=True, chunksize=1
    )
    for df2 in dfs:
        ensure_data_types_category(df2)
    wr.s3.delete_objects(path=paths)
    assert wr.catalog.delete_table_if_exists(database=database, table="test_category") is True


def test_csv_dataset(path, database):
    with pytest.raises(wr.exceptions.UndetectedType):
        wr.s3.to_csv(pd.DataFrame({"A": [None]}), path, dataset=True, database=database, table="test_csv_dataset")
    df = get_df_csv()
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_csv(df, path, dataset=False, mode="overwrite", database=database, table="test_csv_dataset")
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_csv(df, path, dataset=False, table="test_csv_dataset")
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_csv(df, path, dataset=True, mode="overwrite", database=database)
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_csv(df=df, path=path, mode="append")
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_csv(df=df, path=path, partition_cols=["col2"])
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_csv(df=df, path=path, description="foo")
    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.s3.to_csv(df=df, path=path, partition_cols=["col2"], dataset=True, mode="WRONG")
    paths = wr.s3.to_csv(
        df=df,
        path=path,
        sep="|",
        index=False,
        use_threads=True,
        boto3_session=None,
        s3_additional_kwargs=None,
        dataset=True,
        partition_cols=["par0", "par1"],
        mode="overwrite",
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    df2 = wr.s3.read_csv(path=paths, sep="|", header=None)
    assert len(df2.index) == 3
    assert len(df2.columns) == 8
    assert df2[0].sum() == 6
    wr.s3.delete_objects(path=paths)


def test_csv_catalog(path, table, database):
    df = get_df_csv()
    paths = wr.s3.to_csv(
        df=df,
        path=path,
        sep="\t",
        index=True,
        use_threads=True,
        boto3_session=None,
        s3_additional_kwargs=None,
        dataset=True,
        partition_cols=["par0", "par1"],
        mode="overwrite",
        table=table,
        database=database,
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    df2 = wr.athena.read_sql_table(table, database)
    assert len(df2.index) == 3
    assert len(df2.columns) == 11
    assert df2["id"].sum() == 6
    ensure_data_types_csv(df2)
    wr.s3.delete_objects(path=paths)
    assert wr.catalog.delete_table_if_exists(database=database, table=table) is True


def test_csv_catalog_columns(bucket, database):
    path = f"s3://{bucket}/test_csv_catalog_columns /"
    paths = wr.s3.to_csv(
        df=get_df_csv(),
        path=path,
        sep="|",
        columns=["id", "date", "timestamp", "par0", "par1"],
        index=False,
        use_threads=False,
        boto3_session=None,
        s3_additional_kwargs=None,
        dataset=True,
        partition_cols=["par0", "par1"],
        mode="overwrite",
        table="test_csv_catalog_columns",
        database=database,
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    df2 = wr.athena.read_sql_table("test_csv_catalog_columns", database)
    assert len(df2.index) == 3
    assert len(df2.columns) == 5
    assert df2["id"].sum() == 6
    ensure_data_types_csv(df2)

    paths = wr.s3.to_csv(
        df=pd.DataFrame({"id": [4], "date": [None], "timestamp": [None], "par0": [1], "par1": ["a"]}),
        path=path,
        sep="|",
        index=False,
        use_threads=False,
        boto3_session=None,
        s3_additional_kwargs=None,
        dataset=True,
        partition_cols=["par0", "par1"],
        mode="overwrite_partitions",
        table="test_csv_catalog_columns",
        database=database,
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    df2 = wr.athena.read_sql_table("test_csv_catalog_columns", database)
    assert len(df2.index) == 3
    assert len(df2.columns) == 5
    assert df2["id"].sum() == 9
    ensure_data_types_csv(df2)

    wr.s3.delete_objects(path=path)
    assert wr.catalog.delete_table_if_exists(database=database, table="test_csv_catalog_columns") is True


def test_athena_types(bucket, database):
    path = f"s3://{bucket}/test_athena_types/"
    df = get_df_csv()
    paths = wr.s3.to_csv(
        df=df,
        path=path,
        sep=",",
        index=False,
        use_threads=True,
        boto3_session=None,
        s3_additional_kwargs=None,
        dataset=True,
        partition_cols=["par0", "par1"],
        mode="overwrite",
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    columns_types, partitions_types = wr.catalog.extract_athena_types(
        df=df, index=False, partition_cols=["par0", "par1"], file_format="csv"
    )
    wr.catalog.create_csv_table(
        table="test_athena_types",
        database=database,
        path=path,
        partitions_types=partitions_types,
        columns_types=columns_types,
    )
    wr.catalog.create_csv_table(
        database=database, table="test_athena_types", path=path, columns_types={"col0": "string"}, mode="append"
    )
    wr.athena.repair_table("test_athena_types", database)
    assert len(wr.catalog.get_csv_partitions(database, "test_athena_types")) == 3
    df2 = wr.athena.read_sql_table("test_athena_types", database)
    assert len(df2.index) == 3
    assert len(df2.columns) == 10
    assert df2["id"].sum() == 6
    ensure_data_types_csv(df2)
    wr.s3.delete_objects(path=paths)
    assert wr.catalog.delete_table_if_exists(database=database, table="test_athena_types") is True


def test_parquet_catalog_columns(bucket, database):
    path = f"s3://{bucket}/test_parquet_catalog_columns/"
    paths = wr.s3.to_parquet(
        df=get_df_csv()[["id", "date", "timestamp", "par0", "par1"]],
        path=path,
        index=False,
        use_threads=False,
        boto3_session=None,
        s3_additional_kwargs=None,
        dataset=True,
        partition_cols=["par0", "par1"],
        mode="overwrite",
        table="test_parquet_catalog_columns",
        database=database,
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    df2 = wr.athena.read_sql_table("test_parquet_catalog_columns", database)
    assert len(df2.index) == 3
    assert len(df2.columns) == 5
    assert df2["id"].sum() == 6
    ensure_data_types_csv(df2)

    paths = wr.s3.to_parquet(
        df=pd.DataFrame({"id": [4], "date": [None], "timestamp": [None], "par0": [1], "par1": ["a"]}),
        path=path,
        index=False,
        use_threads=False,
        boto3_session=None,
        s3_additional_kwargs=None,
        dataset=True,
        partition_cols=["par0", "par1"],
        mode="overwrite_partitions",
        table="test_parquet_catalog_columns",
        database=database,
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    df2 = wr.athena.read_sql_table("test_parquet_catalog_columns", database)
    assert len(df2.index) == 3
    assert len(df2.columns) == 5
    assert df2["id"].sum() == 9
    ensure_data_types_csv(df2)

    wr.s3.delete_objects(path=path)
    assert wr.catalog.delete_table_if_exists(database=database, table="test_parquet_catalog_columns") is True


@pytest.mark.parametrize("compression", [None, "gzip", "snappy"])
def test_parquet_compress(bucket, database, compression):
    path = f"s3://{bucket}/test_parquet_compress_{compression}/"
    paths = wr.s3.to_parquet(
        df=get_df(),
        path=path,
        compression=compression,
        dataset=True,
        database=database,
        table=f"test_parquet_compress_{compression}",
        mode="overwrite",
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    df2 = wr.athena.read_sql_table(f"test_parquet_compress_{compression}", database)
    ensure_data_types(df2)
    df2 = wr.s3.read_parquet(path=path)
    wr.s3.delete_objects(path=path)
    assert wr.catalog.delete_table_if_exists(database=database, table=f"test_parquet_compress_{compression}") is True
    ensure_data_types(df2)


def test_parquet_char_length(path, database, table):
    df = pd.DataFrame(
        {"id": [1, 2], "cchar": ["foo", "boo"], "date": [datetime.date(2020, 1, 1), datetime.date(2020, 1, 2)]}
    )
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        database=database,
        table=table,
        mode="overwrite",
        partition_cols=["date"],
        dtype={"cchar": "char(3)"},
    )

    df2 = wr.s3.read_parquet(path, dataset=True)
    assert len(df2.index) == 2
    assert len(df2.columns) == 3
    assert df2.id.sum() == 3

    df2 = wr.athena.read_sql_table(table=table, database=database)
    assert len(df2.index) == 2
    assert len(df2.columns) == 3
    assert df2.id.sum() == 3


@pytest.mark.parametrize("col2", [[1, 1, 1, 1, 1], [1, 2, 3, 4, 5], [1, 1, 1, 1, 2], [1, 2, 2, 2, 2]])
@pytest.mark.parametrize("chunked", [True, 1, 2, 100])
def test_parquet_chunked(bucket, database, col2, chunked):
    table = f"test_parquet_chunked_{chunked}_{''.join([str(x) for x in col2])}"
    path = f"s3://{bucket}/{table}/"
    wr.s3.delete_objects(path=path)
    values = list(range(5))
    df = pd.DataFrame({"col1": values, "col2": col2})
    paths = wr.s3.to_parquet(
        df, path, index=False, dataset=True, database=database, table=table, partition_cols=["col2"], mode="overwrite"
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths)

    dfs = list(wr.s3.read_parquet(path=path, dataset=True, chunked=chunked))
    assert sum(values) == pd.concat(dfs, ignore_index=True).col1.sum()
    if chunked is not True:
        assert len(dfs) == int(math.ceil(len(df) / chunked))
        for df2 in dfs[:-1]:
            assert chunked == len(df2)
        assert chunked >= len(dfs[-1])
    else:
        assert len(dfs) == len(set(col2))

    dfs = list(wr.athena.read_sql_table(database=database, table=table, chunksize=chunked))
    assert sum(values) == pd.concat(dfs, ignore_index=True).col1.sum()
    if chunked is not True:
        assert len(dfs) == int(math.ceil(len(df) / chunked))
        for df2 in dfs[:-1]:
            assert chunked == len(df2)
        assert chunked >= len(dfs[-1])

    wr.s3.delete_objects(path=paths)
    assert wr.catalog.delete_table_if_exists(database=database, table=table) is True


@pytest.mark.parametrize("workgroup", [None, 0, 1, 2, 3])
@pytest.mark.parametrize("encryption", [None, "SSE_S3", "SSE_KMS"])
# @pytest.mark.parametrize("workgroup", [3])
# @pytest.mark.parametrize("encryption", [None])
def test_athena_encryption(
    path, path2, database, table, table2, kms_key, encryption, workgroup, workgroup0, workgroup1, workgroup2, workgroup3
):
    kms_key = None if (encryption == "SSE_S3") or (encryption is None) else kms_key
    if workgroup == 0:
        workgroup = workgroup0
    elif workgroup == 1:
        workgroup = workgroup1
    elif workgroup == 2:
        workgroup = workgroup2
    elif workgroup == 3:
        workgroup = workgroup3
    df = pd.DataFrame({"a": [1, 2], "b": ["foo", "boo"]})
    paths = wr.s3.to_parquet(
        df=df, path=path, dataset=True, mode="overwrite", database=database, table=table, s3_additional_kwargs=None
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df2 = wr.athena.read_sql_table(
        table=table,
        ctas_approach=True,
        database=database,
        encryption=encryption,
        workgroup=workgroup,
        kms_key=kms_key,
        keep_files=True,
        ctas_temp_table_name=table2,
        s3_output=path2,
    )
    assert wr.catalog.does_table_exist(database=database, table=table2) is False
    assert len(df2.index) == 2
    assert len(df2.columns) == 2


def test_athena_nested(path, database, table):
    df = pd.DataFrame(
        {
            "c0": [[1, 2, 3], [4, 5, 6]],
            "c1": [[[1, 2], [3, 4]], [[5, 6], [7, 8]]],
            "c2": [[["a", "b"], ["c", "d"]], [["e", "f"], ["g", "h"]]],
            "c3": [[], [[[[[[[[1]]]]]]]]],
            "c4": [{"a": 1}, {"a": 1}],
            "c5": [{"a": {"b": {"c": [1, 2]}}}, {"a": {"b": {"c": [3, 4]}}}],
        }
    )
    paths = wr.s3.to_parquet(
        df=df, path=path, index=False, use_threads=True, dataset=True, mode="overwrite", database=database, table=table
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    df2 = wr.athena.read_sql_query(sql=f"SELECT c0, c1, c2, c4 FROM {table}", database=database)
    assert len(df2.index) == 2
    assert len(df2.columns) == 4


def test_catalog_versioning(bucket, database):
    table = "test_catalog_versioning"
    wr.catalog.delete_table_if_exists(database=database, table=table)
    path = f"s3://{bucket}/{table}/"
    wr.s3.delete_objects(path=path)

    # Version 0
    df = pd.DataFrame({"c0": [1, 2]})
    paths = wr.s3.to_parquet(df=df, path=path, dataset=True, database=database, table=table, mode="overwrite")["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df = wr.athena.read_sql_table(table=table, database=database)
    assert len(df.index) == 2
    assert len(df.columns) == 1
    assert str(df.c0.dtype).startswith("Int")

    # Version 1
    df = pd.DataFrame({"c1": ["foo", "boo"]})
    paths1 = wr.s3.to_parquet(
        df=df, path=path, dataset=True, database=database, table=table, mode="overwrite", catalog_versioning=True
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths1, use_threads=False)
    df = wr.athena.read_sql_table(table=table, database=database)
    assert len(df.index) == 2
    assert len(df.columns) == 1
    assert str(df.c1.dtype) == "string"

    # Version 2
    df = pd.DataFrame({"c1": [1.0, 2.0]})
    paths2 = wr.s3.to_csv(
        df=df,
        path=path,
        dataset=True,
        database=database,
        table=table,
        mode="overwrite",
        catalog_versioning=True,
        index=False,
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths2, use_threads=False)
    wr.s3.wait_objects_not_exist(paths=paths1, use_threads=False)
    df = wr.athena.read_sql_table(table=table, database=database)
    assert len(df.index) == 2
    assert len(df.columns) == 1
    assert str(df.c1.dtype).startswith("float")

    # Version 3 (removing version 2)
    df = pd.DataFrame({"c1": [True, False]})
    paths3 = wr.s3.to_csv(
        df=df,
        path=path,
        dataset=True,
        database=database,
        table=table,
        mode="overwrite",
        catalog_versioning=False,
        index=False,
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths3, use_threads=False)
    wr.s3.wait_objects_not_exist(paths=paths2, use_threads=False)
    df = wr.athena.read_sql_table(table=table, database=database)
    assert len(df.index) == 2
    assert len(df.columns) == 1
    assert str(df.c1.dtype).startswith("boolean")

    # Cleaning Up
    wr.catalog.delete_table_if_exists(database=database, table=table)
    wr.s3.delete_objects(path=path)


def test_unsigned_parquet(bucket, database):
    table = "test_unsigned_parquet"
    path = f"s3://{bucket}/{table}/"
    wr.s3.delete_objects(path=path)
    df = pd.DataFrame({"c0": [0, 0, (2 ** 8) - 1], "c1": [0, 0, (2 ** 16) - 1], "c2": [0, 0, (2 ** 32) - 1]})
    df["c0"] = df.c0.astype("uint8")
    df["c1"] = df.c1.astype("uint16")
    df["c2"] = df.c2.astype("uint32")
    paths = wr.s3.to_parquet(df=df, path=path, dataset=True, database=database, table=table, mode="overwrite")["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df = wr.athena.read_sql_table(table=table, database=database)
    assert df.c0.sum() == (2 ** 8) - 1
    assert df.c1.sum() == (2 ** 16) - 1
    assert df.c2.sum() == (2 ** 32) - 1
    schema = wr.s3.read_parquet_metadata(path=path)[0]
    assert schema["c0"] == "smallint"
    assert schema["c1"] == "int"
    assert schema["c2"] == "bigint"
    df = wr.s3.read_parquet(path=path)
    assert df.c0.sum() == (2 ** 8) - 1
    assert df.c1.sum() == (2 ** 16) - 1
    assert df.c2.sum() == (2 ** 32) - 1

    df = pd.DataFrame({"c0": [0, 0, (2 ** 64) - 1]})
    df["c0"] = df.c0.astype("uint64")
    with pytest.raises(wr.exceptions.UnsupportedType):
        wr.s3.to_parquet(df=df, path=path, dataset=True, database=database, table=table, mode="overwrite")

    wr.s3.delete_objects(path=path)
    wr.catalog.delete_table_if_exists(database=database, table=table)


def test_parquet_overwrite_partition_cols(path, database, table):
    df = pd.DataFrame({"c0": [1, 2, 1, 2], "c1": [1, 2, 1, 2], "c2": [2, 1, 2, 1]})

    paths = wr.s3.to_parquet(
        df=df, path=path, dataset=True, database=database, table=table, mode="overwrite", partition_cols=["c2"]
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df = wr.athena.read_sql_table(table=table, database=database)
    assert len(df.index) == 4
    assert len(df.columns) == 3
    assert df.c0.sum() == 6
    assert df.c1.sum() == 6
    assert df.c2.sum() == 6

    paths = wr.s3.to_parquet(
        df=df, path=path, dataset=True, database=database, table=table, mode="overwrite", partition_cols=["c1", "c2"]
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df = wr.athena.read_sql_table(table=table, database=database)
    assert len(df.index) == 4
    assert len(df.columns) == 3
    assert df.c0.sum() == 6
    assert df.c1.sum() == 6
    assert df.c2.sum() == 6


def test_catalog_parameters(bucket, database):
    table = "test_catalog_parameters"
    path = f"s3://{bucket}/{table}/"
    wr.s3.delete_objects(path=path)
    wr.catalog.delete_table_if_exists(database=database, table=table)

    wr.s3.to_parquet(
        df=pd.DataFrame({"c0": [1, 2]}),
        path=path,
        dataset=True,
        database=database,
        table=table,
        mode="overwrite",
        parameters={"a": "1", "b": "2"},
    )
    pars = wr.catalog.get_table_parameters(database=database, table=table)
    assert pars["a"] == "1"
    assert pars["b"] == "2"
    pars["a"] = "0"
    pars["c"] = "3"
    wr.catalog.upsert_table_parameters(parameters=pars, database=database, table=table)
    pars = wr.catalog.get_table_parameters(database=database, table=table)
    assert pars["a"] == "0"
    assert pars["b"] == "2"
    assert pars["c"] == "3"
    wr.catalog.overwrite_table_parameters(parameters={"d": "4"}, database=database, table=table)
    pars = wr.catalog.get_table_parameters(database=database, table=table)
    assert pars.get("a") is None
    assert pars.get("b") is None
    assert pars.get("c") is None
    assert pars["d"] == "4"
    df = wr.athena.read_sql_table(table=table, database=database)
    assert len(df.index) == 2
    assert len(df.columns) == 1
    assert df.c0.sum() == 3

    wr.s3.to_parquet(
        df=pd.DataFrame({"c0": [3, 4]}),
        path=path,
        dataset=True,
        database=database,
        table=table,
        mode="append",
        parameters={"e": "5"},
    )
    pars = wr.catalog.get_table_parameters(database=database, table=table)
    assert pars.get("a") is None
    assert pars.get("b") is None
    assert pars.get("c") is None
    assert pars["d"] == "4"
    assert pars["e"] == "5"
    df = wr.athena.read_sql_table(table=table, database=database)
    assert len(df.index) == 4
    assert len(df.columns) == 1
    assert df.c0.sum() == 10

    wr.s3.delete_objects(path=path)
    wr.catalog.delete_table_if_exists(database=database, table=table)


def test_athena_cache(path, database, table, workgroup1):
    df = pd.DataFrame({"c0": [0, None]}, dtype="Int64")
    paths = wr.s3.to_parquet(df=df, path=path, dataset=True, mode="overwrite", database=database, table=table)["paths"]
    wr.s3.wait_objects_exist(paths=paths)

    df2 = wr.athena.read_sql_table(table, database, ctas_approach=False, max_cache_seconds=1, workgroup=workgroup1)
    assert df.shape == df2.shape
    assert df.c0.sum() == df2.c0.sum()

    df2 = wr.athena.read_sql_table(table, database, ctas_approach=False, max_cache_seconds=900, workgroup=workgroup1)
    assert df.shape == df2.shape
    assert df.c0.sum() == df2.c0.sum()

    dfs = wr.athena.read_sql_table(
        table, database, ctas_approach=False, max_cache_seconds=900, workgroup=workgroup1, chunksize=1
    )
    assert len(list(dfs)) == 2


def test_cache_query_ctas_approach_true(path, database, table):
    df = pd.DataFrame({"c0": [0, None]}, dtype="Int64")
    paths = wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode="overwrite",
        database=database,
        table=table,
        description="c0",
        parameters={"num_cols": str(len(df.columns)), "num_rows": str(len(df.index))},
        columns_comments={"c0": "0"},
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths)

    with patch(
        "awswrangler.athena._check_for_cached_results", return_value={"has_valid_cache": False}
    ) as mocked_cache_attempt:
        df2 = wr.athena.read_sql_table(table, database, ctas_approach=True, max_cache_seconds=0)
        mocked_cache_attempt.assert_called()
        assert df.shape == df2.shape
        assert df.c0.sum() == df2.c0.sum()

    with patch("awswrangler.athena._resolve_query_without_cache") as resolve_no_cache:
        df3 = wr.athena.read_sql_table(table, database, ctas_approach=True, max_cache_seconds=900)
        resolve_no_cache.assert_not_called()
        assert df.shape == df3.shape
        assert df.c0.sum() == df3.c0.sum()


def test_cache_query_ctas_approach_false(path, database, table):
    df = pd.DataFrame({"c0": [0, None]}, dtype="Int64")
    paths = wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode="overwrite",
        database=database,
        table=table,
        description="c0",
        parameters={"num_cols": str(len(df.columns)), "num_rows": str(len(df.index))},
        columns_comments={"c0": "0"},
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths)

    with patch(
        "awswrangler.athena._check_for_cached_results", return_value={"has_valid_cache": False}
    ) as mocked_cache_attempt:
        df2 = wr.athena.read_sql_table(table, database, ctas_approach=False, max_cache_seconds=0)
        mocked_cache_attempt.assert_called()
        assert df.shape == df2.shape
        assert df.c0.sum() == df2.c0.sum()

    with patch("awswrangler.athena._resolve_query_without_cache") as resolve_no_cache:
        df3 = wr.athena.read_sql_table(table, database, ctas_approach=False, max_cache_seconds=900)
        resolve_no_cache.assert_not_called()
        assert df.shape == df3.shape
        assert df.c0.sum() == df3.c0.sum()


def test_cache_query_semicolon(path, database, table):
    df = pd.DataFrame({"c0": [0, None]}, dtype="Int64")
    paths = wr.s3.to_parquet(df=df, path=path, dataset=True, mode="overwrite", database=database, table=table)["paths"]
    wr.s3.wait_objects_exist(paths=paths)

    with patch(
        "awswrangler.athena._check_for_cached_results", return_value={"has_valid_cache": False}
    ) as mocked_cache_attempt:
        df2 = wr.athena.read_sql_query(
            f"SELECT * FROM {table}", database=database, ctas_approach=True, max_cache_seconds=0
        )
        mocked_cache_attempt.assert_called()
        assert df.shape == df2.shape
        assert df.c0.sum() == df2.c0.sum()

    with patch("awswrangler.athena._resolve_query_without_cache") as resolve_no_cache:
        df3 = wr.athena.read_sql_query(
            f"SELECT * FROM {table};", database=database, ctas_approach=True, max_cache_seconds=900
        )
        resolve_no_cache.assert_not_called()
        assert df.shape == df3.shape
        assert df.c0.sum() == df3.c0.sum()


@pytest.mark.parametrize("partition_cols", [None, ["c2"], ["c1", "c2"]])
def test_store_metadata_partitions_dataset(database, table, path, partition_cols):
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [3, 4, 5], "c2": [6, 7, 8]})
    paths = wr.s3.to_parquet(df=df, path=path, dataset=True, partition_cols=partition_cols)["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    wr.s3.store_parquet_metadata(path=path, database=database, table=table, dataset=True)
    df2 = wr.athena.read_sql_table(table=table, database=database)
    assert len(df.index) == len(df2.index)
    assert len(df.columns) == len(df2.columns)
    assert df.c0.sum() == df2.c0.sum()
    assert df.c1.sum() == df2.c1.astype(int).sum()
    assert df.c2.sum() == df2.c2.astype(int).sum()


@pytest.mark.parametrize("partition_cols", [None, ["c2"], ["c1", "c2"]])
def test_store_metadata_partitions_sample_dataset(database, table, path, partition_cols):
    num_files = 10
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [3, 4, 5], "c2": [6, 7, 8]})
    for _ in range(num_files):
        paths = wr.s3.to_parquet(df=df, path=path, dataset=True, partition_cols=partition_cols)["paths"]
        wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    wr.s3.store_parquet_metadata(
        path=path, database=database, table=table, dtype={"c1": "bigint", "c2": "smallint"}, sampling=0.25, dataset=True
    )
    df2 = wr.athena.read_sql_table(table=table, database=database)
    assert len(df.index) * num_files == len(df2.index)
    assert len(df.columns) == len(df2.columns)
    assert df.c0.sum() * num_files == df2.c0.sum()
    assert df.c1.sum() * num_files == df2.c1.sum()
    assert df.c2.sum() * num_files == df2.c2.sum()


def test_athena_undefined_column(database):
    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.athena.read_sql_query("SELECT 1", database)
    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.athena.read_sql_query("SELECT NULL AS my_null", database)


@pytest.mark.parametrize("partition_cols", [None, ["c1"], ["c2"], ["c1", "c2"], ["c2", "c1"]])
def test_to_parquet_reverse_partitions(database, table, path, partition_cols):
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [3, 4, 5], "c2": [6, 7, 8]})
    paths = wr.s3.to_parquet(
        df=df, path=path, dataset=True, database=database, table=table, partition_cols=partition_cols
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df2 = wr.athena.read_sql_table(table=table, database=database)
    assert df.shape == df2.shape
    assert df.c0.sum() == df2.c0.sum()
    assert df.c1.sum() == df2.c1.sum()
    assert df.c2.sum() == df2.c2.sum()


def test_to_parquet_nested_append(database, table, path):
    df = pd.DataFrame(
        {
            "c0": [[1, 2, 3], [4, 5, 6]],
            "c1": [[[1, 2], [3, 4]], [[5, 6], [7, 8]]],
            "c2": [[["a", "b"], ["c", "d"]], [["e", "f"], ["g", "h"]]],
            "c3": [[], [[[[[[[[1]]]]]]]]],
            "c4": [{"a": 1}, {"a": 1}],
            "c5": [{"a": {"b": {"c": [1, 2]}}}, {"a": {"b": {"c": [3, 4]}}}],
        }
    )
    paths = wr.s3.to_parquet(df=df, path=path, dataset=True, database=database, table=table)["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df2 = wr.athena.read_sql_query(sql=f"SELECT c0, c1, c2, c4 FROM {table}", database=database)
    assert len(df2.index) == 2
    assert len(df2.columns) == 4
    paths = wr.s3.to_parquet(df=df, path=path, dataset=True, database=database, table=table)["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df2 = wr.athena.read_sql_query(sql=f"SELECT c0, c1, c2, c4 FROM {table}", database=database)
    assert len(df2.index) == 4
    assert len(df2.columns) == 4


def test_to_parquet_nested_cast(database, table, path):
    df = pd.DataFrame({"c0": [[1, 2, 3], [4, 5, 6]], "c1": [[], []], "c2": [{"a": 1, "b": 2}, {"a": 3, "b": 4}]})
    paths = wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        database=database,
        table=table,
        dtype={"c0": "array<double>", "c1": "array<string>", "c2": "struct<a:bigint, b:double>"},
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df = pd.DataFrame({"c0": [[1, 2, 3], [4, 5, 6]], "c1": [["a"], ["b"]], "c2": [{"a": 1, "b": 2}, {"a": 3, "b": 4}]})
    paths = wr.s3.to_parquet(df=df, path=path, dataset=True, database=database, table=table)["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df2 = wr.athena.read_sql_query(sql=f"SELECT c0, c2 FROM {table}", database=database)
    assert len(df2.index) == 4
    assert len(df2.columns) == 2


def test_to_parquet_projection_integer(database, table, path):
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [0, 1, 2], "c2": [0, 100, 200], "c3": [0, 1, 2]})
    paths = wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        database=database,
        table=table,
        partition_cols=["c1", "c2", "c3"],
        regular_partitions=False,
        projection_enabled=True,
        projection_types={"c1": "integer", "c2": "integer", "c3": "integer"},
        projection_ranges={"c1": "0,2", "c2": "0,200", "c3": "0,2"},
        projection_intervals={"c2": "100"},
        projection_digits={"c3": "1"},
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df2 = wr.athena.read_sql_table(table, database)
    assert df.shape == df2.shape
    assert df.c0.sum() == df2.c0.sum()
    assert df.c1.sum() == df2.c1.sum()
    assert df.c2.sum() == df2.c2.sum()
    assert df.c3.sum() == df2.c3.sum()


def test_to_parquet_projection_enum(database, table, path):
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [1, 2, 3], "c2": ["foo", "boo", "bar"]})
    paths = wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        database=database,
        table=table,
        partition_cols=["c1", "c2"],
        regular_partitions=False,
        projection_enabled=True,
        projection_types={"c1": "enum", "c2": "enum"},
        projection_values={"c1": "1,2,3", "c2": "foo,boo,bar"},
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df2 = wr.athena.read_sql_table(table, database)
    assert df.shape == df2.shape
    assert df.c0.sum() == df2.c0.sum()
    assert df.c1.sum() == df2.c1.sum()


def test_to_parquet_projection_date(database, table, path):
    df = pd.DataFrame(
        {
            "c0": [0, 1, 2],
            "c1": [dt("2020-01-01"), dt("2020-01-02"), dt("2020-01-03")],
            "c2": [ts("2020-01-01 01:01:01.0"), ts("2020-01-01 01:01:02.0"), ts("2020-01-01 01:01:03.0")],
        }
    )
    paths = wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        database=database,
        table=table,
        partition_cols=["c1", "c2"],
        regular_partitions=False,
        projection_enabled=True,
        projection_types={"c1": "date", "c2": "date"},
        projection_ranges={"c1": "2020-01-01,2020-01-03", "c2": "2020-01-01 01:01:00,2020-01-01 01:01:03"},
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df2 = wr.athena.read_sql_table(table, database)
    print(df2)
    assert df.shape == df2.shape
    assert df.c0.sum() == df2.c0.sum()


def test_to_parquet_projection_injected(database, table, path):
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": ["foo", "boo", "bar"], "c2": ["0", "1", "2"]})
    paths = wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        database=database,
        table=table,
        partition_cols=["c1", "c2"],
        regular_partitions=False,
        projection_enabled=True,
        projection_types={"c1": "injected", "c2": "injected"},
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df2 = wr.athena.read_sql_query(f"SELECT * FROM {table} WHERE c1='foo' AND c2='0'", database)
    assert df2.shape == (1, 3)
    assert df2.c0.iloc[0] == 0


def test_glue_database():

    # Round 1 - Create Database
    database_name = f"database_{get_time_str_with_random_suffix()}"
    print(f"Database Name: {database_name}")
    wr.catalog.create_database(name=database_name, description="Database Description")
    databases = wr.catalog.get_databases()
    test_database_name = ""
    test_database_description = ""

    for database in databases:
        if database["Name"] == database_name:
            test_database_name = database["Name"]
            test_database_description = database["Description"]

    assert test_database_name == database_name
    assert test_database_description == "Database Description"

    # Round 2 - Delete Database
    print(f"Database Name: {database_name}")
    wr.catalog.delete_database(name=database_name)
    databases = wr.catalog.get_databases()
    test_database_name = ""
    test_database_description = ""

    for database in databases:
        if database["Name"] == database_name:
            test_database_name = database["Name"]
            test_database_description = database["Description"]

    assert test_database_name == ""
    assert test_database_description == ""


def test_parquet_catalog_casting_to_string(path, table, database):
    for df in [get_df(), get_df_cast()]:
        paths = wr.s3.to_parquet(
            df=df,
            path=path,
            index=False,
            dataset=True,
            mode="overwrite",
            database=database,
            table=table,
            dtype={
                "iint8": "string",
                "iint16": "string",
                "iint32": "string",
                "iint64": "string",
                "float": "string",
                "double": "string",
                "decimal": "string",
                "string": "string",
                "date": "string",
                "timestamp": "string",
                "timestamp2": "string",
                "bool": "string",
                "binary": "string",
                "category": "string",
                "par0": "string",
                "par1": "string",
            },
        )["paths"]
        wr.s3.wait_objects_exist(paths=paths)
        df = wr.s3.read_parquet(path=path)
        assert df.shape == (3, 16)
        for dtype in df.dtypes.values:
            assert str(dtype) == "string"
        df = wr.athena.read_sql_table(table=table, database=database, ctas_approach=True)
        assert df.shape == (3, 16)
        for dtype in df.dtypes.values:
            assert str(dtype) == "string"
        df = wr.athena.read_sql_table(table=table, database=database, ctas_approach=False)
        assert df.shape == (3, 16)
        for dtype in df.dtypes.values:
            assert str(dtype) == "string"
