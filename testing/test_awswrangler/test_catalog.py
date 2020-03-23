import logging

import boto3
import pytest

import awswrangler as wr

logging.basicConfig(level=logging.INFO, format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)
logging.getLogger("botocore.credentials").setLevel(logging.CRITICAL)


@pytest.fixture(scope="module")
def cloudformation_outputs():
    response = boto3.client("cloudformation").describe_stacks(StackName="aws-data-wrangler-test")
    outputs = {}
    for output in response.get("Stacks")[0].get("Outputs"):
        outputs[output.get("OutputKey")] = output.get("OutputValue")
    yield outputs


@pytest.fixture(scope="module")
def bucket(cloudformation_outputs):
    if "BucketName" in cloudformation_outputs:
        bucket = cloudformation_outputs["BucketName"]
        wr.s3.delete_objects(f"s3://{bucket}/")
    else:
        raise Exception("You must deploy/update the test infrastructure (CloudFormation)")
    yield bucket
    wr.s3.delete_objects(f"s3://{bucket}/")


@pytest.fixture(scope="module")
def database(cloudformation_outputs):
    if "GlueDatabaseName" in cloudformation_outputs:
        database = cloudformation_outputs["GlueDatabaseName"]
    else:
        raise Exception("You must deploy the test infrastructure using Cloudformation!")
    yield database
    tables = wr.catalog.tables(database=database)["Table"].tolist()
    for t in tables:
        print(f"Dropping: {database}.{t}...")
        wr.catalog.delete_table_if_exists(database=database, table=t)


def test_get_databases(database):
    dbs = list(wr.catalog.get_databases())
    assert len(dbs) > 0
    for db in dbs:
        if db["Name"] == database:
            assert db["Description"] == "AWS Data Wrangler Test Arena - Glue Database"


def test_catalog(bucket, database):
    account_id = boto3.client("sts").get_caller_identity().get("Account")
    path = f"s3://{bucket}/test_catalog/"
    assert wr.catalog.delete_table_if_exists(database=database, table="test_catalog") is False
    assert wr.catalog.does_table_exist(database=database, table="test_catalog") is False
    wr.catalog.create_parquet_table(
        database=database,
        table="test_catalog",
        path=path,
        columns_types={"col0": "int", "col1": "double"},
        partitions_types={"y": "int", "m": "int"},
        compression="snappy",
    )
    assert wr.catalog.does_table_exist(database=database, table="test_catalog") is True
    assert wr.catalog.delete_table_if_exists(database=database, table="test_catalog") is True
    assert wr.catalog.delete_table_if_exists(database=database, table="test_catalog") is False
    wr.catalog.create_parquet_table(
        database=database,
        table="test_catalog",
        path=path,
        columns_types={"col0": "int", "col1": "double"},
        partitions_types={"y": "int", "m": "int"},
        compression="snappy",
        description="Foo boo bar",
        parameters={"tag": "test"},
        columns_comments={"col0": "my int", "y": "year"},
    )
    wr.catalog.add_parquet_partitions(
        database=database,
        table="test_catalog",
        partitions_values={f"{path}y=2020/m=1/": ["2020", "1"], f"{path}y=2021/m=2/": ["2021", "2"]},
        compression="snappy",
    )
    assert wr.catalog.get_table_location(database=database, table="test_catalog") == path
    partitions_values = wr.catalog.get_parquet_partitions(database=database, table="test_catalog")
    assert len(partitions_values) == 2
    partitions_values = wr.catalog.get_parquet_partitions(
        database=database, table="test_catalog", catalog_id=account_id, expression="y = 2021 AND m = 2"
    )
    assert len(partitions_values) == 1
    assert len(set(partitions_values[f"{path}y=2021/m=2/"]) & set(["2021", "2"])) == 2
    dtypes = wr.catalog.get_table_types(database=database, table="test_catalog")
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
        if tbl["Name"] == "test_catalog":
            assert tbl["TableType"] == "EXTERNAL_TABLE"
    tables = list(wr.catalog.get_tables(database=database))
    assert len(tables) > 0
    for tbl in tables:
        assert tbl["DatabaseName"] == database
    # search
    tables = list(wr.catalog.search_tables(text="parquet", catalog_id=account_id))
    assert len(tables) > 0
    for tbl in tables:
        if tbl["Name"] == "test_catalog":
            assert tbl["TableType"] == "EXTERNAL_TABLE"
    # prefix
    tables = list(wr.catalog.get_tables(name_prefix="test_cat", catalog_id=account_id))
    assert len(tables) > 0
    for tbl in tables:
        if tbl["Name"] == "test_catalog":
            assert tbl["TableType"] == "EXTERNAL_TABLE"
    # suffix
    tables = list(wr.catalog.get_tables(name_suffix="_catalog", catalog_id=account_id))
    assert len(tables) > 0
    for tbl in tables:
        if tbl["Name"] == "test_catalog":
            assert tbl["TableType"] == "EXTERNAL_TABLE"
    # name_contains
    tables = list(wr.catalog.get_tables(name_contains="cat", catalog_id=account_id))
    assert len(tables) > 0
    for tbl in tables:
        if tbl["Name"] == "test_catalog":
            assert tbl["TableType"] == "EXTERNAL_TABLE"
    # prefix & suffix & name_contains
    tables = list(wr.catalog.get_tables(name_prefix="t", name_contains="_", name_suffix="g", catalog_id=account_id))
    assert len(tables) > 0
    for tbl in tables:
        if tbl["Name"] == "test_catalog":
            assert tbl["TableType"] == "EXTERNAL_TABLE"
    # prefix & suffix
    tables = list(wr.catalog.get_tables(name_prefix="t", name_suffix="g", catalog_id=account_id))
    assert len(tables) > 0
    for tbl in tables:
        if tbl["Name"] == "test_catalog":
            assert tbl["TableType"] == "EXTERNAL_TABLE"
    # DataFrames
    assert len(wr.catalog.databases().index) > 0
    assert len(wr.catalog.tables().index) > 0
    assert (
        len(
            wr.catalog.tables(
                database=database,
                search_text="parquet",
                name_prefix="t",
                name_contains="_",
                name_suffix="g",
                catalog_id=account_id,
            ).index
        )
        > 0
    )
    assert len(wr.catalog.table(database=database, table="test_catalog").index) > 0
    assert len(wr.catalog.table(database=database, table="test_catalog", catalog_id=account_id).index) > 0
