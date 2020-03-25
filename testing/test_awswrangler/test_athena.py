import logging

import boto3
import pytest

import awswrangler as wr
from awswrangler import exceptions

from ._utils import get_athena_ctas_df, get_athena_df

logging.basicConfig(level=logging.INFO, format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)
logging.getLogger("botocore.credentials").setLevel(logging.CRITICAL)


def test_normalize_column_name():
    assert wr.athena.normalize_column_name("foo()__Boo))))____BAR") == "foo_boo_bar"
    assert wr.athena.normalize_column_name("foo()__Boo))))_{}{}{{}{}{}{___BAR[][][][]") == "foo_boo_bar"


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


@pytest.fixture(scope="module")
def workgroup_secondary(bucket):
    wkg_name = "awswrangler_test"
    client = boto3.client("athena")
    wkgs = client.list_work_groups()
    wkgs = [x["Name"] for x in wkgs["WorkGroups"]]
    if wkg_name not in wkgs:
        client.create_work_group(
            Name=wkg_name,
            Configuration={
                "ResultConfiguration": {
                    "OutputLocation": f"s3://{bucket}/athena_workgroup_secondary/",
                    "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
                },
                "EnforceWorkGroupConfiguration": True,
                "PublishCloudWatchMetricsEnabled": True,
                "BytesScannedCutoffPerQuery": 100_000_000,
                "RequesterPaysEnabled": False,
            },
            Description="AWS Data Wrangler Test WorkGroup",
        )
    yield wkg_name


@pytest.fixture(scope="module")
def kms_key(cloudformation_outputs):
    if "KmsKeyArn" in cloudformation_outputs:
        database = cloudformation_outputs["KmsKeyArn"]
    else:
        raise Exception("You must deploy the test infrastructure using Cloudformation!")
    yield database


@pytest.fixture(scope="module")
def table(bucket, database):
    df = get_athena_df()
    wr.s3.to_parquet(
        df=df,
        path=f"s3://{bucket}/test_athena",
        index=True,
        use_threads=True,
        dataset=True,
        mode="overwrite",
        database=database,
        table="test_athena",
        partition_cols=["par0", "par1"],
    )
    yield "test_athena"
    wr.catalog.delete_table_if_exists(database=database, table="test_athena")


@pytest.fixture(scope="module")
def table_ctas(bucket, database):
    df = get_athena_ctas_df()
    wr.s3.to_parquet(
        df=df,
        path=f"s3://{bucket}/test_athena_ctas",
        index=True,
        use_threads=True,
        dataset=True,
        mode="overwrite",
        database=database,
        table="test_athena_ctas",
        partition_cols=["par0", "par1"],
    )
    yield "test_athena_ctas"
    wr.catalog.delete_table_if_exists(database=database, table="test_athena")


def test_read(bucket, database, table, workgroup_secondary):
    df = wr.athena.read_sql_query(
        sql=f"SELECT * FROM {table}",
        database=database,
        ctas_approach=False,
        workgroup=workgroup_secondary,
        encryption="SSE_S3",
        s3_output=f"s3://{bucket}/athena_workgroup_secondary/",
    )

    assert len(df.index) == 3

    assert str(df["iint8"].dtype) == "Int8"
    assert str(df["iint16"].dtype) == "Int16"
    assert str(df["iint32"].dtype) == "Int32"
    assert str(df["iint64"].dtype) == "Int64"
    assert str(df["float"].dtype) == "float32"
    assert str(df["double"].dtype) == "float64"
    assert str(df["decimal"].dtype) == "object"
    assert str(df["string_object"].dtype) == "string"
    assert str(df["string"].dtype) == "string"
    assert str(df["date"].dtype) == "object"
    assert str(df["timestamp"].dtype) == "datetime64[ns]"
    assert str(df["bool"].dtype) == "boolean"
    assert str(df["binary"].dtype) == "object"
    assert str(df["category"].dtype) == "float64"
    assert str(df["__index_level_0__"].dtype) == "Int64"
    assert str(df["par0"].dtype) == "Int64"
    assert str(df["par1"].dtype) == "string"

    row = df[df["iint8"] == 1].iloc[0]
    assert str(type(row["decimal"]).__name__) == "Decimal"
    assert str(type(row["date"]).__name__) == "date"
    assert str(type(row["binary"]).__name__) == "bytes"


# def test_read_chunksize(database, table):
#     dfs = wr.athena.read_sql_query(
#         sql=f"SELECT * FROM {table}",
#         database=database,
#         ctas_approach=False,
#         chunksize=1
#     )
#
#     for df in dfs:
#         assert len(df.index) == 1
#
#         assert str(df["iint8"].dtype) == "Int8"
#         assert str(df["iint16"].dtype) == "Int16"
#         assert str(df["iint32"].dtype) == "Int32"
#         assert str(df["iint64"].dtype) == "Int64"
#         assert str(df["float"].dtype) == "float32"
#         assert str(df["double"].dtype) == "float64"
#         assert str(df["decimal"].dtype) == "object"
#         assert str(df["string_object"].dtype) == "string"
#         assert str(df["string"].dtype) == "string"
#         assert str(df["date"].dtype) == "object"
#         assert str(df["timestamp"].dtype) == "datetime64[ns]"
#         assert str(df["bool"].dtype) == "boolean"
#         assert str(df["binary"].dtype) == "object"
#         assert str(df["category"].dtype) == "float64"
#         assert str(df["__index_level_0__"].dtype) == "Int64"
#         assert str(df["par0"].dtype) == "Int64"
#         assert str(df["par1"].dtype) == "string"
#
#         if df["iint8"].iloc[0] == 1:
#             row = df[df["iint8"] == 1].iloc[0]
#             assert str(type(row["decimal"]).__name__) == "Decimal"
#             assert str(type(row["date"]).__name__) == "date"
#             assert str(type(row["binary"]).__name__) == "bytes"


def test_read_ctas(database, table_ctas, kms_key):
    wr.athena.repair_table(database=database, table=table_ctas)
    df = wr.athena.read_sql_query(
        sql=f"SELECT * FROM {table_ctas}", database=database, ctas_approach=True, encryption="SSE_KMS", kms_key=kms_key
    )

    assert len(df.index) == 3

    assert str(df["iint8"].dtype) == "Int32"
    assert str(df["iint16"].dtype) == "Int32"
    assert str(df["iint32"].dtype) == "Int32"
    assert str(df["iint64"].dtype) == "Int64"
    assert str(df["float"].dtype) == "float32"
    assert str(df["double"].dtype) == "float64"
    assert str(df["decimal"].dtype) == "object"
    assert str(df["string_object"].dtype) == "string"
    assert str(df["string"].dtype) == "string"
    assert str(df["date"].dtype) == "object"
    assert str(df["timestamp"].dtype) == "datetime64[ns]"
    assert str(df["bool"].dtype) == "boolean"
    assert str(df["binary"].dtype) == "object"
    assert str(df["category"].dtype) == "float64"
    assert str(df["list"].dtype) == "object"
    assert str(df["list_list"].dtype) == "object"
    assert str(df["__index_level_0__"].dtype) == "Int64"
    assert str(df["par0"].dtype) == "Int64"
    assert str(df["par1"].dtype) == "string"

    row = df[df["iint8"] == 1].iloc[0]
    assert str(type(row["decimal"]).__name__) == "Decimal"
    assert str(type(row["date"]).__name__) == "date"
    assert str(type(row["binary"]).__name__) == "bytes"
    assert str(type(row["list"][0]).__name__) == "int64"
    assert str(type(row["list_list"][0][0]).__name__) == "int64"


def test_read_list(database):
    with pytest.raises(exceptions.UnsupportedType):
        wr.athena.read_sql_query(sql=f"SELECT ARRAY[1, 2, 3]", database=database, ctas_approach=False)


def test_query_cancelled(database):
    client_athena = boto3.client("athena")
    query_execution_id = wr.athena.start_query_execution(
        sql="""
SELECT
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(),
rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand(), rand()
        """,
        database=database,
    )
    client_athena.stop_query_execution(QueryExecutionId=query_execution_id)
    with pytest.raises(exceptions.QueryCancelled):
        assert wr.athena.wait_query(query_execution_id=query_execution_id)


def test_query_failed(database):
    query_execution_id = wr.athena.start_query_execution(sql="SELECT random(-1)", database=database)
    with pytest.raises(exceptions.QueryFailed):
        assert wr.athena.wait_query(query_execution_id=query_execution_id)
