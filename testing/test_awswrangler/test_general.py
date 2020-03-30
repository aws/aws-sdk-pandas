import logging

import boto3
import pandas as pd
import pytest

import awswrangler as wr

from ._utils import ensure_data_types, get_df, get_df_list, get_query_long

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
def region(cloudformation_outputs):
    if "Region" in cloudformation_outputs:
        region = cloudformation_outputs["Region"]
    else:
        raise Exception("You must deploy/update the test infrastructure (CloudFormation)!")
    yield region


@pytest.fixture(scope="module")
def bucket(cloudformation_outputs):
    if "BucketName" in cloudformation_outputs:
        bucket = cloudformation_outputs["BucketName"]
    else:
        raise Exception("You must deploy/update the test infrastructure (CloudFormation)")
    yield bucket


@pytest.fixture(scope="module")
def database(cloudformation_outputs):
    if "GlueDatabaseName" in cloudformation_outputs:
        database = cloudformation_outputs["GlueDatabaseName"]
    else:
        raise Exception("You must deploy the test infrastructure using Cloudformation!")
    yield database


@pytest.fixture(scope="module")
def kms_key(cloudformation_outputs):
    if "KmsKeyArn" in cloudformation_outputs:
        key = cloudformation_outputs["KmsKeyArn"]
    else:
        raise Exception("You must deploy the test infrastructure using Cloudformation!")
    yield key


def test_athena_ctas(bucket, database, kms_key):
    paths = wr.s3.to_parquet(
        df=get_df_list(),
        path=f"s3://{bucket}/test_athena_ctas",
        index=True,
        use_threads=True,
        dataset=True,
        mode="overwrite",
        database=database,
        table="test_athena_ctas",
        partition_cols=["par0", "par1"],
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    df = wr.athena.read_sql_query(
        sql=f"SELECT * FROM test_athena_ctas",
        database=database,
        ctas_approach=True,
        encryption="SSE_KMS",
        kms_key=kms_key,
    )
    assert len(df.index) == 3
    ensure_data_types(df=df, has_list=True)
    dfs = wr.athena.read_sql_query(
        sql=f"SELECT * FROM test_athena_ctas", database=database, ctas_approach=True, chunksize=1
    )
    for df in dfs:
        ensure_data_types(df=df, has_list=True)
    wr.catalog.delete_table_if_exists(database=database, table="test_athena_ctas")
    wr.s3.delete_objects(path=paths)
    wr.s3.wait_objects_not_exist(paths=paths)


def test_athena(bucket, database, kms_key):
    wr.s3.delete_objects(path=f"s3://{bucket}/test_athena")
    paths = wr.s3.to_parquet(
        df=get_df(),
        path=f"s3://{bucket}/test_athena",
        index=True,
        use_threads=True,
        dataset=True,
        mode="overwrite",
        database=database,
        table="test_athena",
        partition_cols=["par0", "par1"],
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    dfs = wr.athena.read_sql_query(
        sql="SELECT * FROM test_athena",
        database=database,
        ctas_approach=False,
        chunksize=1,
        encryption="SSE_KMS",
        kms_key=kms_key,
    )
    for df2 in dfs:
        print(df2)
        ensure_data_types(df=df2)
    df = wr.athena.read_sql_query(sql="SELECT * FROM test_athena", database=database, ctas_approach=False)
    assert len(df.index) == 3
    ensure_data_types(df=df)
    wr.athena.repair_table(table="test_athena", database=database)
    wr.catalog.delete_table_if_exists(database=database, table="test_athena")
    wr.s3.delete_objects(path=paths)
    wr.s3.wait_objects_not_exist(paths=paths)


def test_csv(bucket):
    session = boto3.Session()
    df = pd.DataFrame({"id": [1, 2, 3]})
    path0 = f"s3://{bucket}/test_csv0.csv"
    path1 = f"s3://{bucket}/test_csv1.csv"
    path2 = f"s3://{bucket}/test_csv2.csv"
    wr.s3.to_csv(df=df, path=path0, index=False)
    wr.s3.to_csv(df=df, path=path1, index=False, boto3_session=None)
    wr.s3.to_csv(df=df, path=path2, index=False, boto3_session=session)
    assert df.equals(wr.s3.read_csv(path=path0, use_threads=False))
    assert df.equals(wr.s3.read_csv(path=path0, use_threads=True))
    assert df.equals(wr.s3.read_csv(path=path0, use_threads=False, boto3_session=session))
    assert df.equals(wr.s3.read_csv(path=path0, use_threads=True, boto3_session=session))
    paths = [path0, path1, path2]
    df2 = pd.concat(objs=[df, df, df], sort=False, ignore_index=True)
    assert df2.equals(wr.s3.read_csv(path=paths, use_threads=False))
    assert df2.equals(wr.s3.read_csv(path=paths, use_threads=True))
    assert df2.equals(wr.s3.read_csv(path=paths, use_threads=False, boto3_session=session))
    assert df2.equals(wr.s3.read_csv(path=paths, use_threads=True, boto3_session=session))
    with pytest.raises(wr.exceptions.InvalidArgumentType):
        wr.s3.read_csv(path=1)
    wr.s3.delete_objects(path=[path0, path1, path2], use_threads=False)
    wr.s3.wait_objects_not_exist(paths=[path0, path1, path2], use_threads=False)


def test_metadata():
    assert wr.__version__ == "1.0.0"
    assert wr.__title__ == "awswrangler"
    assert wr.__description__ == "Pandas on AWS."
    assert wr.__license__ == "Apache License 2.0"


def test_s3_get_bucket_region(bucket, region):
    assert wr.s3.get_bucket_region(bucket=bucket) == region
    assert wr.s3.get_bucket_region(bucket=bucket, boto3_session=boto3.Session()) == region


def test_catalog_get_databases(database):
    dbs = list(wr.catalog.get_databases())
    assert len(dbs) > 0
    for db in dbs:
        if db["Name"] == database:
            assert db["Description"] == "AWS Data Wrangler Test Arena - Glue Database"


def test_athena_query_cancelled(database):
    session = boto3.Session()
    query_execution_id = wr.athena.start_query_execution(sql=get_query_long(), database=database, boto3_session=session)
    wr.athena.stop_query_execution(query_execution_id=query_execution_id, boto3_session=session)
    with pytest.raises(wr.exceptions.QueryCancelled):
        assert wr.athena.wait_query(query_execution_id=query_execution_id)


def test_athena_query_failed(database):
    query_execution_id = wr.athena.start_query_execution(sql="SELECT random(-1)", database=database)
    with pytest.raises(wr.exceptions.QueryFailed):
        assert wr.athena.wait_query(query_execution_id=query_execution_id)
