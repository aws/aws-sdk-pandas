import bz2
import datetime
import gzip
import logging
import lzma
import math
from io import BytesIO, TextIOWrapper

import boto3
import pandas as pd
import pytest

import awswrangler as wr

from ._utils import (ensure_data_types, ensure_data_types_category, ensure_data_types_csv, get_df, get_df_cast,
                     get_df_category, get_df_csv, get_df_list, get_query_long)

logging.basicConfig(level=logging.INFO, format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)
logging.getLogger("botocore.credentials").setLevel(logging.CRITICAL)


@pytest.fixture(scope="module")
def cloudformation_outputs():
    response = boto3.client("cloudformation").describe_stacks(StackName="aws-data-wrangler")
    outputs = {}
    for output in response.get("Stacks")[0].get("Outputs"):
        outputs[output.get("OutputKey")] = output.get("OutputValue")
    yield outputs


@pytest.fixture(scope="module")
def region(cloudformation_outputs):
    yield cloudformation_outputs["Region"]


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
def external_schema(cloudformation_outputs, database):
    region = cloudformation_outputs.get("Region")
    sql = f"""
    CREATE EXTERNAL SCHEMA IF NOT EXISTS aws_data_wrangler_external FROM data catalog
    DATABASE '{database}'
    IAM_ROLE '{cloudformation_outputs["RedshiftRole"]}'
    REGION '{region}';
    """
    engine = wr.catalog.get_engine(connection=f"aws-data-wrangler-redshift")
    with engine.connect() as con:
        con.execute(sql)
    yield "aws_data_wrangler_external"


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


def test_athena_ctas(bucket, database, kms_key):
    wr.s3.delete_objects(path=f"s3://{bucket}/test_athena_ctas/")
    wr.s3.delete_objects(path=f"s3://{bucket}/test_athena_ctas_result/")
    df = get_df_list()
    columns_types, partitions_types = wr.catalog.extract_athena_types(df=df, partition_cols=["par0", "par1"])
    assert len(columns_types) == 16
    assert len(partitions_types) == 2
    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.catalog.extract_athena_types(df=df, file_format="avro")
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
    dirs = wr.s3.list_directories(path=f"s3://{bucket}/test_athena_ctas/")
    for d in dirs:
        assert d.startswith(f"s3://{bucket}/test_athena_ctas/par0=")
    df = wr.s3.read_parquet_table(table="test_athena_ctas", database=database)
    assert len(df.index) == 3
    ensure_data_types(df=df, has_list=True)
    df = wr.athena.read_sql_table(
        table="test_athena_ctas",
        database=database,
        ctas_approach=True,
        encryption="SSE_KMS",
        kms_key=kms_key,
        s3_output=f"s3://{bucket}/test_athena_ctas_result",
        keep_files=False,
    )
    assert len(df.index) == 3
    ensure_data_types(df=df, has_list=True)
    temp_table = "test_athena_ctas2"
    s3_output = f"s3://{bucket}/s3_output/"
    final_destination = f"{s3_output}{temp_table}/"

    # keep_files=False
    wr.s3.delete_objects(path=s3_output)
    dfs = wr.athena.read_sql_query(
        sql=f"SELECT * FROM test_athena_ctas",
        database=database,
        ctas_approach=True,
        chunksize=1,
        keep_files=False,
        ctas_temp_table_name=temp_table,
        s3_output=s3_output,
    )
    assert wr.catalog.does_table_exist(database=database, table=temp_table) is False
    assert len(wr.s3.list_objects(path=s3_output)) > 2
    assert len(wr.s3.list_objects(path=final_destination)) > 0
    for df in dfs:
        ensure_data_types(df=df, has_list=True)
    assert len(wr.s3.list_objects(path=s3_output)) == 0

    # keep_files=True
    wr.s3.delete_objects(path=s3_output)
    dfs = wr.athena.read_sql_query(
        sql=f"SELECT * FROM test_athena_ctas",
        database=database,
        ctas_approach=True,
        chunksize=2,
        keep_files=True,
        ctas_temp_table_name=temp_table,
        s3_output=s3_output,
    )
    assert wr.catalog.does_table_exist(database=database, table=temp_table) is False
    assert len(wr.s3.list_objects(path=s3_output)) > 2
    assert len(wr.s3.list_objects(path=final_destination)) > 0
    for df in dfs:
        ensure_data_types(df=df, has_list=True)
    assert len(wr.s3.list_objects(path=s3_output)) > 2

    # Cleaning Up
    wr.catalog.delete_table_if_exists(database=database, table="test_athena_ctas")
    wr.s3.delete_objects(path=paths)
    wr.s3.wait_objects_not_exist(paths=paths)
    wr.s3.delete_objects(path=f"s3://{bucket}/test_athena_ctas_result/")


def test_athena(bucket, database, kms_key, workgroup0, workgroup1):
    wr.s3.delete_objects(path=f"s3://{bucket}/test_athena/")
    paths = wr.s3.to_parquet(
        df=get_df(),
        path=f"s3://{bucket}/test_athena",
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
        print(df2)
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
    wr.s3.delete_objects(path=paths)
    wr.s3.wait_objects_not_exist(paths=paths)
    wr.s3.delete_objects(path=f"s3://{bucket}/athena_workgroup0/")
    wr.s3.delete_objects(path=f"s3://{bucket}/athena_workgroup1/")


def test_csv(bucket):
    session = boto3.Session()
    df = pd.DataFrame({"id": [1, 2, 3]})
    path0 = f"s3://{bucket}/test_csv0.csv"
    path1 = f"s3://{bucket}/test_csv1.csv"
    path2 = f"s3://{bucket}/test_csv2.csv"
    wr.s3.to_csv(df=df, path=path0, index=False)
    wr.s3.wait_objects_exist(paths=[path0])
    assert wr.s3.does_object_exist(path=path0) is True
    assert wr.s3.size_objects(path=[path0], use_threads=False)[path0] == 9
    assert wr.s3.size_objects(path=[path0], use_threads=True)[path0] == 9
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
    with pytest.raises(wr.exceptions.InvalidArgument):
        wr.s3.read_csv(path=paths, iterator=True)
    wr.s3.delete_objects(path=paths, use_threads=False)
    wr.s3.wait_objects_not_exist(paths=paths, use_threads=False)


def test_json(bucket):
    df0 = pd.DataFrame({"id": [1, 2, 3]})
    path0 = f"s3://{bucket}/test_json0.json"
    path1 = f"s3://{bucket}/test_json1.json"
    wr.s3.to_json(df=df0, path=path0)
    wr.s3.to_json(df=df0, path=path1)
    wr.s3.wait_objects_exist(paths=[path0, path1])
    assert df0.equals(wr.s3.read_json(path=path0, use_threads=False))
    df1 = pd.concat(objs=[df0, df0], sort=False, ignore_index=True)
    assert df1.equals(wr.s3.read_json(path=[path0, path1], use_threads=True))
    wr.s3.delete_objects(path=[path0, path1], use_threads=False)


def test_fwf(bucket):
    text = "1 Herfelingen27-12-18\n2   Lambusart14-06-18\n3Spormaggiore15-04-18"
    path0 = f"s3://{bucket}/test_fwf0.txt"
    path1 = f"s3://{bucket}/test_fwf1.txt"
    client_s3 = boto3.client("s3")
    client_s3.put_object(Body=text, Bucket=bucket, Key="test_fwf0.txt")
    client_s3.put_object(Body=text, Bucket=bucket, Key="test_fwf1.txt")
    wr.s3.wait_objects_exist(paths=[path0, path1])
    df = wr.s3.read_fwf(path=path0, use_threads=False, widths=[1, 12, 8], names=["id", "name", "date"])
    assert len(df.index) == 3
    assert len(df.columns) == 3
    df = wr.s3.read_fwf(path=[path0, path1], use_threads=True, widths=[1, 12, 8], names=["id", "name", "date"])
    assert len(df.index) == 6
    assert len(df.columns) == 3
    wr.s3.delete_objects(path=[path0, path1], use_threads=False)


def test_parquet(bucket):
    wr.s3.delete_objects(path=f"s3://{bucket}/test_parquet/")
    df_file = pd.DataFrame({"id": [1, 2, 3]})
    path_file = f"s3://{bucket}/test_parquet/test_parquet_file.parquet"
    df_dataset = pd.DataFrame({"id": [1, 2, 3], "partition": ["A", "A", "B"]})
    df_dataset["partition"] = df_dataset["partition"].astype("category")
    path_dataset = f"s3://{bucket}/test_parquet/test_parquet_dataset"
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_parquet(df=df_file, path=path_file, mode="append")
    with pytest.raises(wr.exceptions.InvalidCompression):
        wr.s3.to_parquet(df=df_file, path=path_file, compression="WRONG")
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_parquet(df=df_dataset, path=path_dataset, partition_cols=["col2"])
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_parquet(df=df_dataset, path=path_dataset, description="foo")
    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.s3.to_parquet(df=df_dataset, path=path_dataset, partition_cols=["col2"], dataset=True, mode="WRONG")
    paths = wr.s3.to_parquet(df=df_file, path=path_file)["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    assert len(wr.s3.read_parquet(path=path_file, use_threads=True, boto3_session=None).index) == 3
    assert len(wr.s3.read_parquet(path=[path_file], use_threads=False, boto3_session=boto3.Session()).index) == 3
    paths = wr.s3.to_parquet(df=df_dataset, path=path_dataset, dataset=True)["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    assert len(wr.s3.read_parquet(path=paths, dataset=True).index) == 3
    assert len(wr.s3.read_parquet(path=path_dataset, use_threads=True, boto3_session=boto3.Session()).index) == 3
    dataset_paths = wr.s3.to_parquet(
        df=df_dataset, path=path_dataset, dataset=True, partition_cols=["partition"], mode="overwrite"
    )["paths"]
    wr.s3.wait_objects_exist(paths=dataset_paths)
    assert len(wr.s3.read_parquet(path=path_dataset, use_threads=True, boto3_session=None).index) == 3
    assert len(wr.s3.read_parquet(path=dataset_paths, use_threads=True).index) == 3
    assert len(wr.s3.read_parquet(path=path_dataset, dataset=True, use_threads=True).index) == 3
    wr.s3.to_parquet(df=df_dataset, path=path_dataset, dataset=True, partition_cols=["partition"], mode="overwrite")
    wr.s3.to_parquet(
        df=df_dataset, path=path_dataset, dataset=True, partition_cols=["partition"], mode="overwrite_partitions"
    )
    wr.s3.delete_objects(path=f"s3://{bucket}/test_parquet/")


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
    assert len(columns_types) == 17
    assert len(partitions_types) == 2
    columns_types, partitions_types, partitions_values = wr.s3.store_parquet_metadata(
        path=f"s3://{bucket}/test_parquet_catalog2", database=database, table="test_parquet_catalog2", dataset=True
    )
    assert len(columns_types) == 17
    assert len(partitions_types) == 2
    assert len(partitions_values) == 2
    wr.s3.delete_objects(path=f"s3://{bucket}/test_parquet_catalog/")
    wr.s3.delete_objects(path=f"s3://{bucket}/test_parquet_catalog2/")
    assert wr.catalog.delete_table_if_exists(database=database, table="test_parquet_catalog") is True
    assert wr.catalog.delete_table_if_exists(database=database, table="test_parquet_catalog2") is True


def test_parquet_catalog_duplicated(bucket, database):
    path = f"s3://{bucket}/test_parquet_catalog_dedup/"
    df = pd.DataFrame({"A": [1], "a": [1]})
    wr.s3.to_parquet(
        df=df,
        path=path,
        index=False,
        dataset=True,
        mode="overwrite",
        database=database,
        table="test_parquet_catalog_dedup",
    )
    df = wr.s3.read_parquet(path=path)
    assert len(df.index) == 1
    assert len(df.columns) == 1
    wr.s3.delete_objects(path=path)
    assert wr.catalog.delete_table_if_exists(database=database, table="test_parquet_catalog_dedup") is True


def test_parquet_catalog_casting(bucket, database):
    path = f"s3://{bucket}/test_parquet_catalog_casting/"
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
    assert len(df.index) == 3
    assert len(df.columns) == 15
    ensure_data_types(df=df, has_list=False)
    df = wr.athena.read_sql_table(table="__test_parquet_catalog_casting", database=database, ctas_approach=True)
    assert len(df.index) == 3
    assert len(df.columns) == 15
    ensure_data_types(df=df, has_list=False)
    df = wr.athena.read_sql_table(table="__test_parquet_catalog_casting", database=database, ctas_approach=False)
    assert len(df.index) == 3
    assert len(df.columns) == 15
    ensure_data_types(df=df, has_list=False)
    wr.s3.delete_objects(path=path)
    assert wr.catalog.delete_table_if_exists(database=database, table="__test_parquet_catalog_casting") is True


def test_catalog(bucket, database):
    account_id = boto3.client("sts").get_caller_identity().get("Account")
    path = f"s3://{bucket}/test_catalog/"
    wr.catalog.delete_table_if_exists(database=database, table="test_catalog")
    assert wr.catalog.does_table_exist(database=database, table="test_catalog") is False
    wr.catalog.create_parquet_table(
        database=database,
        table="test_catalog",
        path=path,
        columns_types={"col0": "int", "col1": "double"},
        partitions_types={"y": "int", "m": "int"},
        compression="snappy",
    )
    wr.catalog.create_parquet_table(
        database=database, table="test_catalog", path=path, columns_types={"col0": "string"}, mode="append"
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
    assert len(set(partitions_values[f"{path}y=2021/m=2/"]) & {"2021", "2"}) == 2
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
    assert wr.catalog.delete_table_if_exists(database=database, table="test_catalog") is True


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


def test_athena_read_list(database):
    with pytest.raises(wr.exceptions.UnsupportedType):
        wr.athena.read_sql_query(sql=f"SELECT ARRAY[1, 2, 3]", database=database, ctas_approach=False)


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


def test_s3_empty_dfs():
    df = pd.DataFrame()
    with pytest.raises(wr.exceptions.EmptyDataFrame):
        wr.s3.to_parquet(df=df, path="")
    with pytest.raises(wr.exceptions.EmptyDataFrame):
        wr.s3.to_csv(df=df, path="")


def test_absent_object(bucket):
    path = f"s3://{bucket}/test_absent_object"
    assert wr.s3.does_object_exist(path=path) is False
    assert len(wr.s3.size_objects(path=path)) == 0
    assert wr.s3.wait_objects_exist(paths=[]) is None


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


def test_parquet_validate_schema(bucket, database):
    path = f"s3://{bucket}/test_parquet_file_validate/"
    wr.s3.delete_objects(path=path)
    df = pd.DataFrame({"id": [1, 2, 3]})
    path_file = f"s3://{bucket}/test_parquet_file_validate/0.parquet"
    wr.s3.to_parquet(df=df, path=path_file)
    wr.s3.wait_objects_exist(paths=[path_file])
    df2 = pd.DataFrame({"id2": [1, 2, 3], "val": ["foo", "boo", "bar"]})
    path_file2 = f"s3://{bucket}/test_parquet_file_validate/1.parquet"
    wr.s3.to_parquet(df=df2, path=path_file2)
    wr.s3.wait_objects_exist(paths=[path_file2], use_threads=False)
    df3 = wr.s3.read_parquet(path=path, validate_schema=False)
    assert len(df3.index) == 6
    assert len(df3.columns) == 3
    with pytest.raises(ValueError):
        wr.s3.read_parquet(path=path, validate_schema=True)
    with pytest.raises(ValueError):
        wr.s3.store_parquet_metadata(path=path, database=database, table="test_parquet_validate_schema", dataset=True)
    wr.s3.delete_objects(path=path)


def test_csv_dataset(bucket, database):
    path = f"s3://{bucket}/test_csv_dataset/"
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


def test_csv_catalog(bucket, database):
    path = f"s3://{bucket}/test_csv_catalog/"
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
        table="test_csv_catalog",
        database=database,
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    df2 = wr.athena.read_sql_table("test_csv_catalog", database)
    assert len(df2.index) == 3
    assert len(df2.columns) == 11
    assert df2["id"].sum() == 6
    ensure_data_types_csv(df2)
    wr.s3.delete_objects(path=paths)
    assert wr.catalog.delete_table_if_exists(database=database, table="test_csv_catalog") is True


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


@pytest.mark.parametrize("compression", ["gzip", "bz2", "xz"])
def test_csv_compress(bucket, compression):
    path = f"s3://{bucket}/test_csv_compress_{compression}/"
    wr.s3.delete_objects(path=path)
    df = get_df_csv()
    if compression == "gzip":
        buffer = BytesIO()
        with gzip.GzipFile(mode="w", fileobj=buffer) as zipped_file:
            df.to_csv(TextIOWrapper(zipped_file, "utf8"), index=False, header=None)
        s3_resource = boto3.resource("s3")
        s3_object = s3_resource.Object(bucket, f"test_csv_compress_{compression}/test.csv.gz")
        s3_object.put(Body=buffer.getvalue())
        file_path = f"s3://{bucket}/test_csv_compress_{compression}/test.csv.gz"
    elif compression == "bz2":
        buffer = BytesIO()
        with bz2.BZ2File(mode="w", filename=buffer) as zipped_file:
            df.to_csv(TextIOWrapper(zipped_file, "utf8"), index=False, header=None)
        s3_resource = boto3.resource("s3")
        s3_object = s3_resource.Object(bucket, f"test_csv_compress_{compression}/test.csv.bz2")
        s3_object.put(Body=buffer.getvalue())
        file_path = f"s3://{bucket}/test_csv_compress_{compression}/test.csv.bz2"
    elif compression == "xz":
        buffer = BytesIO()
        with lzma.LZMAFile(mode="w", filename=buffer) as zipped_file:
            df.to_csv(TextIOWrapper(zipped_file, "utf8"), index=False, header=None)
        s3_resource = boto3.resource("s3")
        s3_object = s3_resource.Object(bucket, f"test_csv_compress_{compression}/test.csv.xz")
        s3_object.put(Body=buffer.getvalue())
        file_path = f"s3://{bucket}/test_csv_compress_{compression}/test.csv.xz"
    else:
        file_path = f"s3://{bucket}/test_csv_compress_{compression}/test.csv"
        wr.s3.to_csv(df=df, path=file_path, index=False, header=None)

    wr.s3.wait_objects_exist(paths=[file_path])
    df2 = wr.s3.read_csv(path=[file_path], names=df.columns)
    assert len(df2.index) == 3
    assert len(df2.columns) == 10
    dfs = wr.s3.read_csv(path=[file_path], names=df.columns, chunksize=1)
    for df3 in dfs:
        assert len(df3.columns) == 10
    wr.s3.delete_objects(path=path)


def test_parquet_char_length(bucket, database, external_schema):
    path = f"s3://{bucket}/test_parquet_char_length/"
    table = "test_parquet_char_length"

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

    engine = wr.catalog.get_engine("aws-data-wrangler-redshift")
    df2 = wr.db.read_sql_table(con=engine, table=table, schema=external_schema)
    assert len(df2.index) == 2
    assert len(df2.columns) == 3
    assert df2.id.sum() == 3

    wr.s3.delete_objects(path=path)
    assert wr.catalog.delete_table_if_exists(database=database, table=table) is True


def test_merge(bucket):
    path = f"s3://{bucket}/test_merge/"
    df = pd.DataFrame({"id": [1, 2, 3], "par": [1, 2, 3]})
    paths = wr.s3.to_parquet(df=df, path=path, dataset=True, partition_cols=["par"], mode="overwrite")["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    df = wr.s3.read_parquet(path=path, dataset=True)
    assert df.id.sum() == 6
    assert df.par.astype("Int64").sum() == 6

    path2 = f"s3://{bucket}/test_merge2/"
    df = pd.DataFrame({"id": [1, 2, 3], "par": [1, 2, 3]})
    paths = wr.s3.to_parquet(df=df, path=path2, dataset=True, partition_cols=["par"], mode="overwrite")["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    paths = wr.s3.merge_datasets(source_path=path2, target_path=path, mode="append", use_threads=True)
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df = wr.s3.read_parquet(path=path, dataset=True)
    assert df.id.sum() == 12
    assert df.par.astype("Int64").sum() == 12

    paths = wr.s3.merge_datasets(source_path=path2, target_path=path, mode="overwrite", use_threads=False)
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df = wr.s3.read_parquet(path=path, dataset=True)
    assert df.id.sum() == 6
    assert df.par.astype("Int64").sum() == 6

    df = pd.DataFrame({"id": [4], "par": [3]})
    paths = wr.s3.to_parquet(df=df, path=path2, dataset=True, partition_cols=["par"], mode="overwrite")["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    paths = wr.s3.merge_datasets(source_path=path2, target_path=path, mode="overwrite_partitions", use_threads=True)
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df = wr.s3.read_parquet(path=path, dataset=True)
    assert df.id.sum() == 7
    assert df.par.astype("Int64").sum() == 6

    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.s3.merge_datasets(source_path=path, target_path="bar", mode="WRONG")

    assert len(wr.s3.merge_datasets(source_path=f"s3://{bucket}/empty/", target_path="bar")) == 0

    wr.s3.delete_objects(path=path)
    wr.s3.delete_objects(path=path2)


def test_copy(bucket):
    path = f"s3://{bucket}/test_copy/"
    df = pd.DataFrame({"id": [1, 2, 3], "par": [1, 2, 3]})
    paths = wr.s3.to_parquet(df=df, path=path, dataset=True, partition_cols=["par"], mode="overwrite")["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    df = wr.s3.read_parquet(path=path, dataset=True)
    assert df.id.sum() == 6
    assert df.par.astype("Int64").sum() == 6

    path2 = f"s3://{bucket}/test_copy2/"
    df = pd.DataFrame({"id": [1, 2, 3], "par": [1, 2, 3]})
    paths = wr.s3.to_parquet(df=df, path=path2, dataset=True, partition_cols=["par"], mode="overwrite")["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    paths = wr.s3.copy_objects(paths, source_path=path2, target_path=path, use_threads=True)
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df = wr.s3.read_parquet(path=path, dataset=True)
    assert df.id.sum() == 12
    assert df.par.astype("Int64").sum() == 12

    assert len(wr.s3.copy_objects([], source_path="boo", target_path="bar")) == 0

    wr.s3.delete_objects(path=path)
    wr.s3.delete_objects(path=path2)


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
def test_athena_encryption(
    bucket, database, kms_key, encryption, workgroup, workgroup0, workgroup1, workgroup2, workgroup3
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
    table = f"test_athena_encryption_{str(encryption).lower()}_{str(workgroup).lower()}"
    path = f"s3://{bucket}/{table}/"
    wr.s3.delete_objects(path=path)
    df = pd.DataFrame({"a": [1, 2], "b": ["foo", "boo"]})
    paths = wr.s3.to_parquet(
        df=df, path=path, dataset=True, mode="overwrite", database=database, table=table, s3_additional_kwargs=None
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    temp_table = table + "2"
    s3_output = f"s3://{bucket}/encryptio_s3_output/"
    final_destination = f"{s3_output}{temp_table}/"
    wr.s3.delete_objects(path=final_destination)
    df2 = wr.athena.read_sql_table(
        table=table,
        ctas_approach=True,
        database=database,
        encryption=encryption,
        workgroup=workgroup,
        kms_key=kms_key,
        keep_files=True,
        ctas_temp_table_name=temp_table,
        s3_output=s3_output,
    )
    assert wr.catalog.does_table_exist(database=database, table=temp_table) is False
    assert len(wr.s3.list_objects(path=s3_output)) > 2
    print(df2)
    assert len(df2.index) == 2
    assert len(df2.columns) == 2
    wr.catalog.delete_table_if_exists(database=database, table=table)
    wr.s3.delete_objects(path=paths)


def test_athena_nested(bucket, database):
    table = "test_athena_nested"
    path = f"s3://{bucket}/{table}/"
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


def test_copy_replacing_filename(bucket):
    path = f"s3://{bucket}/test_copy_replacing_filename/"
    wr.s3.delete_objects(path=path)
    df = pd.DataFrame({"c0": [1, 2]})
    file_path = f"{path}myfile.parquet"
    wr.s3.to_parquet(df=df, path=file_path)
    wr.s3.wait_objects_exist(paths=[file_path], use_threads=False)
    path2 = f"s3://{bucket}/test_copy_replacing_filename2/"
    wr.s3.copy_objects(
        paths=[file_path], source_path=path, target_path=path2, replace_filenames={"myfile.parquet": "myfile2.parquet"}
    )
    expected_file = f"{path2}myfile2.parquet"
    wr.s3.wait_objects_exist(paths=[expected_file], use_threads=False)
    objs = wr.s3.list_objects(path=path2)
    assert objs[0] == expected_file
    wr.s3.delete_objects(path=path)
    wr.s3.delete_objects(path=path2)


def test_unsigned_parquet(bucket, database, external_schema):
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
    engine = wr.catalog.get_engine("aws-data-wrangler-redshift")
    df = wr.db.read_sql_table(con=engine, table=table, schema=external_schema)
    assert df.c0.sum() == (2 ** 8) - 1
    assert df.c1.sum() == (2 ** 16) - 1
    assert df.c2.sum() == (2 ** 32) - 1

    df = pd.DataFrame({"c0": [0, 0, (2 ** 64) - 1]})
    df["c0"] = df.c0.astype("uint64")
    with pytest.raises(wr.exceptions.UnsupportedType):
        wr.s3.to_parquet(df=df, path=path, dataset=True, database=database, table=table, mode="overwrite")

    wr.s3.delete_objects(path=path)
    wr.catalog.delete_table_if_exists(database=database, table=table)


def test_parquet_uint64(bucket):
    path = f"s3://{bucket}/test_parquet_uint64/"
    wr.s3.delete_objects(path=path)
    df = pd.DataFrame(
        {
            "c0": [0, 0, (2 ** 8) - 1],
            "c1": [0, 0, (2 ** 16) - 1],
            "c2": [0, 0, (2 ** 32) - 1],
            "c3": [0, 0, (2 ** 64) - 1],
            "c4": [0, 1, 2],
        }
    )
    print(df)
    df["c0"] = df.c0.astype("uint8")
    df["c1"] = df.c1.astype("uint16")
    df["c2"] = df.c2.astype("uint32")
    df["c3"] = df.c3.astype("uint64")
    paths = wr.s3.to_parquet(df=df, path=path, dataset=True, mode="overwrite", partition_cols=["c4"])["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df = wr.s3.read_parquet(path=path, dataset=True)
    print(df)
    print(df.dtypes)
    assert len(df.index) == 3
    assert len(df.columns) == 5
    assert df.c0.max() == (2 ** 8) - 1
    assert df.c1.max() == (2 ** 16) - 1
    assert df.c2.max() == (2 ** 32) - 1
    assert df.c3.max() == (2 ** 64) - 1
    assert df.c4.astype("uint8").sum() == 3
    wr.s3.delete_objects(path=path)


def test_parquet_overwrite_partition_cols(bucket, database, external_schema):
    table = "test_parquet_overwrite_partition_cols"
    path = f"s3://{bucket}/{table}/"
    wr.s3.delete_objects(path=path)
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

    engine = wr.catalog.get_engine("aws-data-wrangler-redshift")
    df = wr.db.read_sql_table(con=engine, table=table, schema=external_schema)
    assert len(df.index) == 4
    assert len(df.columns) == 3
    assert df.c0.sum() == 6
    assert df.c1.sum() == 6
    assert df.c2.sum() == 6

    wr.s3.delete_objects(path=path)
    wr.catalog.delete_table_if_exists(database=database, table=table)


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
