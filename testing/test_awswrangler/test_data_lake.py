import datetime
import logging

import boto3
import pandas as pd
import pytest

import awswrangler as wr

from ._utils import (ensure_data_types, ensure_data_types_category, get_df, get_df_cast, get_df_category, get_df_list,
                     get_query_long)

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
def workgroup0(bucket):
    wkg_name = "awswrangler_test_0"
    client = boto3.client("athena")
    wkgs = client.list_work_groups()
    wkgs = [x["Name"] for x in wkgs["WorkGroups"]]
    if wkg_name not in wkgs:
        client.create_work_group(
            Name=wkg_name,
            Configuration={
                "ResultConfiguration": {
                    "OutputLocation": f"s3://{bucket}/athena_workgroup0/",
                    "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
                },
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
    wkg_name = "awswrangler_test_1"
    client = boto3.client("athena")
    wkgs = client.list_work_groups()
    wkgs = [x["Name"] for x in wkgs["WorkGroups"]]
    if wkg_name not in wkgs:
        client.create_work_group(
            Name=wkg_name,
            Configuration={
                "ResultConfiguration": {"OutputLocation": f"s3://{bucket}/athena_workgroup1/"},
                "EnforceWorkGroupConfiguration": True,
                "PublishCloudWatchMetricsEnabled": True,
                "BytesScannedCutoffPerQuery": 100_000_000,
                "RequesterPaysEnabled": False,
            },
            Description="AWS Data Wrangler Test WorkGroup Number 1",
        )
    yield wkg_name


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
    )
    for df2 in dfs:
        print(df2)
        ensure_data_types(df=df2)
    df = wr.athena.read_sql_query(
        sql="SELECT * FROM __test_athena", database=database, ctas_approach=False, workgroup=workgroup1
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
    wr.s3.delete_objects(path=f"s3://{bucket}/test_parquet_file")
    wr.s3.delete_objects(path=f"s3://{bucket}/test_parquet_dataset")
    df_file = pd.DataFrame({"id": [1, 2, 3]})
    path_file = f"s3://{bucket}/test_parquet_file.parquet"
    df_dataset = pd.DataFrame({"id": [1, 2, 3], "partition": ["A", "A", "B"]})
    df_dataset["partition"] = df_dataset["partition"].astype("category")
    path_dataset = f"s3://{bucket}/test_parquet_dataset"
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
    wr.s3.delete_objects(path=f"s3://{bucket}/test_parquet_file")
    wr.s3.delete_objects(path=f"s3://{bucket}/test_parquet_dataset")


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
