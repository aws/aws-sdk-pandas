import logging

import boto3
import pandas as pd
import pytest

import awswrangler as wr

from ._utils import dt, extract_cloudformation_outputs, get_time_str_with_random_suffix, path_generator, ts

logging.basicConfig(level=logging.INFO, format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)
logging.getLogger("botocore.credentials").setLevel(logging.CRITICAL)


@pytest.fixture(scope="module")
def cloudformation_outputs():
    yield extract_cloudformation_outputs()


@pytest.fixture(scope="module")
def region(cloudformation_outputs):
    yield cloudformation_outputs["Region"]


@pytest.fixture(scope="module")
def database(cloudformation_outputs):
    yield cloudformation_outputs["GlueDatabaseName"]


@pytest.fixture(scope="module")
def external_schema(cloudformation_outputs, database):
    region = cloudformation_outputs.get("Region")
    sql = f"""
    CREATE EXTERNAL SCHEMA IF NOT EXISTS aws_data_wrangler_external FROM data catalog
    DATABASE '{database}'
    IAM_ROLE '{cloudformation_outputs["RedshiftRole"]}'
    REGION '{region}';
    """
    engine = wr.catalog.get_engine(connection="aws-data-wrangler-redshift")
    with engine.connect() as con:
        con.execute(sql)
    yield "aws_data_wrangler_external"


@pytest.fixture(scope="function")
def path(cloudformation_outputs):
    yield from path_generator(cloudformation_outputs["BucketName"])


@pytest.fixture(scope="function")
def table(database):
    name = f"tbl_{get_time_str_with_random_suffix()}"
    print(f"Table name: {name}")
    wr.catalog.delete_table_if_exists(database=database, table=name)
    yield name
    wr.catalog.delete_table_if_exists(database=database, table=name)


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


def test_read_parquet_filter_partitions(path):
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [0, 1, 2], "c2": [0, 0, 1]})
    paths = wr.s3.to_parquet(df, path, dataset=True, partition_cols=["c1", "c2"])["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df2 = wr.s3.read_parquet(path, dataset=True, filters=[("c1", "==", "0")])
    assert df2.shape == (1, 3)
    assert df2.c0.iloc[0] == 0
    assert df2.c1.iloc[0] == 0
    assert df2.c2.iloc[0] == 0
    df2 = wr.s3.read_parquet(path, dataset=True, filters=[("c1", "==", "1"), ("c2", "==", "0")])
    assert df2.shape == (1, 3)
    assert df2.c0.iloc[0] == 1
    assert df2.c1.iloc[0] == 1
    assert df2.c2.iloc[0] == 0
    df2 = wr.s3.read_parquet(path, dataset=True, filters=[("c2", "==", "0")])
    assert df2.shape == (2, 3)
    assert df2.c0.astype(int).sum() == 1
    assert df2.c1.astype(int).sum() == 1
    assert df2.c2.astype(int).sum() == 0


@pytest.mark.parametrize("use_threads", [True, False])
@pytest.mark.parametrize("chunksize", [None, 1])
def test_read_partitioned_json(path, use_threads, chunksize):
    df = pd.DataFrame({"c0": [0, 1], "c1": ["foo", "boo"]})
    paths = [f"{path}year={y}/month={m}/0.json" for y, m in [(2020, 1), (2020, 2), (2021, 1)]]
    for p in paths:
        wr.s3.to_json(df, p, orient="records", lines=True)
    wr.s3.wait_objects_exist(paths, use_threads=False)
    df2 = wr.s3.read_json(path, dataset=True, use_threads=use_threads, chunksize=chunksize)
    if chunksize is None:
        assert df2.shape == (6, 4)
        assert df2.c0.sum() == 3
    else:
        for d in df2:
            assert d.shape == (1, 4)


@pytest.mark.parametrize("use_threads", [True, False])
@pytest.mark.parametrize("chunksize", [None, 1])
def test_read_partitioned_csv(path, use_threads, chunksize):
    df = pd.DataFrame({"c0": [0, 1], "c1": ["foo", "boo"]})
    paths = [f"{path}year={y}/month={m}/0.csv" for y, m in [(2020, 1), (2020, 2), (2021, 1)]]
    for p in paths:
        wr.s3.to_csv(df, p, index=False)
    wr.s3.wait_objects_exist(paths, use_threads=False)
    df2 = wr.s3.read_csv(path, dataset=True, use_threads=use_threads, chunksize=chunksize)
    if chunksize is None:
        assert df2.shape == (6, 4)
        assert df2.c0.sum() == 3
    else:
        for d in df2:
            assert d.shape == (1, 4)


@pytest.mark.parametrize("use_threads", [True, False])
@pytest.mark.parametrize("chunksize", [None, 1])
def test_read_partitioned_fwf(path, use_threads, chunksize):
    text = "0foo\n1boo"
    client_s3 = boto3.client("s3")
    paths = [f"{path}year={y}/month={m}/0.csv" for y, m in [(2020, 1), (2020, 2), (2021, 1)]]
    for p in paths:
        bucket, key = wr._utils.parse_path(p)
        client_s3.put_object(Body=text, Bucket=bucket, Key=key)
    wr.s3.wait_objects_exist(paths, use_threads=False)
    df2 = wr.s3.read_fwf(
        path, dataset=True, use_threads=use_threads, chunksize=chunksize, widths=[1, 3], names=["c0", "c1"]
    )
    if chunksize is None:
        assert df2.shape == (6, 4)
        assert df2.c0.sum() == 3
    else:
        for d in df2:
            assert d.shape == (1, 4)
