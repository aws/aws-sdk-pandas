import itertools
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


@pytest.mark.parametrize("partition_cols", [None, ["c2"], ["c1", "c2"]])
def test_metadata_partitions_dataset(path, partition_cols):
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [3, 4, 5], "c2": [6, 7, 8]})
    paths = wr.s3.to_parquet(df=df, path=path, dataset=True, partition_cols=partition_cols)["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    columns_types, partitions_types = wr.s3.read_parquet_metadata(path=path, dataset=True)
    partitions_types = partitions_types if partitions_types is not None else {}
    assert len(columns_types) + len(partitions_types) == len(df.columns)
    assert columns_types.get("c0") == "bigint"
    assert (columns_types.get("c1") == "bigint") or (partitions_types.get("c1") == "string")
    assert (columns_types.get("c1") == "bigint") or (partitions_types.get("c1") == "string")


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


def test_json_chunksize(path):
    num_files = 10
    df = pd.DataFrame({"id": [1, 2, 3], "value": ["foo", "boo", "bar"]})
    paths = [f"{path}{i}.json" for i in range(num_files)]
    for p in paths:
        wr.s3.to_json(df, p, orient="records", lines=True)
    wr.s3.wait_objects_exist(paths)
    dfs = list(wr.s3.read_json(paths, lines=True, chunksize=1))
    assert len(dfs) == (3 * num_files)
    for d in dfs:
        assert len(d.columns) == 2
        assert d.id.iloc[0] in (1, 2, 3)
        assert d.value.iloc[0] in ("foo", "boo", "bar")


def test_parquet_cast_string(path):
    df = pd.DataFrame({"id": [1, 2, 3], "value": ["foo", "boo", "bar"]})
    path_file = f"{path}0.parquet"
    wr.s3.to_parquet(df, path_file, dtype={"id": "string"})
    wr.s3.wait_objects_exist([path_file])
    df2 = wr.s3.read_parquet(path_file)
    assert str(df2.id.dtypes) == "string"
    df2["id"] = df2["id"].astype(int)
    assert df.shape == df2.shape
    for col, row in tuple(itertools.product(df.columns, range(3))):
        assert df[col].iloc[row] == df2[col].iloc[row]


@pytest.mark.parametrize("partition_cols", [None, ["c2"], ["value", "c2"]])
def test_parquet_cast_string_dataset(path, partition_cols):
    df = pd.DataFrame({"id": [1, 2, 3], "value": ["foo", "boo", "bar"], "c2": [4, 5, 6], "c3": [7.0, 8.0, 9.0]})
    paths = wr.s3.to_parquet(
        df, path, dataset=True, partition_cols=partition_cols, dtype={"id": "string", "c3": "string"}
    )["paths"]
    wr.s3.wait_objects_exist(paths)
    df2 = wr.s3.read_parquet(path, dataset=True).sort_values("id", ignore_index=True)
    assert str(df2.id.dtypes) == "string"
    assert str(df2.c3.dtypes) == "string"
    df2["id"] = df2["id"].astype(int)
    df2["c3"] = df2["c3"].astype(float)
    assert df.shape == df2.shape
    for col, row in tuple(itertools.product(df.columns, range(3))):
        assert df[col].iloc[row] == df2[col].iloc[row]


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


def test_to_parquet_file_sanitize(path):
    df = pd.DataFrame({"C0": [0, 1], "camelCase": [2, 3], "c**--2": [4, 5]})
    path_file = f"{path}0.parquet"
    wr.s3.to_parquet(df, path_file)
    wr.s3.wait_objects_exist([path_file])
    df2 = wr.s3.read_parquet(path_file)
    assert df.shape == df2.shape
    assert list(df2.columns) == ["c0", "camel_case", "c_2"]
    assert df2.c0.sum() == 1
    assert df2.camel_case.sum() == 5
    assert df2.c_2.sum() == 9


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


@pytest.mark.parametrize(
    "encoding,strings,wrong_encoding,exception",
    [
        ("utf-8", ["漢字", "ãóú", "г, д, ж, з, к, л"], "ISO-8859-1", AssertionError),
        ("ISO-8859-1", ["Ö, ö, Ü, ü", "ãóú", "øe"], "utf-8", UnicodeDecodeError),
        ("ISO-8859-1", ["Ö, ö, Ü, ü", "ãóú", "øe"], None, UnicodeDecodeError),
    ],
)
@pytest.mark.parametrize("line_terminator", ["\n", "\r"])
def test_csv_encoding(path, encoding, strings, wrong_encoding, exception, line_terminator):
    file_path = f"{path}0.csv"
    df = pd.DataFrame({"c0": [1, 2, 3], "c1": strings})
    wr.s3.to_csv(df, file_path, index=False, encoding=encoding, line_terminator=line_terminator)
    wr.s3.wait_objects_exist(paths=[file_path])
    df2 = wr.s3.read_csv(file_path, encoding=encoding, lineterminator=line_terminator)
    assert df.equals(df2)
    with pytest.raises(exception):
        df2 = wr.s3.read_csv(file_path, encoding=wrong_encoding)
        assert df.equals(df2)


def test_to_parquet_file_dtype(path):
    df = pd.DataFrame({"c0": [1.0, None, 2.0], "c1": [pd.NA, pd.NA, pd.NA]})
    file_path = f"{path}0.parquet"
    wr.s3.to_parquet(df, file_path, dtype={"c0": "bigint", "c1": "string"})
    wr.s3.wait_objects_exist(paths=[file_path])
    df2 = wr.s3.read_parquet(file_path)
    assert df2.shape == df.shape
    assert df2.c0.sum() == 3
    assert str(df2.c0.dtype) == "Int64"
    assert str(df2.c1.dtype) == "string"


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
