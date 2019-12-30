import json
import logging
from datetime import date, datetime
from decimal import Decimal

import pytest
import boto3
import pandas as pd
from pyspark.sql import SparkSession
import pg8000

import awswrangler as wr
from awswrangler import Session, Redshift
from awswrangler.exceptions import InvalidRedshiftDiststyle, InvalidRedshiftDistkey, InvalidRedshiftSortstyle, InvalidRedshiftSortkey

logging.basicConfig(level=logging.INFO, format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)


@pytest.fixture(scope="module")
def cloudformation_outputs():
    response = boto3.client("cloudformation").describe_stacks(StackName="aws-data-wrangler-test")
    outputs = {}
    for output in response.get("Stacks")[0].get("Outputs"):
        outputs[output.get("OutputKey")] = output.get("OutputValue")
    yield outputs


@pytest.fixture(scope="module")
def session():
    yield Session(spark_session=SparkSession.builder.appName("AWS Wrangler Test").getOrCreate())


@pytest.fixture(scope="module")
def bucket(session, cloudformation_outputs):
    if "BucketName" in cloudformation_outputs:
        bucket = cloudformation_outputs.get("BucketName")
        session.s3.delete_objects(path=f"s3://{bucket}/")
    else:
        raise Exception("You must deploy the test infrastructure using SAM!")
    yield bucket
    session.s3.delete_objects(path=f"s3://{bucket}/")


@pytest.fixture(scope="module")
def redshift_parameters(cloudformation_outputs):
    redshift_parameters = {}
    if "RedshiftAddress" in cloudformation_outputs:
        redshift_parameters["RedshiftAddress"] = cloudformation_outputs.get("RedshiftAddress")
    else:
        raise Exception("You must deploy the test infrastructure using SAM!")
    if "Password" in cloudformation_outputs:
        redshift_parameters["Password"] = cloudformation_outputs.get("Password")
    else:
        raise Exception("You must deploy the test infrastructure using SAM!")
    if "RedshiftPort" in cloudformation_outputs:
        redshift_parameters["RedshiftPort"] = cloudformation_outputs.get("RedshiftPort")
    else:
        raise Exception("You must deploy the test infrastructure using SAM!")
    if "RedshiftRole" in cloudformation_outputs:
        redshift_parameters["RedshiftRole"] = cloudformation_outputs.get("RedshiftRole")
    else:
        raise Exception("You must deploy the test infrastructure using SAM!")
    yield redshift_parameters


@pytest.mark.parametrize(
    "sample_name,mode,factor,diststyle,distkey,sortstyle,sortkey",
    [
        ("micro", "overwrite", 1, "AUTO", "name", None, ["id"]),
        ("micro", "append", 2, None, None, "INTERLEAVED", ["id", "value"]),
        ("small", "overwrite", 1, "KEY", "name", "INTERLEAVED", ["id", "name"]),
        ("small", "append", 2, None, None, "INTERLEAVED", ["id", "name", "date"]),
        ("nano", "overwrite", 1, "ALL", None, "compound", ["id", "name", "date"]),
        ("nano", "append", 2, "ALL", "name", "INTERLEAVED", ["id"]),
    ],
)
def test_to_redshift_pandas(session, bucket, redshift_parameters, sample_name, mode, factor, diststyle, distkey,
                            sortstyle, sortkey):

    if sample_name == "micro":
        dates = ["date"]
    if sample_name == "small":
        dates = ["date"]
    if sample_name == "nano":
        dates = ["date", "time"]
    dataframe = pd.read_csv(f"data_samples/{sample_name}.csv", parse_dates=dates, infer_datetime_format=True)
    dataframe["date"] = dataframe["date"].dt.date
    con = Redshift.generate_connection(
        database="test",
        host=redshift_parameters.get("RedshiftAddress"),
        port=redshift_parameters.get("RedshiftPort"),
        user="test",
        password=redshift_parameters.get("Password"),
    )
    path = f"s3://{bucket}/redshift-load/"
    session.pandas.to_redshift(
        dataframe=dataframe,
        path=path,
        schema="public",
        table="test",
        connection=con,
        iam_role=redshift_parameters.get("RedshiftRole"),
        diststyle=diststyle,
        distkey=distkey,
        sortstyle=sortstyle,
        sortkey=sortkey,
        mode=mode,
        preserve_index=True,
    )
    cursor = con.cursor()
    cursor.execute("SELECT * from public.test")
    rows = cursor.fetchall()
    cursor.close()
    con.close()
    assert len(dataframe.index) * factor == len(rows)
    assert len(list(dataframe.columns)) + 1 == len(list(rows[0]))


def test_to_redshift_pandas_cast(session, bucket, redshift_parameters):
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["name1", "name2", "name3"],
        "foo": [None, None, None],
        "boo": [date(2020, 1, 1), None, None],
        "bar": [datetime(2021, 1, 1), None, None]
    })
    schema = {"id": "BIGINT", "name": "VARCHAR", "foo": "REAL", "boo": "DATE", "bar": "TIMESTAMP"}
    con = Redshift.generate_connection(
        database="test",
        host=redshift_parameters.get("RedshiftAddress"),
        port=redshift_parameters.get("RedshiftPort"),
        user="test",
        password=redshift_parameters.get("Password"),
    )
    path = f"s3://{bucket}/redshift-load/"
    session.pandas.to_redshift(dataframe=df,
                               path=path,
                               schema="public",
                               table="test",
                               connection=con,
                               iam_role=redshift_parameters.get("RedshiftRole"),
                               mode="overwrite",
                               preserve_index=False,
                               cast_columns=schema)
    cursor = con.cursor()
    cursor.execute("SELECT * from public.test")
    rows = cursor.fetchall()
    cursor.close()
    con.close()
    assert len(df.index) == len(rows)
    assert len(list(df.columns)) == len(list(rows[0]))


@pytest.mark.parametrize(
    "sample_name,mode,factor,diststyle,distkey,exc,sortstyle,sortkey",
    [
        ("micro", "overwrite", 1, "FOO", "name", InvalidRedshiftDiststyle, None, None),
        ("micro", "overwrite", 2, "KEY", "FOO", InvalidRedshiftDistkey, None, None),
        ("small", "overwrite", 1, "KEY", None, InvalidRedshiftDistkey, None, None),
        ("small", "overwrite", 1, None, None, InvalidRedshiftSortkey, None, ["foo"]),
        ("small", "overwrite", 1, None, None, InvalidRedshiftSortstyle, "foo", ["id"]),
    ],
)
def test_to_redshift_pandas_exceptions(session, bucket, redshift_parameters, sample_name, mode, factor, diststyle,
                                       distkey, sortstyle, sortkey, exc):
    dataframe = pd.read_csv(f"data_samples/{sample_name}.csv")
    con = Redshift.generate_connection(
        database="test",
        host=redshift_parameters.get("RedshiftAddress"),
        port=redshift_parameters.get("RedshiftPort"),
        user="test",
        password=redshift_parameters.get("Password"),
    )
    path = f"s3://{bucket}/redshift-load/"
    with pytest.raises(exc):
        assert session.pandas.to_redshift(
            dataframe=dataframe,
            path=path,
            schema="public",
            table="test",
            connection=con,
            iam_role=redshift_parameters.get("RedshiftRole"),
            diststyle=diststyle,
            distkey=distkey,
            sortstyle=sortstyle,
            sortkey=sortkey,
            mode=mode,
            preserve_index=False,
        )
    con.close()


@pytest.mark.parametrize(
    "sample_name,mode,factor,diststyle,distkey,sortstyle,sortkey",
    [
        ("micro", "overwrite", 1, "AUTO", "name", None, ["id"]),
        ("micro", "append", 2, None, None, "INTERLEAVED", ["id", "value"]),
        ("small", "overwrite", 1, "KEY", "name", "INTERLEAVED", ["id", "name"]),
        ("small", "append", 2, None, None, "INTERLEAVED", ["id", "name", "date"]),
        ("nano", "overwrite", 1, "ALL", None, "compound", ["id", "name", "date"]),
        ("nano", "append", 2, "ALL", "name", "INTERLEAVED", ["id"]),
    ],
)
def test_to_redshift_spark(session, bucket, redshift_parameters, sample_name, mode, factor, diststyle, distkey,
                           sortstyle, sortkey):
    path = f"data_samples/{sample_name}.csv"
    if sample_name == "micro":
        schema = "id SMALLINT, name STRING, value FLOAT, date DATE"
        timestamp_format = "yyyy-MM-dd"
    elif sample_name == "small":
        schema = "id BIGINT, name STRING, date DATE"
        timestamp_format = "dd-MM-yy"
    elif sample_name == "nano":
        schema = "id INTEGER, name STRING, value DOUBLE, date DATE, time TIMESTAMP"
        timestamp_format = "yyyy-MM-dd"
    dataframe = session.spark.read_csv(path=path,
                                       schema=schema,
                                       timestampFormat=timestamp_format,
                                       dateFormat=timestamp_format,
                                       header=True)
    con = Redshift.generate_connection(
        database="test",
        host=redshift_parameters.get("RedshiftAddress"),
        port=redshift_parameters.get("RedshiftPort"),
        user="test",
        password=redshift_parameters.get("Password"),
    )
    session.spark.to_redshift(
        dataframe=dataframe,
        path=f"s3://{bucket}/redshift-load/",
        connection=con,
        schema="public",
        table="test",
        iam_role=redshift_parameters.get("RedshiftRole"),
        diststyle=diststyle,
        distkey=distkey,
        sortstyle=sortstyle,
        sortkey=sortkey,
        mode=mode,
        min_num_partitions=2,
    )
    cursor = con.cursor()
    cursor.execute("SELECT * from public.test")
    rows = cursor.fetchall()
    cursor.close()
    con.close()
    assert (dataframe.count() * factor) == len(rows)
    assert len(list(dataframe.columns)) == len(list(rows[0]))


def test_to_redshift_spark_big(session, bucket, redshift_parameters):
    dataframe = session.spark_session.createDataFrame(
        pd.DataFrame({
            "A": list(range(100_000)),
            "B": list(range(100_000)),
            "C": list(range(100_000))
        }))
    con = Redshift.generate_connection(
        database="test",
        host=redshift_parameters.get("RedshiftAddress"),
        port=redshift_parameters.get("RedshiftPort"),
        user="test",
        password=redshift_parameters.get("Password"),
    )
    session.spark.to_redshift(
        dataframe=dataframe,
        path=f"s3://{bucket}/redshift-load/",
        connection=con,
        schema="public",
        table="test",
        iam_role=redshift_parameters.get("RedshiftRole"),
        mode="overwrite",
        min_num_partitions=10,
    )
    cursor = con.cursor()
    cursor.execute("SELECT * from public.test")
    rows = cursor.fetchall()
    cursor.close()
    con.close()
    assert dataframe.count() == len(rows)
    assert len(list(dataframe.columns)) == len(list(rows[0]))


def test_to_redshift_spark_bool(session, bucket, redshift_parameters):
    dataframe = session.spark_session.createDataFrame(pd.DataFrame({"A": [1, 2, 3], "B": [True, False, True]}))
    con = Redshift.generate_connection(
        database="test",
        host=redshift_parameters.get("RedshiftAddress"),
        port=redshift_parameters.get("RedshiftPort"),
        user="test",
        password=redshift_parameters.get("Password"),
    )
    session.spark.to_redshift(
        dataframe=dataframe,
        path=f"s3://{bucket}/redshift-load-bool/",
        connection=con,
        schema="public",
        table="test",
        iam_role=redshift_parameters.get("RedshiftRole"),
        mode="overwrite",
        min_num_partitions=1,
    )
    cursor = con.cursor()
    cursor.execute("SELECT * from public.test")
    rows = cursor.fetchall()
    cursor.close()
    con.close()
    assert dataframe.count() == len(rows)
    assert len(list(dataframe.columns)) == len(list(rows[0]))
    assert type(rows[0][0]) == int
    assert type(rows[0][1]) == bool


def test_stress_to_redshift_spark_big(session, bucket, redshift_parameters):
    print("Creating DataFrame...")
    dataframe = session.spark_session.createDataFrame(pd.DataFrame({
        "A": list(range(10_000)),
        "B": list(range(10_000))
    }))
    dataframe.cache()
    for i in range(10):
        print(f"Run number: {i}")
        con = Redshift.generate_connection(
            database="test",
            host=redshift_parameters.get("RedshiftAddress"),
            port=redshift_parameters.get("RedshiftPort"),
            user="test",
            password=redshift_parameters.get("Password"),
        )
        session.spark.to_redshift(
            dataframe=dataframe,
            path=f"s3://{bucket}/redshift-load-{i}/",
            connection=con,
            schema="public",
            table="test",
            iam_role=redshift_parameters.get("RedshiftRole"),
            mode="overwrite",
            min_num_partitions=16,
        )
        con.close()
        dataframe.unpersist()


@pytest.mark.parametrize(
    "sample_name,mode,factor,diststyle,distkey,exc,sortstyle,sortkey",
    [
        ("micro", "overwrite", 1, "FOO", "name", InvalidRedshiftDiststyle, None, None),
        ("micro", "overwrite", 2, "KEY", "FOO", InvalidRedshiftDistkey, None, None),
        ("small", "overwrite", 1, "KEY", None, InvalidRedshiftDistkey, None, None),
        ("small", "overwrite", 1, None, None, InvalidRedshiftSortkey, None, ["foo"]),
        ("small", "overwrite", 1, None, None, InvalidRedshiftSortstyle, "foo", ["id"]),
    ],
)
def test_to_redshift_spark_exceptions(session, bucket, redshift_parameters, sample_name, mode, factor, diststyle,
                                      distkey, sortstyle, sortkey, exc):
    path = f"data_samples/{sample_name}.csv"
    dataframe = session.spark.read_csv(path=path)
    con = Redshift.generate_connection(
        database="test",
        host=redshift_parameters.get("RedshiftAddress"),
        port=redshift_parameters.get("RedshiftPort"),
        user="test",
        password=redshift_parameters.get("Password"),
    )
    with pytest.raises(exc):
        assert session.spark.to_redshift(
            dataframe=dataframe,
            path=f"s3://{bucket}/redshift-load/",
            connection=con,
            schema="public",
            table="test",
            iam_role=redshift_parameters.get("RedshiftRole"),
            diststyle=diststyle,
            distkey=distkey,
            sortstyle=sortstyle,
            sortkey=sortkey,
            mode=mode,
            min_num_partitions=2,
        )
    con.close()


def test_write_load_manifest(session, bucket):
    boto3.client("s3").upload_file("data_samples/small.csv", bucket, "data_samples/small.csv")
    object_path = f"s3://{bucket}/data_samples/small.csv"
    manifest_path = f"s3://{bucket}/manifest.json"
    session.redshift.write_load_manifest(manifest_path=manifest_path, objects_paths=[object_path])
    manifest_json = (boto3.client("s3").get_object(Bucket=bucket, Key="manifest.json").get("Body").read())
    manifest = json.loads(manifest_json)
    assert manifest.get("entries")[0].get("url") == object_path
    assert manifest.get("entries")[0].get("mandatory")
    assert manifest.get("entries")[0].get("meta").get("content_length") == 2247


def test_connection_timeout(redshift_parameters):
    with pytest.raises(pg8000.core.InterfaceError):
        Redshift.generate_connection(
            database="test",
            host=redshift_parameters.get("RedshiftAddress"),
            port=12345,
            user="test",
            password=redshift_parameters.get("Password"),
        )


def test_connection_with_different_port_types(redshift_parameters):
    conn = Redshift.generate_connection(
        database="test",
        host=redshift_parameters.get("RedshiftAddress"),
        port=str(redshift_parameters.get("RedshiftPort")),
        user="test",
        password=redshift_parameters.get("Password"),
    )
    conn.close()
    conn = Redshift.generate_connection(
        database="test",
        host=redshift_parameters.get("RedshiftAddress"),
        port=float(redshift_parameters.get("RedshiftPort")),
        user="test",
        password=redshift_parameters.get("Password"),
    )
    conn.close()


def test_to_redshift_pandas_decimal(session, bucket, redshift_parameters):
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "decimal_2": [Decimal((0, (1, 9, 9), -2)), None, Decimal((0, (1, 9, 0), -2))],
        "decimal_5": [Decimal((0, (1, 9, 9, 9, 9, 9), -5)), None,
                      Decimal((0, (1, 9, 0, 0, 0, 0), -5))],
    })
    con = Redshift.generate_connection(
        database="test",
        host=redshift_parameters.get("RedshiftAddress"),
        port=redshift_parameters.get("RedshiftPort"),
        user="test",
        password=redshift_parameters.get("Password"),
    )
    path = f"s3://{bucket}/redshift-load/"
    session.pandas.to_redshift(
        dataframe=df,
        path=path,
        schema="public",
        table="test",
        connection=con,
        iam_role=redshift_parameters.get("RedshiftRole"),
        mode="overwrite",
        preserve_index=False,
    )
    cursor = con.cursor()
    cursor.execute("SELECT * from public.test")
    rows = cursor.fetchall()
    cursor.close()
    con.close()
    assert len(df.index) == len(rows)
    assert len(list(df.columns)) == len(list(rows[0]))
    for row in rows:
        if row[0] == 1:
            assert row[1] == Decimal((0, (1, 9, 9), -2))
            assert row[2] == Decimal((0, (1, 9, 9, 9, 9, 9), -5))
        elif row[1] == 2:
            assert row[1] is None
            assert row[2] is None
        elif row[2] == 3:
            assert row[1] == Decimal((0, (1, 9, 0), -2))
            assert row[2] == Decimal((0, (1, 9, 0, 0, 0, 0), -5))


def test_to_redshift_spark_decimal(session, bucket, redshift_parameters):
    df = session.spark_session.createDataFrame(pd.DataFrame({
        "id": [1, 2, 3],
        "decimal_2": [Decimal((0, (1, 9, 9), -2)), None, Decimal((0, (1, 9, 0), -2))],
        "decimal_5": [Decimal((0, (1, 9, 9, 9, 9, 9), -5)), None,
                      Decimal((0, (1, 9, 0, 0, 0, 0), -5))]
    }),
                                               schema="id INTEGER, decimal_2 DECIMAL(3,2), decimal_5 DECIMAL(6,5)")
    con = Redshift.generate_connection(
        database="test",
        host=redshift_parameters.get("RedshiftAddress"),
        port=redshift_parameters.get("RedshiftPort"),
        user="test",
        password=redshift_parameters.get("Password"),
    )
    path = f"s3://{bucket}/redshift-load2/"
    session.spark.to_redshift(
        dataframe=df,
        path=path,
        schema="public",
        table="test2",
        connection=con,
        iam_role=redshift_parameters.get("RedshiftRole"),
        mode="overwrite",
    )
    cursor = con.cursor()
    cursor.execute("SELECT * from public.test2")
    rows = cursor.fetchall()
    cursor.close()
    con.close()
    assert df.count() == len(rows)
    assert len(list(df.columns)) == len(list(rows[0]))
    for row in rows:
        if row[0] == 1:
            assert row[1] == Decimal((0, (1, 9, 9), -2))
            assert row[2] == Decimal((0, (1, 9, 9, 9, 9, 9), -5))
        elif row[1] == 2:
            assert row[1] is None
            assert row[2] is None
        elif row[2] == 3:
            assert row[1] == Decimal((0, (1, 9, 0), -2))
            assert row[2] == Decimal((0, (1, 9, 0, 0, 0, 0), -5))


def test_to_parquet(session, bucket, redshift_parameters):
    n: int = 1_000_000
    df = pd.DataFrame({"id": list((range(n))), "name": list(["foo" if i % 2 == 0 else "boo" for i in range(n)])})
    con = Redshift.generate_connection(
        database="test",
        host=redshift_parameters.get("RedshiftAddress"),
        port=redshift_parameters.get("RedshiftPort"),
        user="test",
        password=redshift_parameters.get("Password"),
    )
    path = f"s3://{bucket}/test_to_parquet/"
    session.pandas.to_redshift(
        dataframe=df,
        path=path,
        schema="public",
        table="test",
        connection=con,
        iam_role=redshift_parameters.get("RedshiftRole"),
        mode="overwrite",
        preserve_index=True,
    )
    path = f"s3://{bucket}/test_to_parquet2/"
    paths = session.redshift.to_parquet(sql="SELECT * FROM public.test",
                                        path=path,
                                        iam_role=redshift_parameters.get("RedshiftRole"),
                                        connection=con,
                                        partition_cols=["name"])
    assert len(paths) == 4


@pytest.mark.parametrize("sample_name", ["micro", "small", "nano"])
def test_read_sql_redshift_pandas(session, bucket, redshift_parameters, sample_name):
    if sample_name == "micro":
        dates = ["date"]
    elif sample_name == "small":
        dates = ["date"]
    else:
        dates = ["date", "time"]
    df = pd.read_csv(f"data_samples/{sample_name}.csv", parse_dates=dates, infer_datetime_format=True)
    df["date"] = df["date"].dt.date
    con = Redshift.generate_connection(
        database="test",
        host=redshift_parameters.get("RedshiftAddress"),
        port=redshift_parameters.get("RedshiftPort"),
        user="test",
        password=redshift_parameters.get("Password"),
    )
    path = f"s3://{bucket}/test_read_sql_redshift_pandas/"
    session.pandas.to_redshift(
        dataframe=df,
        path=path,
        schema="public",
        table="test",
        connection=con,
        iam_role=redshift_parameters.get("RedshiftRole"),
        mode="overwrite",
        preserve_index=True,
    )
    path2 = f"s3://{bucket}/test_read_sql_redshift_pandas2/"
    df2 = session.pandas.read_sql_redshift(sql="select * from public.test",
                                           iam_role=redshift_parameters.get("RedshiftRole"),
                                           connection=con,
                                           temp_s3_path=path2)
    assert len(df.index) == len(df2.index)
    assert len(df.columns) + 1 == len(df2.columns)


def test_read_sql_redshift_pandas2(session, bucket, redshift_parameters):
    n: int = 1_000_000
    df = pd.DataFrame({"id": list((range(n))), "val": list(["foo" if i % 2 == 0 else "boo" for i in range(n)])})
    con = Redshift.generate_connection(
        database="test",
        host=redshift_parameters.get("RedshiftAddress"),
        port=redshift_parameters.get("RedshiftPort"),
        user="test",
        password=redshift_parameters.get("Password"),
    )
    path = f"s3://{bucket}/test_read_sql_redshift_pandas2/"
    session.pandas.to_redshift(
        dataframe=df,
        path=path,
        schema="public",
        table="test",
        connection=con,
        iam_role=redshift_parameters.get("RedshiftRole"),
        mode="overwrite",
        preserve_index=True,
    )
    path2 = f"s3://{bucket}/test_read_sql_redshift_pandas22/"
    df2 = session.pandas.read_sql_redshift(sql="select * from public.test",
                                           iam_role=redshift_parameters.get("RedshiftRole"),
                                           connection=con,
                                           temp_s3_path=path2)
    wr.s3.delete_objects(path=f"s3://{bucket}/")
    assert len(df.index) == len(df2.index)
    assert len(df.columns) + 1 == len(df2.columns)


def test_to_redshift_pandas_upsert(session, bucket, redshift_parameters):
    wr.s3.delete_objects(path=f"s3://{bucket}/")
    con = Redshift.generate_connection(
        database="test",
        host=redshift_parameters.get("RedshiftAddress"),
        port=redshift_parameters.get("RedshiftPort"),
        user="test",
        password=redshift_parameters.get("Password"),
    )

    df = pd.DataFrame({"id": list((range(1_000))), "val": list(["foo" if i % 2 == 0 else "boo" for i in range(1_000)])})

    df3 = pd.DataFrame({
        "id": list((range(1_000, 1_500))),
        "val": list(["foo" if i % 2 == 0 else "boo" for i in range(500)])
    })

    for i in range(10):
        print(f"run: {i}")

        # CREATE
        path = f"s3://{bucket}/test_to_redshift_pandas_upsert/"
        session.pandas.to_redshift(dataframe=df,
                                   path=path,
                                   schema="public",
                                   table="test_upsert",
                                   connection=con,
                                   iam_role=redshift_parameters.get("RedshiftRole"),
                                   mode="overwrite",
                                   preserve_index=True,
                                   primary_keys=["id"])
        path = f"s3://{bucket}/test_to_redshift_pandas_upsert2/"
        df2 = session.pandas.read_sql_redshift(sql="select * from public.test_upsert",
                                               iam_role=redshift_parameters.get("RedshiftRole"),
                                               connection=con,
                                               temp_s3_path=path)
        assert len(df.index) == len(df2.index)
        assert len(df.columns) + 1 == len(df2.columns)

        # UPSERT
        path = f"s3://{bucket}/test_to_redshift_pandas_upsert3/"
        session.pandas.to_redshift(dataframe=df3,
                                   path=path,
                                   schema="public",
                                   table="test_upsert",
                                   connection=con,
                                   iam_role=redshift_parameters.get("RedshiftRole"),
                                   mode="upsert",
                                   preserve_index=True,
                                   primary_keys=["id"])
        path = f"s3://{bucket}/test_to_redshift_pandas_upsert4/"
        df4 = session.pandas.read_sql_redshift(sql="select * from public.test_upsert",
                                               iam_role=redshift_parameters.get("RedshiftRole"),
                                               connection=con,
                                               temp_s3_path=path)
        assert len(df.index) + len(df3.index) == len(df4.index)
        assert len(df.columns) + 1 == len(df2.columns)

    wr.s3.delete_objects(path=f"s3://{bucket}/")
    con.close()
