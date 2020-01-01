from time import sleep
import logging
import csv
from datetime import datetime, date
from decimal import Decimal
import warnings

import pytest
import boto3
import pandas as pd
import numpy as np

import awswrangler as wr
from awswrangler import Session, Pandas, Aurora
from awswrangler.exceptions import LineTerminatorNotFound, EmptyDataframe, InvalidSerDe, UndetectedType

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
def bucket(session, cloudformation_outputs):
    if "BucketName" in cloudformation_outputs:
        bucket = cloudformation_outputs["BucketName"]
        session.s3.delete_objects(path=f"s3://{bucket}/")
    else:
        raise Exception("You must deploy the test infrastructure using Cloudformation!")
    yield bucket
    # session.s3.delete_objects(path=f"s3://{bucket}/")


@pytest.fixture(scope="module")
def database(cloudformation_outputs):
    if "GlueDatabaseName" in cloudformation_outputs:
        database = cloudformation_outputs["GlueDatabaseName"]
    else:
        raise Exception("You must deploy the test infrastructure using Cloudformation!")
    yield database


@pytest.fixture(scope="module")
def session(database):
    yield Session(athena_database=database)


@pytest.fixture(scope="module")
def kms_key(cloudformation_outputs):
    if "KmsKeyArn" in cloudformation_outputs:
        database = cloudformation_outputs["KmsKeyArn"]
    else:
        raise Exception("You must deploy the test infrastructure using Cloudformation!")
    yield database


@pytest.fixture(scope="module")
def loggroup(cloudformation_outputs):
    if "LogGroupName" in cloudformation_outputs:
        database = cloudformation_outputs["LogGroupName"]
    else:
        raise Exception("You must deploy the test infrastructure using Cloudformation!")
    yield database


@pytest.fixture(scope="module")
def logstream(cloudformation_outputs, loggroup):
    if "LogStream" in cloudformation_outputs:
        logstream = cloudformation_outputs["LogStream"]
    else:
        raise Exception("You must deploy the test infrastructure using Cloudformation!")
    client = boto3.client("logs")
    response = client.describe_log_streams(logGroupName=loggroup, logStreamNamePrefix=logstream)
    token = response["logStreams"][0].get("uploadSequenceToken")
    events = []
    for i in range(5):
        events.append({"timestamp": int(1000 * datetime.utcnow().timestamp()), "message": str(i)})
    args = {"logGroupName": loggroup, "logStreamName": logstream, "logEvents": events}
    if token:
        args["sequenceToken"] = token
    client.put_log_events(**args)
    yield logstream


@pytest.fixture(scope="module")
def postgres_parameters(cloudformation_outputs):
    postgres_parameters = {}
    if "PostgresAddress" in cloudformation_outputs:
        postgres_parameters["PostgresAddress"] = cloudformation_outputs.get("PostgresAddress")
    else:
        raise Exception("You must deploy the test infrastructure using SAM!")
    if "Password" in cloudformation_outputs:
        postgres_parameters["Password"] = cloudformation_outputs.get("Password")
    else:
        raise Exception("You must deploy the test infrastructure using SAM!")
    conn = Aurora.generate_connection(database="postgres",
                                      host=postgres_parameters["PostgresAddress"],
                                      port=3306,
                                      user="test",
                                      password=postgres_parameters["Password"],
                                      engine="postgres")
    with conn.cursor() as cursor:
        sql = "CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE"
        cursor.execute(sql)
    conn.commit()
    conn.close()
    yield postgres_parameters


@pytest.fixture(scope="module")
def mysql_parameters(cloudformation_outputs):
    mysql_parameters = {}
    if "MysqlAddress" in cloudformation_outputs:
        mysql_parameters["MysqlAddress"] = cloudformation_outputs.get("MysqlAddress")
    else:
        raise Exception("You must deploy the test infrastructure using SAM!")
    if "Password" in cloudformation_outputs:
        mysql_parameters["Password"] = cloudformation_outputs.get("Password")
    else:
        raise Exception("You must deploy the test infrastructure using SAM!")
    conn = Aurora.generate_connection(database="mysql",
                                      host=mysql_parameters["MysqlAddress"],
                                      port=3306,
                                      user="test",
                                      password=mysql_parameters["Password"],
                                      engine="mysql")
    with conn.cursor() as cursor:
        sql = "CREATE DATABASE IF NOT EXISTS test"
        with warnings.catch_warnings():
            warnings.filterwarnings(action="ignore", message=".*database exists.*")
            cursor.execute(sql)
    conn.commit()
    conn.close()
    yield mysql_parameters


@pytest.mark.parametrize("sample, row_num", [("data_samples/micro.csv", 30), ("data_samples/small.csv", 100)])
def test_read_csv(session, bucket, sample, row_num):
    boto3.client("s3").upload_file(sample, bucket, sample)
    path = f"s3://{bucket}/{sample}"
    dataframe = session.pandas.read_csv(path=path)
    session.s3.delete_objects(path=path)
    assert len(dataframe.index) == row_num


@pytest.mark.parametrize("sample, row_num", [("data_samples/micro.csv", 30), ("data_samples/small.csv", 100)])
def test_read_csv_iterator(session, bucket, sample, row_num):
    boto3.client("s3").upload_file(sample, bucket, sample)
    path = f"s3://{bucket}/{sample}"
    dataframe_iter = session.pandas.read_csv(path=path, max_result_size=200)
    total_count = 0
    for dataframe in dataframe_iter:
        total_count += len(dataframe.index)
    session.s3.delete_objects(path=path)
    assert total_count == row_num


@pytest.mark.parametrize("sample, row_num", [("data_samples/micro.csv", 30), ("data_samples/small.csv", 100)])
def test_read_csv_usecols(session, bucket, sample, row_num):
    boto3.client("s3").upload_file(sample, bucket, sample)
    path = f"s3://{bucket}/{sample}"
    dataframe = session.pandas.read_csv(path=path, usecols=["id", "name"])
    session.s3.delete_objects(path=path)
    assert len(dataframe.index) == row_num
    assert len(dataframe.columns) == 2


@pytest.mark.parametrize("sample, row_num", [("data_samples/micro.csv", 30), ("data_samples/small.csv", 100)])
def test_read_csv_iterator_usecols(session, bucket, sample, row_num):
    boto3.client("s3").upload_file(sample, bucket, sample)
    path = f"s3://{bucket}/{sample}"
    dataframe_iter = session.pandas.read_csv(path=path, usecols=[0, 1], max_result_size=200)
    total_count = 0
    for dataframe in dataframe_iter:
        total_count += len(dataframe.index)
        assert len(dataframe.columns) == 2
    session.s3.delete_objects(path=path)
    assert total_count == row_num


def test_read_csv_thousands_and_decimal(session, bucket):
    text = "col1;col2\n1.000.000,00;2.000.000,00\n3.000.000,00;4.000.000,00"
    filename = "test_read_csv_thousands_and_decimal/sample.txt"
    boto3.resource("s3").Object(bucket, filename).put(Body=text)
    path = f"s3://{bucket}/{filename}"
    df = session.pandas.read_csv(path=path, sep=";", thousands=".", decimal=",")
    assert len(df.index) == 2
    assert len(df.columns) == 2
    assert df.iloc[0].col1 == 1_000_000
    assert df.iloc[0].col2 == 2_000_000
    assert df.iloc[1].col1 == 3_000_000
    assert df.iloc[1].col2 == 4_000_000


@pytest.mark.parametrize(
    "mode, file_format, preserve_index, partition_cols, procs_cpu_bound, factor",
    [
        ("overwrite", "csv", False, [], 1, 1),
        ("append", "csv", False, [], 1, 2),
        ("overwrite_partitions", "csv", False, [], 1, 1),
        ("overwrite", "csv", True, [], 1, 1),
        ("append", "csv", True, [], 1, 2),
        ("overwrite_partitions", "csv", True, [], 1, 1),
        ("overwrite", "csv", False, [], 5, 1),
        ("append", "csv", False, [], 5, 2),
        ("overwrite_partitions", "csv", False, [], 5, 1),
        ("overwrite", "csv", True, [], 5, 1),
        ("append", "csv", True, [], 5, 2),
        ("overwrite_partitions", "csv", True, [], 5, 1),
        ("overwrite", "csv", False, ["date"], 1, 1),
        ("append", "csv", False, ["date"], 1, 2),
        ("overwrite_partitions", "csv", False, ["date"], 1, 1),
        ("overwrite", "csv", True, ["date"], 1, 1),
        ("append", "csv", True, ["date"], 1, 2),
        ("overwrite_partitions", "csv", True, ["date"], 1, 1),
        ("overwrite", "csv", False, ["date"], 5, 1),
        ("append", "csv", False, ["date"], 5, 2),
        ("overwrite_partitions", "csv", False, ["date"], 5, 1),
        ("overwrite", "csv", True, ["date"], 5, 1),
        ("append", "csv", True, ["date"], 5, 2),
        ("overwrite_partitions", "csv", True, ["date"], 5, 1),
        ("overwrite", "csv", False, ["name", "date"], 1, 1),
        ("append", "csv", False, ["name", "date"], 1, 2),
        ("overwrite_partitions", "csv", False, ["name", "date"], 1, 1),
        ("overwrite", "csv", True, ["name", "date"], 1, 1),
        ("append", "csv", True, ["name", "date"], 1, 2),
        ("overwrite_partitions", "csv", True, ["name", "date"], 1, 1),
        ("overwrite", "csv", False, ["name", "date"], 5, 1),
        ("append", "csv", False, ["name", "date"], 5, 2),
        ("overwrite_partitions", "csv", False, ["name", "date"], 5, 1),
        ("overwrite", "csv", True, ["name", "date"], 5, 1),
        ("append", "csv", True, ["name", "date"], 5, 2),
        ("overwrite_partitions", "csv", True, ["name", "date"], 2, 1),
        ("overwrite", "parquet", False, [], 1, 1),
        ("append", "parquet", False, [], 1, 2),
        ("overwrite_partitions", "parquet", False, [], 1, 1),
        ("overwrite", "parquet", True, [], 1, 1),
        ("append", "parquet", True, [], 1, 2),
        ("overwrite_partitions", "parquet", True, [], 1, 1),
        ("overwrite", "parquet", False, [], 5, 1),
        ("append", "parquet", False, [], 5, 2),
        ("overwrite_partitions", "parquet", False, [], 5, 1),
        ("overwrite", "parquet", True, [], 5, 1),
        ("append", "parquet", True, [], 5, 2),
        ("overwrite_partitions", "parquet", True, [], 5, 1),
        ("overwrite", "parquet", False, ["date"], 1, 1),
        ("append", "parquet", False, ["date"], 1, 2),
        ("overwrite_partitions", "parquet", False, ["date"], 1, 1),
        ("overwrite", "parquet", True, ["date"], 1, 1),
        ("append", "parquet", True, ["date"], 1, 2),
        ("overwrite_partitions", "parquet", True, ["date"], 1, 1),
        ("overwrite", "parquet", False, ["date"], 5, 1),
        ("append", "parquet", False, ["date"], 5, 2),
        ("overwrite_partitions", "parquet", False, ["date"], 5, 1),
        ("overwrite", "parquet", True, ["date"], 5, 1),
        ("append", "parquet", True, ["date"], 5, 2),
        ("overwrite_partitions", "parquet", True, ["date"], 5, 1),
        ("overwrite", "parquet", False, ["name", "date"], 1, 1),
        ("append", "parquet", False, ["name", "date"], 1, 2),
        ("overwrite_partitions", "parquet", False, ["name", "date"], 1, 1),
        ("overwrite", "parquet", True, ["name", "date"], 1, 1),
        ("append", "parquet", True, ["name", "date"], 1, 2),
        ("overwrite_partitions", "parquet", True, ["name", "date"], 1, 1),
        ("overwrite", "parquet", False, ["name", "date"], 5, 1),
        ("append", "parquet", False, ["name", "date"], 5, 2),
        ("overwrite_partitions", "parquet", False, ["name", "date"], 5, 1),
        ("overwrite", "parquet", True, ["name", "date"], 5, 1),
        ("append", "parquet", True, ["name", "date"], 5, 2),
        ("overwrite_partitions", "parquet", True, ["name", "date"], 5, 1),
    ],
)
def test_to_s3(
    session,
    bucket,
    database,
    mode,
    file_format,
    preserve_index,
    partition_cols,
    procs_cpu_bound,
    factor,
):
    dataframe = pd.read_csv("data_samples/micro.csv")
    func = session.pandas.to_csv if file_format == "csv" else session.pandas.to_parquet
    path = f"s3://{bucket}/test/"
    objects_paths = func(
        dataframe=dataframe,
        database=database,
        path=path,
        preserve_index=preserve_index,
        mode=mode,
        partition_cols=partition_cols,
        procs_cpu_bound=procs_cpu_bound,
    )
    num_partitions = (len([keys for keys in dataframe.groupby(partition_cols)]) if partition_cols else 1)
    assert len(objects_paths) >= num_partitions
    dataframe2 = None
    for counter in range(10):
        sleep(1)
        dataframe2 = session.pandas.read_sql_athena(ctas_approach=False, sql="select * from test", database=database)
        if factor * len(dataframe.index) == len(dataframe2.index):
            break
    assert factor * len(dataframe.index) == len(dataframe2.index)
    if preserve_index:
        assert (len(list(dataframe.columns)) + 1) == len(list(dataframe2.columns))
    else:
        assert len(list(dataframe.columns)) == len(list(dataframe2.columns))


def test_to_parquet_with_cast_int(
    session,
    bucket,
    database,
):
    dataframe = pd.read_csv("data_samples/nano.csv", dtype={"id": "Int64"}, parse_dates=["date", "time"])
    path = f"s3://{bucket}/test/"
    session.pandas.to_parquet(dataframe=dataframe,
                              database=database,
                              path=path,
                              preserve_index=False,
                              mode="overwrite",
                              procs_cpu_bound=1,
                              cast_columns={"value": "int"})
    dataframe2 = None
    for counter in range(10):
        sleep(1)
        dataframe2 = session.pandas.read_sql_athena(ctas_approach=False, sql="select * from test", database=database)
        if len(dataframe.index) == len(dataframe2.index):
            break
    session.s3.delete_objects(path=path)
    assert len(dataframe.index) == len(dataframe2.index)
    assert len(list(dataframe.columns)) == len(list(dataframe2.columns))
    assert dataframe[dataframe["id"] == 0].iloc[0]["name"] == dataframe2[dataframe2["id"] == 0].iloc[0]["name"]


@pytest.mark.parametrize("sample, row_num, max_result_size", [
    ("data_samples/nano.csv", 5, 5000),
    ("data_samples/micro.csv", 30, 100),
    ("data_samples/small.csv", 100, 100),
    ("data_samples/micro.csv", 30, 500),
    ("data_samples/small.csv", 100, 500),
    ("data_samples/micro.csv", 30, 3000),
    ("data_samples/small.csv", 100, 3000),
    ("data_samples/micro.csv", 30, 700),
])
def test_read_sql_athena_iterator(session, bucket, database, sample, row_num, max_result_size):
    parse_dates = []
    if sample == "data_samples/nano.csv":
        parse_dates.append("time")
        parse_dates.append("date")
    dataframe_sample = pd.read_csv(sample, parse_dates=parse_dates)
    path = f"s3://{bucket}/test/"
    session.pandas.to_parquet(dataframe=dataframe_sample,
                              database=database,
                              path=path,
                              preserve_index=False,
                              mode="overwrite")
    total_count = 0
    for counter in range(10):
        sleep(1)
        dataframe_iter = session.pandas.read_sql_athena(ctas_approach=False,
                                                        sql="select * from test",
                                                        database=database,
                                                        max_result_size=max_result_size)
        total_count = 0
        for dataframe in dataframe_iter:
            total_count += len(dataframe.index)
            assert len(list(dataframe.columns)) == len(list(dataframe_sample.columns))
        if total_count == row_num:
            break
    session.s3.delete_objects(path=path)
    assert total_count == row_num


@pytest.mark.parametrize("body, sep, quotechar, lineterminator, last_index, last_terminator_suspect_index,"
                         "first_non_special_byte_index, sep_counter, quote_counter", [
                             (b'"foo","boo"\n', ",", '"', "\n", None, 11, 9, 0, 1),
                             (b'"foo","boo"\n"bar', ",", '"', "\n", None, 11, 9, 0, 1),
                             (b'!foo!;!boo!@', ";", '!', "@", None, 11, 9, 0, 1),
                             (b'"foo","boo"\n"bar\n', ",", '"', "\n", 16, 11, 9, 0, 1),
                         ])
def test_extract_terminator_profile(body, sep, quotechar, lineterminator, last_index, last_terminator_suspect_index,
                                    first_non_special_byte_index, sep_counter, quote_counter):
    profile = Pandas._extract_terminator_profile(body=body,
                                                 sep=sep,
                                                 quotechar=quotechar,
                                                 lineterminator=lineterminator,
                                                 last_index=last_index)
    assert profile["last_terminator_suspect_index"] == last_terminator_suspect_index
    assert profile["first_non_special_byte_index"] == first_non_special_byte_index
    assert profile["sep_counter"] == sep_counter
    assert profile["quote_counter"] == quote_counter


@pytest.mark.parametrize("body, sep, quoting, quotechar, lineterminator, ret", [
    (b"012\njawdnkjawnd", ",", csv.QUOTE_MINIMAL, '"', "\n", 3),
    (b"012\n456\njawdnkjawnd", ",", csv.QUOTE_MINIMAL, '"', "\n", 7),
    (b'012",\n"foo', ",", csv.QUOTE_ALL, '"', "\n", 5),
    (b'012",\n', ",", csv.QUOTE_ALL, '"', "\n", 5),
    (b'012",\n"012,\n', ",", csv.QUOTE_ALL, '"', "\n", 5),
    (b'012",\n,,,,,,,,"012,\n', ",", csv.QUOTE_ALL, '"', "\n", 5),
    (b'012",,,,\n"012,\n', ",", csv.QUOTE_ALL, '"', "\n", 8),
    (b'012",,,,\n,,,,,,""012,\n', ",", csv.QUOTE_ALL, '"', "\n", 8),
    (b'012",,,,\n,,,,,,""012"\n,', ",", csv.QUOTE_ALL, '"', "\n", 21),
    (b'012",,,,\n,,,,,,""01"2""\n,"a', ",", csv.QUOTE_ALL, '"', "\n", 8),
    (b'"foo","boo"\n"\n","bar"', ",", csv.QUOTE_ALL, '"', "\n", 11),
    (b'"foo"\n"boo","\n","\n","\n","\n","\n",,,,,,"\n",,,,', ",", csv.QUOTE_ALL, '"', "\n", 5),
    (b'012",\n"foo","\n\n\n\n","\n', ",", csv.QUOTE_ALL, '"', "\n", 5),
])
def test_find_terminator(body, sep, quoting, quotechar, lineterminator, ret):
    assert Pandas._find_terminator(body=body,
                                   sep=sep,
                                   quoting=quoting,
                                   quotechar=quotechar,
                                   lineterminator=lineterminator) == ret


@pytest.mark.parametrize("body, sep, quoting, quotechar, lineterminator",
                         [(b"jawdnkjawnd", ",", csv.QUOTE_MINIMAL, '"', "\n"),
                          (b"jawdnkjawnd", ",", csv.QUOTE_ALL, '"', "\n"),
                          (b"jawdnkj\nawnd", ",", csv.QUOTE_ALL, '"', "\n"),
                          (b'jawdnkj"x\n\n"awnd', ",", csv.QUOTE_ALL, '"', "\n"),
                          (b'jawdnkj""\n,,,,,,,,,,awnd', ",", csv.QUOTE_ALL, '"', "\n"),
                          (b'jawdnkj,""""""\nawnd', ",", csv.QUOTE_ALL, '"', "\n")])
def test_find_terminator_exception(body, sep, quoting, quotechar, lineterminator):
    with pytest.raises(LineTerminatorNotFound):
        assert Pandas._find_terminator(body=body,
                                       sep=sep,
                                       quoting=quoting,
                                       quotechar=quotechar,
                                       lineterminator=lineterminator)


@pytest.mark.parametrize("max_result_size", [400, 700, 1000, 10000])
def test_etl_complex(session, bucket, database, max_result_size):
    dataframe = pd.read_csv("data_samples/complex.csv",
                            dtype={"my_int_with_null": "Int64"},
                            parse_dates=["my_timestamp", "my_date"])
    path = f"s3://{bucket}/test/"
    session.pandas.to_parquet(dataframe=dataframe,
                              database=database,
                              path=path,
                              preserve_index=False,
                              mode="overwrite",
                              procs_cpu_bound=1)
    sleep(1)
    df_iter = session.pandas.read_sql_athena(ctas_approach=False,
                                             sql="select * from test",
                                             database=database,
                                             max_result_size=max_result_size)
    count = 0
    for df in df_iter:
        count += len(df.index)
        for row in df.itertuples():
            assert len(list(dataframe.columns)) == len(list(df.columns))
            assert isinstance(row.my_timestamp, datetime)
            assert isinstance(row.my_date, date)
            assert isinstance(row.my_float, float)
            assert isinstance(row.my_int, np.int64)
            assert isinstance(row.my_string, str)
            assert str(row.my_timestamp) == "2018-01-01 04:03:02.001000"
            assert str(row.my_date) == "2019-02-02 00:00:00"
            assert str(row.my_float) == "12345.6789"
            assert str(row.my_int) == "123456789"
            assert str(
                row.my_string
            ) == "foo\nboo\nbar\nFOO\nBOO\nBAR\nxxxxx\nÁÃÀÂÇ\n汉字汉字汉字汉字汉字汉字汉字æøåæøåæøåæøåæøåæøåæøåæøåæøåæøå汉字汉字汉字汉字汉字汉字汉字æøåæøåæøåæøåæøåæøåæøåæøåæøåæøå"
    session.s3.delete_objects(path=path)
    assert count == len(dataframe.index)


def test_etl_complex_ctas(session, bucket, database):
    dataframe = pd.read_csv("data_samples/complex.csv",
                            dtype={"my_int_with_null": "Int64"},
                            parse_dates=["my_timestamp", "my_date"])
    path = f"s3://{bucket}/test/"
    session.pandas.to_parquet(dataframe=dataframe,
                              database=database,
                              path=path,
                              preserve_index=False,
                              mode="overwrite",
                              procs_cpu_bound=1)
    sleep(1)
    df = session.pandas.read_sql_athena(ctas_approach=True, sql="select * from test", database=database)
    for row in df.itertuples():
        assert isinstance(row.my_timestamp, datetime)
        assert isinstance(row.my_date, date)
        assert isinstance(row.my_float, float)
        assert isinstance(row.my_int, int)
        assert isinstance(row.my_string, str)
        assert str(row.my_int_with_null) in ("1", "nan")
        assert str(row.my_timestamp) == "2018-01-01 04:03:02.001000"
        assert str(row.my_date) == "2019-02-02 00:00:00"
        assert str(row.my_float) == "12345.6789"
        assert row.my_int == 123456789
        assert str(
            row.my_string
        ) == "foo\nboo\nbar\nFOO\nBOO\nBAR\nxxxxx\nÁÃÀÂÇ\n汉字汉字汉字汉字汉字汉字汉字æøåæøåæøåæøåæøåæøåæøåæøåæøåæøå汉字汉字汉字汉字汉字汉字汉字æøåæøåæøåæøåæøåæøåæøåæøåæøåæøå"
    session.s3.delete_objects(path=path)
    assert len(list(dataframe.columns)) == len(list(df.columns))
    assert len(df.index) == len(dataframe.index)


def test_to_parquet_with_kms(
    bucket,
    database,
    kms_key,
):
    extra_args = {"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": kms_key}
    session_inner = Session(s3_additional_kwargs=extra_args)
    dataframe = pd.read_csv("data_samples/nano.csv")
    path = f"s3://{bucket}/test/"
    session_inner.pandas.to_parquet(dataframe=dataframe,
                                    database=database,
                                    path=path,
                                    preserve_index=False,
                                    mode="overwrite",
                                    procs_cpu_bound=1)
    dataframe2 = None
    for counter in range(10):
        sleep(1)
        dataframe2 = session_inner.pandas.read_sql_athena(ctas_approach=False,
                                                          sql="select * from test",
                                                          database=database)
        if len(dataframe.index) == len(dataframe2.index):
            break
    session_inner.s3.delete_objects(path=path)
    assert len(dataframe.index) == len(dataframe2.index)
    assert len(list(dataframe.columns)) == len(list(dataframe2.columns))
    assert dataframe[dataframe["id"] == 0].iloc[0]["name"] == dataframe2[dataframe2["id"] == 0].iloc[0]["name"]


def test_to_parquet_with_empty_dataframe(session, bucket, database):
    dataframe = pd.DataFrame()
    with pytest.raises(EmptyDataframe):
        assert session.pandas.to_parquet(dataframe=dataframe,
                                         database=database,
                                         path=f"s3://{bucket}/test/",
                                         preserve_index=False,
                                         mode="overwrite",
                                         procs_cpu_bound=1)


def test_read_log_query(session, loggroup):
    dataframe = session.pandas.read_log_query(
        log_group_names=[loggroup],
        query="fields @timestamp, @message | sort @timestamp desc | limit 5",
    )
    assert len(dataframe.index) == 5
    assert len(dataframe.columns) == 3


@pytest.mark.parametrize("file_format, serde, index, partition_cols",
                         [("csv", "OpenCSVSerDe", None, []), ("csv", "OpenCSVSerDe", "default", []),
                          ("csv", "OpenCSVSerDe", "my_date", []), ("csv", "OpenCSVSerDe", "my_timestamp", []),
                          ("csv", "OpenCSVSerDe", "my_timestamp", []),
                          ("csv", "LazySimpleSerDe", "my_date", ["my_timestamp", "my_float"]),
                          ("csv", "LazySimpleSerDe", None, []), ("csv", "LazySimpleSerDe", "default", []),
                          ("csv", "LazySimpleSerDe", "my_date", []), ("csv", "LazySimpleSerDe", "my_timestamp", []),
                          ("csv", "LazySimpleSerDe", "my_timestamp", ["my_date", "my_int"]),
                          ("parquet", None, None, []), ("parquet", None, "default", []),
                          ("parquet", None, "my_date", []), ("parquet", None, "my_timestamp", []),
                          ("parquet", None, None, ["my_int"]), ("parquet", None, "default", ["my_int"]),
                          ("parquet", None, "my_date", ["my_int"]), ("parquet", None, "my_timestamp", ["my_int"]),
                          ("parquet", None, None, ["my_float"]), ("parquet", None, "default", ["my_float"]),
                          ("parquet", None, "my_date", ["my_float"]), ("parquet", None, "my_timestamp", ["my_float"]),
                          ("parquet", None, None, ["my_date"]), ("parquet", None, "default", ["my_date"]),
                          ("parquet", None, "my_date", ["my_date"]), ("parquet", None, "my_timestamp", ["my_date"]),
                          ("parquet", None, None, ["my_timestamp"]), ("parquet", None, "default", ["my_timestamp"]),
                          ("parquet", None, "my_date", ["my_timestamp"]),
                          ("parquet", None, "my_timestamp", ["my_timestamp"]),
                          ("parquet", None, None, ["my_timestamp", "my_date"]),
                          ("parquet", None, "default", ["my_date", "my_timestamp"]),
                          ("parquet", None, "my_date", ["my_timestamp", "my_date"]),
                          ("parquet", None, "my_timestamp", ["my_date", "my_timestamp"]),
                          ("parquet", None, "default", ["my_date", "my_timestamp", "my_int"]),
                          ("parquet", None, "my_date", ["my_timestamp", "my_float", "my_date"])])
def test_to_s3_types(session, bucket, database, file_format, serde, index, partition_cols):
    dataframe = pd.read_csv("data_samples/complex.csv",
                            dtype={"my_int_with_null": "Int64"},
                            parse_dates=["my_timestamp", "my_date"])
    dataframe["my_date"] = dataframe["my_date"].dt.date
    dataframe["my_bool"] = True

    preserve_index = True
    if not index:
        preserve_index = False
    elif index != "default":
        dataframe["new_index"] = dataframe[index]
        dataframe = dataframe.set_index("new_index")

    args = {
        "dataframe": dataframe,
        "database": database,
        "path": f"s3://{bucket}/test/",
        "preserve_index": preserve_index,
        "mode": "overwrite",
        "procs_cpu_bound": 1,
        "partition_cols": partition_cols
    }

    if file_format == "csv":
        func = session.pandas.to_csv
        args["serde"] = serde
        del dataframe["my_string"]
    else:
        func = session.pandas.to_parquet
    objects_paths = func(**args)
    assert len(objects_paths) == 1
    sleep(2)
    dataframe2 = session.pandas.read_sql_athena(ctas_approach=False, sql="select * from test", database=database)
    for row in dataframe2.itertuples():

        if file_format == "parquet":
            if index:
                if index == "my_date":
                    assert isinstance(row.new_index, date)
                elif index == "my_timestamp":
                    assert isinstance(row.new_index, datetime)
            assert isinstance(row.my_timestamp, datetime)
            assert type(row.my_date) == date
            assert isinstance(row.my_float, float)
            assert isinstance(row.my_int, np.int64)
            assert isinstance(row.my_string, str)
            assert isinstance(row.my_bool, bool)
            assert str(
                row.my_string
            ) == "foo\nboo\nbar\nFOO\nBOO\nBAR\nxxxxx\nÁÃÀÂÇ\n汉字汉字汉字汉字汉字汉字汉字æøåæøåæøåæøåæøåæøåæøåæøåæøåæøå汉字汉字汉字汉字汉字汉字汉字æøåæøåæøåæøåæøåæøåæøåæøåæøåæøå"
        elif file_format == "csv":
            if serde == "LazySimpleSerDe":
                assert isinstance(row.my_float, float)
                assert isinstance(row.my_int, np.int64)
        assert str(row.my_timestamp).startswith("2018-01-01 04:03:02.001")
        assert str(row.my_date) == "2019-02-02"
        assert str(row.my_float) == "12345.6789"
        assert str(row.my_int) == "123456789"
        assert str(row.my_bool) == "True"

    assert len(dataframe.index) == len(dataframe2.index)
    if index:
        assert (len(list(dataframe.columns)) + 1) == len(list(dataframe2.columns))
    else:
        assert len(list(dataframe.columns)) == len(list(dataframe2.columns))


def test_to_csv_with_sep(
    session,
    bucket,
    database,
):
    dataframe = pd.read_csv("data_samples/nano.csv")
    session.pandas.to_csv(dataframe=dataframe,
                          database=database,
                          path=f"s3://{bucket}/test/",
                          preserve_index=False,
                          mode="overwrite",
                          sep="|")
    dataframe2 = None
    for counter in range(10):
        sleep(1)
        dataframe2 = session.pandas.read_sql_athena(ctas_approach=False, sql="select * from test", database=database)
        if len(dataframe.index) == len(dataframe2.index):
            break
    assert len(dataframe.index) == len(dataframe2.index)
    assert len(list(dataframe.columns)) == len(list(dataframe2.columns))


def test_to_csv_serde_exception(
    session,
    bucket,
    database,
):
    dataframe = pd.read_csv("data_samples/nano.csv")
    with pytest.raises(InvalidSerDe):
        assert session.pandas.to_csv(dataframe=dataframe,
                                     database=database,
                                     path=f"s3://{bucket}/test/",
                                     preserve_index=False,
                                     mode="overwrite",
                                     serde="foo")


@pytest.mark.parametrize("compression", [None, "snappy", "gzip"])
def test_to_parquet_compressed(session, bucket, database, compression):
    dataframe = pd.read_csv("data_samples/small.csv")
    session.pandas.to_parquet(dataframe=dataframe,
                              database=database,
                              path=f"s3://{bucket}/test/",
                              preserve_index=False,
                              mode="overwrite",
                              compression=compression,
                              procs_cpu_bound=1)
    dataframe2 = None
    for counter in range(10):
        sleep(1)
        dataframe2 = session.pandas.read_sql_athena(ctas_approach=False, sql="select * from test", database=database)
        if len(dataframe.index) == len(dataframe2.index):
            break
    assert len(dataframe.index) == len(dataframe2.index)
    assert len(list(dataframe.columns)) == len(list(dataframe2.columns))
    assert dataframe[dataframe["id"] == 1].iloc[0]["name"] == dataframe2[dataframe2["id"] == 1].iloc[0]["name"]


def test_to_parquet_lists(session, bucket, database):
    dataframe = pd.DataFrame({
        "id": [0, 1],
        "col_int": [[1, 2], [3, 4, 5]],
        "col_float": [[1.0, 2.0, 3.0], [4.0, 5.0]],
        "col_string": [["foo"], ["boo", "bar"]],
        "col_timestamp": [[datetime(2019, 1, 1), datetime(2019, 1, 2)], [datetime(2019, 1, 3)]],
        "col_date": [[date(2019, 1, 1), date(2019, 1, 2)], [date(2019, 1, 3)]],
        "col_list_int": [[[1]], [[2, 3], [4, 5, 6]]],
        "col_list_list_string": [[[["foo"]]], [[["boo", "bar"]]]],
    })
    paths = session.pandas.to_parquet(dataframe=dataframe,
                                      database=database,
                                      path=f"s3://{bucket}/test/",
                                      preserve_index=False,
                                      mode="overwrite",
                                      procs_cpu_bound=1)
    assert len(paths) == 1
    dataframe2 = None
    for counter in range(10):
        sleep(1)
        dataframe2 = session.pandas.read_sql_athena(ctas_approach=False,
                                                    sql="select id, col_int, col_float, col_list_int from test",
                                                    database=database)
        if len(dataframe.index) == len(dataframe2.index):
            break
    assert len(dataframe.index) == len(dataframe2.index)
    assert 4 == len(list(dataframe2.columns))
    val = dataframe[dataframe["id"] == 0].iloc[0]["col_list_int"]
    val2 = dataframe2[dataframe2["id"] == 0].iloc[0]["col_list_int"]
    assert val == val2


def test_to_parquet_with_cast_null(
    session,
    bucket,
    database,
):
    dataframe = pd.DataFrame({
        "id": [0, 1],
        "col_null_tinyint": [None, None],
        "col_null_smallint": [None, None],
        "col_null_int": [None, None],
        "col_null_bigint": [None, None],
        "col_null_float": [None, None],
        "col_null_double": [None, None],
        "col_null_string": [None, None],
        "col_null_date": [None, None],
        "col_null_timestamp": [None, None],
    })
    session.pandas.to_parquet(dataframe=dataframe,
                              database=database,
                              path=f"s3://{bucket}/test/",
                              preserve_index=False,
                              mode="overwrite",
                              procs_cpu_bound=1,
                              cast_columns={
                                  "col_null_tinyint": "tinyint",
                                  "col_null_smallint": "smallint",
                                  "col_null_int": "int",
                                  "col_null_bigint": "bigint",
                                  "col_null_float": "float",
                                  "col_null_double": "double",
                                  "col_null_string": "string",
                                  "col_null_date": "date",
                                  "col_null_timestamp": "timestamp",
                              })
    dataframe2 = None
    for counter in range(10):
        sleep(1)
        dataframe2 = session.pandas.read_sql_athena(ctas_approach=False, sql="select * from test", database=database)
        if len(dataframe.index) == len(dataframe2.index):
            break
    assert len(dataframe.index) == len(dataframe2.index)
    assert len(list(dataframe.columns)) == len(list(dataframe2.columns))


def test_read_sql_athena_with_time_zone(session, database):
    query = "select current_timestamp as value, typeof(current_timestamp) as type"
    dataframe = session.pandas.read_sql_athena(ctas_approach=False, sql=query, database=database)
    assert len(dataframe.index) == 1
    assert len(dataframe.columns) == 2
    assert dataframe["type"][0] == "timestamp with time zone"
    assert dataframe["value"][0].year == datetime.utcnow().year


def test_normalize_columns_names_athena():
    dataframe = pd.DataFrame({
        "CamelCase": [1, 2, 3],
        "With Spaces": [4, 5, 6],
        "With-Dash": [7, 8, 9],
        "Ãccént": [10, 11, 12],
        "with.dot": [10, 11, 12],
        "Camel_Case2": [13, 14, 15],
        "Camel___Case3": [16, 17, 18]
    })
    Pandas.normalize_columns_names_athena(dataframe=dataframe, inplace=True)
    assert dataframe.columns[0] == "camel_case"
    assert dataframe.columns[1] == "with_spaces"
    assert dataframe.columns[2] == "with_dash"
    assert dataframe.columns[3] == "accent"
    assert dataframe.columns[4] == "with_dot"
    assert dataframe.columns[5] == "camel_case2"
    assert dataframe.columns[6] == "camel_case3"


def test_to_parquet_with_normalize(
    session,
    bucket,
    database,
):
    dataframe = pd.DataFrame({
        "CamelCase": [1, 2, 3],
        "With Spaces": [4, 5, 6],
        "With-Dash": [7, 8, 9],
        "Ãccént": [10, 11, 12],
        "with.dot": [10, 11, 12],
        "Camel_Case2": [13, 14, 15],
        "Camel___Case3": [16, 17, 18]
    })
    session.pandas.to_parquet(dataframe=dataframe,
                              database=database,
                              path=f"s3://{bucket}/TestTable-with.dot/",
                              mode="overwrite")
    dataframe2 = None
    for counter in range(10):
        sleep(1)
        dataframe2 = session.pandas.read_sql_athena(ctas_approach=False,
                                                    sql="select * from test_table_with_dot",
                                                    database=database)
        if len(dataframe.index) == len(dataframe2.index):
            break
    assert len(dataframe.index) == len(dataframe2.index)
    assert (len(list(dataframe.columns)) + 1) == len(list(dataframe2.columns))
    assert dataframe2.columns[0] == "camel_case"
    assert dataframe2.columns[1] == "with_spaces"
    assert dataframe2.columns[2] == "with_dash"
    assert dataframe2.columns[3] == "accent"
    assert dataframe2.columns[4] == "with_dot"
    assert dataframe2.columns[5] == "camel_case2"
    assert dataframe2.columns[6] == "camel_case3"


def test_to_parquet_with_normalize_and_cast(
    session,
    bucket,
    database,
):
    dataframe = pd.DataFrame({
        "CamelCase": [1, 2, 3],
        "With Spaces": [4, 5, 6],
        "With-Dash": [7, 8, 9],
        "Ãccént": [10, 11, 12],
        "with.dot": [10, 11, 12],
        "Camel_Case2": [13, 14, 15],
        "Camel___Case3": [16, 17, 18]
    })
    session.pandas.to_parquet(dataframe=dataframe,
                              database=database,
                              path=f"s3://{bucket}/TestTable-with.dot/",
                              mode="overwrite",
                              partition_cols=["CamelCase"],
                              cast_columns={
                                  "Camel_Case2": "double",
                                  "Camel___Case3": "float"
                              })
    dataframe2 = None
    for counter in range(10):
        sleep(1)
        dataframe2 = session.pandas.read_sql_athena(ctas_approach=False,
                                                    sql="select * from test_table_with_dot",
                                                    database=database)
        if len(dataframe.index) == len(dataframe2.index):
            break
    assert len(dataframe.index) == len(dataframe2.index)
    assert (len(list(dataframe.columns)) + 1) == len(list(dataframe2.columns))
    assert dataframe2.columns[0] == "with_spaces"
    assert dataframe2.columns[1] == "with_dash"
    assert dataframe2.columns[2] == "accent"
    assert dataframe2.columns[3] == "with_dot"
    assert dataframe2.columns[4] == "camel_case2"
    assert dataframe2.columns[5] == "camel_case3"
    assert dataframe2.columns[6] == "__index_level_0__"
    assert dataframe2.columns[7] == "camel_case"
    assert dataframe2[dataframe2.columns[4]].dtype == "float64"
    assert dataframe2[dataframe2.columns[5]].dtype == "float64"


def test_drop_duplicated_columns():
    dataframe = pd.DataFrame({
        "a": [1, 2, 3],
        "b": [4, 5, 6],
        "c": [7, 8, 9],
    })
    dataframe.columns = ["a", "a", "c"]
    dataframe = Pandas.drop_duplicated_columns(dataframe=dataframe)
    assert dataframe.columns[0] == "a"
    assert dataframe.columns[1] == "c"


def test_to_parquet_duplicated_columns(
    session,
    bucket,
    database,
):
    dataframe = pd.DataFrame({
        "a": [1, 2, 3],
        "b": [4, 5, 6],
        "c": [7, 8, 9],
    })
    dataframe.columns = ["a", "a", "c"]
    session.pandas.to_parquet(dataframe=dataframe, database=database, path=f"s3://{bucket}/test/", mode="overwrite")
    dataframe2 = None
    for counter in range(10):
        sleep(1)
        dataframe2 = session.pandas.read_sql_athena(ctas_approach=False, sql="select * from test", database=database)
        if len(dataframe.index) == len(dataframe2.index):
            break
    assert len(dataframe.index) == len(dataframe2.index)
    assert len(list(dataframe.columns)) == len(list(dataframe2.columns))
    assert dataframe2.columns[0] == "a"
    assert dataframe2.columns[1] == "c"


def test_to_parquet_with_pyarrow_null_type(
    session,
    bucket,
    database,
):
    dataframe = pd.DataFrame({
        "a": [1, 2, 3],
        "b": [4, 5, 6],
        "col_null": [None, None, None],
        "c": [7, 8, 9],
    })
    with pytest.raises(UndetectedType):
        assert session.pandas.to_parquet(dataframe=dataframe,
                                         database=database,
                                         path=f"s3://{bucket}/test/",
                                         mode="overwrite")


def test_to_parquet_casting_to_string(
    session,
    bucket,
    database,
):
    dataframe = pd.DataFrame({
        "a": [1, 2, 3],
        "col_string_null": [None, None, None],
        "c": [7, 8, 9],
    })
    session.pandas.to_parquet(dataframe=dataframe,
                              database=database,
                              path=f"s3://{bucket}/test/",
                              mode="overwrite",
                              cast_columns={"col_string_null": "string"})
    dataframe2 = None
    for counter in range(10):
        sleep(1)
        dataframe2 = session.pandas.read_sql_athena(ctas_approach=False, sql="select * from test", database=database)
        if len(dataframe.index) == len(dataframe2.index):
            break
    assert len(dataframe.index) == len(dataframe2.index)
    assert (len(list(dataframe.columns)) + 1) == len(list(dataframe2.columns))


def test_to_parquet_casting_with_null_object(
    session,
    bucket,
    database,
):
    dataframe = pd.DataFrame({
        "a": [1, 2, 3],
        "b": [4, 5, 6],
        "col_null": [None, None, None],
    })
    with pytest.raises(UndetectedType):
        assert session.pandas.to_parquet(dataframe=dataframe,
                                         database=database,
                                         path=f"s3://{bucket}/test/",
                                         mode="overwrite")


def test_read_sql_athena_with_nulls(session, bucket, database):
    df = pd.DataFrame({"col_int": [1, None, 3], "col_bool": [True, False, False], "col_bool_null": [True, None, False]})
    path = f"s3://{bucket}/test/"
    session.pandas.to_parquet(dataframe=df,
                              database=database,
                              path=path,
                              preserve_index=False,
                              mode="overwrite",
                              cast_columns={
                                  "col_int": "int",
                                  "col_bool_null": "boolean"
                              })
    df2 = None
    for counter in range(10):
        sleep(1)
        df2 = session.pandas.read_sql_athena(ctas_approach=False, sql="select * from test", database=database)
        assert len(list(df.columns)) == len(list(df2.columns))
        if len(df.index) == len(df2.index):
            break
    assert len(df.index) == len(df2.index)
    assert df2.dtypes[0] == "Int64"
    assert df2.dtypes[1] == "bool"
    assert df2.dtypes[2] == "bool"
    session.s3.delete_objects(path=path)


def test_partition_date(session, bucket, database):
    df = pd.DataFrame({
        "col1": ["val1", "val2"],
        "datecol": ["2019-11-09", "2019-11-08"],
        'partcol': ["2019-11-09", "2019-11-08"]
    })
    df["datecol"] = pd.to_datetime(df.datecol).dt.date
    df["partcol"] = pd.to_datetime(df.partcol).dt.date
    path = f"s3://{bucket}/test/"
    session.pandas.to_parquet(dataframe=df,
                              database=database,
                              path=path,
                              partition_cols=["datecol"],
                              preserve_index=False,
                              mode="overwrite")
    df2 = None
    for counter in range(10):
        sleep(1)
        df2 = session.pandas.read_sql_athena(ctas_approach=False, sql="select * from test", database=database)
        assert len(list(df.columns)) == len(list(df2.columns))
        if len(df.index) == len(df2.index):
            break
    assert len(df.index) == len(df2.index)
    assert df2.dtypes[0] == "object"
    assert df2.dtypes[1] == "object"
    assert df2.dtypes[2] == "object"
    session.s3.delete_objects(path=path)


def test_partition_cast_date(session, bucket, database):
    df = pd.DataFrame({
        "col1": ["val1", "val2"],
        "datecol": ["2019-11-09", "2019-11-08"],
        "partcol": ["2019-11-09", "2019-11-08"]
    })
    path = f"s3://{bucket}/test/"
    schema = {
        "col1": "string",
        "datecol": "date",
        "partcol": "date",
    }
    session.pandas.to_parquet(dataframe=df,
                              database=database,
                              path=path,
                              partition_cols=["partcol"],
                              preserve_index=False,
                              cast_columns=schema,
                              mode="overwrite")
    df2 = None
    for counter in range(10):
        sleep(1)
        df2 = session.pandas.read_sql_athena(ctas_approach=False, sql="select * from test", database=database)
        assert len(list(df.columns)) == len(list(df2.columns))
        if len(df.index) == len(df2.index):
            break
    assert len(df.index) == len(df2.index)
    assert df2.dtypes[0] == "object"
    assert df2.dtypes[1] == "object"
    assert df2.dtypes[2] == "object"
    session.s3.delete_objects(path=path)


def test_partition_cast_timestamp(session, bucket, database):
    df = pd.DataFrame({
        "col1": ["val1", "val2"],
        "datecol": ["2019-11-09", "2019-11-08"],
        "partcol": ["2019-11-09", "2019-11-08"]
    })
    path = f"s3://{bucket}/test/"
    schema = {
        "col1": "string",
        "datecol": "timestamp",
        "partcol": "timestamp",
    }
    session.pandas.to_parquet(dataframe=df,
                              database=database,
                              path=path,
                              partition_cols=["partcol"],
                              preserve_index=False,
                              cast_columns=schema,
                              mode="overwrite")
    df2 = None
    for counter in range(10):
        sleep(1)
        df2 = session.pandas.read_sql_athena(ctas_approach=False, sql="select * from test", database=database)
        assert len(list(df.columns)) == len(list(df2.columns))
        if len(df.index) == len(df2.index):
            break
    assert len(df.index) == len(df2.index)
    assert str(df2.dtypes[0]) == "object"
    assert str(df2.dtypes[1]).startswith("datetime64")
    assert str(df2.dtypes[2]).startswith("datetime64")
    session.s3.delete_objects(path=path)


def test_partition_cast(session, bucket, database):
    df = pd.DataFrame({
        "col1": ["val1", "val2"],
        "datecol": ["2019-11-09", "2019-11-08"],
        "partcol": ["2019-11-09", "2019-11-08"],
        "col_double": ["1.0", "1.1"],
        "col_bool": ["True", "False"],
    })
    path = f"s3://{bucket}/test/"
    schema = {
        "col1": "string",
        "datecol": "timestamp",
        "partcol": "timestamp",
        "col_double": "double",
        "col_bool": "boolean",
    }
    session.pandas.to_parquet(dataframe=df,
                              database=database,
                              path=path,
                              partition_cols=["partcol"],
                              preserve_index=False,
                              cast_columns=schema,
                              mode="overwrite")
    df2 = None
    for counter in range(10):
        sleep(1)
        df2 = session.pandas.read_sql_athena(ctas_approach=False, sql="select * from test", database=database)
        assert len(list(df.columns)) == len(list(df2.columns))
        if len(df.index) == len(df2.index):
            break
    assert len(df.index) == len(df2.index)
    assert df2.dtypes[0] == "object"
    assert str(df2.dtypes[1]).startswith("datetime")
    assert str(df2.dtypes[2]).startswith("float")
    assert str(df2.dtypes[3]).startswith("bool")
    assert str(df2.dtypes[4]).startswith("datetime")
    session.s3.delete_objects(path=path)


@pytest.mark.parametrize("procs", [1, 2, 8])
def test_partition_single_row(session, bucket, database, procs):
    data = {
        "col1": [
            1,
            2,
            3,
        ],
        "datecol": [
            "2019-11-09",
            "2019-11-09",
            "2019-11-08",
        ],
        "partcol": [
            "2019-11-09",
            "2019-11-09",
            "2019-11-08",
        ]
    }
    df = pd.DataFrame(data)
    df = df.astype({"datecol": "datetime64", "partcol": "datetime64"})
    schema = {
        "col1": "bigint",
        "datecol": "date",
        "partcol": "date",
    }
    path = f"s3://{bucket}/test/"
    session.pandas.to_parquet(dataframe=df,
                              database=database,
                              path=path,
                              partition_cols=["datecol"],
                              mode="overwrite",
                              cast_columns=schema,
                              procs_cpu_bound=procs,
                              preserve_index=False)
    df2 = None
    for counter in range(10):
        sleep(1)
        df2 = session.pandas.read_sql_athena(ctas_approach=False, sql="select * from test", database=database)
        assert len(list(df.columns)) == len(list(df2.columns))
        if len(df.index) == len(df2.index):
            break
    assert len(df.index) == len(df2.index)
    assert df2.dtypes[0] == "Int64"
    assert df2.dtypes[1] == "object"
    assert df2.dtypes[2] == "object"
    session.s3.delete_objects(path=path)


@pytest.mark.parametrize("partition_cols", [None, ["pt"]])
def test_nan_cast(session, bucket, database, partition_cols):
    dtypes = {"col1": "object", "col2": "object", "col3": "object", "col4": "object", "pt": "object"}
    df = pd.read_csv("data_samples/nan.csv", dtype=dtypes)
    schema = {
        "col1": "string",
        "col2": "string",
        "col3": "string",
        "col4": "string",
        "pt": "string",
    }
    path = f"s3://{bucket}/test/"
    session.pandas.to_parquet(dataframe=df,
                              database=database,
                              path=path,
                              partition_cols=partition_cols,
                              mode="overwrite",
                              cast_columns=schema)
    df2 = None
    for counter in range(10):
        sleep(1)
        df2 = session.pandas.read_sql_athena(ctas_approach=False, sql="select * from test", database=database)
        assert len(list(df.columns)) == len(list(df2.columns)) - 1
        if len(df.index) == len(df2.index):
            break
    assert len(df.index) == len(df2.index)
    assert df2.dtypes[0] == "object"
    assert df2.dtypes[1] == "object"
    assert df2.dtypes[2] == "object"
    assert df2.dtypes[3] == "object"
    assert df2.iloc[:, 0].isna().sum() == 4
    assert df2.iloc[:, 1].isna().sum() == 2
    assert df2.iloc[:, 2].isna().sum() == 2
    assert df2.iloc[:, 3].isna().sum() == 2
    assert df2.iloc[:, 4].isna().sum() == 0
    assert df2.iloc[:, 5].isna().sum() == 0
    if partition_cols is None:
        assert df2.dtypes[4] == "object"
        assert df2.dtypes[5] == "Int64"
    else:
        assert df2.dtypes[4] == "Int64"
        assert df2.dtypes[5] == "object"
    session.s3.delete_objects(path=path)


def test_to_parquet_date_null(session, bucket, database):
    df = pd.DataFrame({
        "col1": ["val1", "val2"],
        "datecol": [date(2019, 11, 9), None],
    })
    path = f"s3://{bucket}/test/"
    session.pandas.to_parquet(dataframe=df,
                              database=database,
                              table="test",
                              path=path,
                              mode="overwrite",
                              preserve_index=False,
                              procs_cpu_bound=1)
    df2 = None
    for counter in range(10):  # Retrying to workaround s3 eventual consistency
        sleep(1)
        df2 = session.pandas.read_sql_athena(ctas_approach=False, sql="select * from test", database=database)
        if len(df.index) == len(df2.index):
            break
    path = f"s3://{bucket}/test2/"
    session.pandas.to_parquet(dataframe=df2,
                              database=database,
                              table="test2",
                              path=path,
                              mode="overwrite",
                              preserve_index=False,
                              procs_cpu_bound=1)
    df3 = None
    for counter in range(10):  # Retrying to workaround s3 eventual consistency
        sleep(1)
        df3 = session.pandas.read_sql_athena(ctas_approach=False, sql="select * from test2", database=database)
        if len(df2.index) == len(df3.index):
            break

    session.s3.delete_objects(path=path)

    assert len(list(df.columns)) == len(list(df2.columns)) == len(list(df3.columns))
    assert len(df.index) == len(df2.index) == len(df3.index)

    assert df[df.col1 == "val1"].iloc[0].datecol == df2[df2.col1 == "val1"].iloc[0].datecol
    assert df2[df2.col1 == "val1"].iloc[0].datecol == df3[df3.col1 == "val1"].iloc[0].datecol == date(2019, 11, 9)

    assert df[df.col1 == "val2"].iloc[0].datecol == df2[df2.col1 == "val2"].iloc[0].datecol
    assert df2[df2.col1 == "val2"].iloc[0].datecol == df3[df3.col1 == "val2"].iloc[0].datecol is None


def test_to_parquet_date_null_at_first(session, bucket, database):
    df = pd.DataFrame({
        "col1": ["val0", "val1", "val2", "val3", "val4", "val5", "val6", "val7", "val8", "val9"],
        "datecol": [None, pd.NaT, None, pd.NaT, None, pd.NaT, None, pd.NaT, None,
                    date(2019, 11, 9)],
    })
    path = f"s3://{bucket}/test/"
    session.pandas.to_parquet(dataframe=df,
                              database=database,
                              table="test",
                              path=path,
                              mode="overwrite",
                              preserve_index=False,
                              procs_cpu_bound=1,
                              cast_columns={"datecol": "date"})
    df2 = None
    for counter in range(10):  # Retrying to workaround s3 eventual consistency
        sleep(1)
        df2 = session.pandas.read_sql_athena(ctas_approach=False, sql="select * from test", database=database)
        if len(df.index) == len(df2.index):
            break

    session.s3.delete_objects(path=path)

    assert len(list(df.columns)) == len(list(df2.columns))
    assert len(df.index) == len(df2.index)

    assert df[df.col1 == "val9"].iloc[0].datecol == df2[df2.col1 == "val9"].iloc[0].datecol == date(2019, 11, 9)
    assert df[df.col1 == "val0"].iloc[0].datecol == df2[df2.col1 == "val0"].iloc[0].datecol is None


def test_to_parquet_lists2(session, bucket, database):
    df = pd.DataFrame({
        "A": [1, 2, 3],
        "B": [[], [4.0, None, 6.0], []],
        "C": [[], [7, None, 9], []],
        "D": [[], ["foo", None, "bar"], []],
        "E": [10, 11, 12]
    })
    path = f"s3://{bucket}/test/"
    session.pandas.to_parquet(dataframe=df,
                              database=database,
                              path=path,
                              mode="overwrite",
                              preserve_index=False,
                              procs_cpu_bound=1)
    df2 = None
    for counter in range(10):  # Retrying to workaround s3 eventual consistency
        sleep(1)
        df2 = session.pandas.read_sql_athena(ctas_approach=False, sql="select * from test", database=database)
        if len(df.index) == len(df2.index):
            break
    session.s3.delete_objects(path=path)

    assert len(list(df.columns)) == len(list(df2.columns))
    assert len(df.index) == len(df2.index)

    assert df2[df2.a == 2].iloc[0].b[0] == 4.0
    assert df2[df2.a == 2].iloc[0].c[0] == 7
    assert df2[df2.a == 2].iloc[0].d[0] == "foo"


def test_to_parquet_decimal(session, bucket, database):
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "decimal_2": [Decimal((0, (1, 9, 9), -2)), None, Decimal((0, (1, 9, 0), -2))],
        "decimal_5": [Decimal((0, (1, 9, 9, 9, 9, 9), -5)), None,
                      Decimal((0, (1, 9, 0, 0, 0, 0), -5))],
    })
    path = f"s3://{bucket}/test/"
    session.pandas.to_parquet(dataframe=df,
                              database=database,
                              path=path,
                              mode="overwrite",
                              preserve_index=False,
                              procs_cpu_bound=1)
    df2 = None
    for counter in range(10):  # Retrying to workaround s3 eventual consistency
        sleep(1)
        df2 = session.pandas.read_sql_athena(ctas_approach=False, sql="select * from test", database=database)
        if len(df.index) == len(df2.index):
            break
    session.s3.delete_objects(path=path)

    assert len(list(df.columns)) == len(list(df2.columns))
    assert len(df.index) == len(df2.index)

    assert df2[df2.id == 1].iloc[0].decimal_2 == Decimal((0, (1, 9, 9), -2))
    assert df2[df2.id == 1].iloc[0].decimal_5 == Decimal((0, (1, 9, 9, 9, 9, 9), -5))
    assert df2[df2.id == 2].iloc[0].decimal_2 is None
    assert df2[df2.id == 2].iloc[0].decimal_5 is None
    assert df2[df2.id == 3].iloc[0].decimal_2 == Decimal((0, (1, 9, 0), -2))
    assert df2[df2.id == 3].iloc[0].decimal_5 == Decimal((0, (1, 9, 0, 0, 0, 0), -5))


def test_read_parquet_dataset(session, bucket):
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "decimal_2": [Decimal((0, (1, 9, 9), -2)),
                      Decimal((0, (1, 9, 9), -2)),
                      Decimal((0, (1, 9, 0), -2))],
        "decimal_5": [
            Decimal((0, (1, 9, 9, 9, 9, 9), -5)),
            Decimal((0, (1, 9, 9, 9, 9, 9), -5)),
            Decimal((0, (1, 9, 0, 0, 0, 0), -5))
        ],
        "float": [1.1, 2.2, 3.3],
        "list_int": [[1, 2], [1], [3, 4, 5]],
        "list_float": [[1.0, 2.0, 3.0], [9.9], [4.0, 5.0]],
        "list_string": [["foo"], ["xxx"], ["boo", "bar"]],
        "list_timestamp": [[datetime(2019, 1, 1), datetime(2019, 1, 2)], [datetime(2019, 1, 3)], [datetime(2019, 1,
                                                                                                           3)]],
        "partition": [0, 0, 1]
    })
    path = f"s3://{bucket}/test_read_parquet/"
    session.pandas.to_parquet(dataframe=df,
                              path=path,
                              mode="overwrite",
                              preserve_index=False,
                              procs_cpu_bound=4,
                              partition_cols=["partition"])
    df2 = session.pandas.read_parquet(path=path)
    assert len(list(df.columns)) == len(list(df2.columns))
    assert len(df.index) == len(df2.index)


def test_read_parquet_file(session, bucket):
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "decimal_2": [Decimal((0, (1, 9, 9), -2)),
                      Decimal((0, (1, 9, 9), -2)),
                      Decimal((0, (1, 9, 0), -2))],
        "decimal_5": [
            Decimal((0, (1, 9, 9, 9, 9, 9), -5)),
            Decimal((0, (1, 9, 9, 9, 9, 9), -5)),
            Decimal((0, (1, 9, 0, 0, 0, 0), -5))
        ],
        "float": [1.1, 2.2, 3.3],
        "list_int": [[1, 2], [1], [3, 4, 5]],
        "list_float": [[1.0, 2.0, 3.0], [9.9], [4.0, 5.0]],
        "list_string": [["foo"], ["xxx"], ["boo", "bar"]],
        "list_timestamp": [[datetime(2019, 1, 1), datetime(2019, 1, 2)], [datetime(2019, 1, 3)], [datetime(2019, 1,
                                                                                                           3)]],
        "partition": [0, 0, 1]
    })
    path = f"s3://{bucket}/test_read_parquet_file/"
    filepath = session.pandas.to_parquet(dataframe=df,
                                         path=path,
                                         mode="overwrite",
                                         preserve_index=False,
                                         procs_cpu_bound=1)
    df2 = session.pandas.read_parquet(path=filepath[0])
    assert len(list(df.columns)) == len(list(df2.columns))
    assert len(df.index) == len(df2.index)


def test_read_parquet_file_stress(session, bucket):
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "decimal_2": [Decimal((0, (1, 9, 9), -2)),
                      Decimal((0, (1, 9, 9), -2)),
                      Decimal((0, (1, 9, 0), -2))],
        "decimal_5": [
            Decimal((0, (1, 9, 9, 9, 9, 9), -5)),
            Decimal((0, (1, 9, 9, 9, 9, 9), -5)),
            Decimal((0, (1, 9, 0, 0, 0, 0), -5))
        ],
        "float": [1.1, 2.2, 3.3],
        "list_int": [[1, 2], [1], [3, 4, 5]],
        "list_float": [[1.0, 2.0, 3.0], [9.9], [4.0, 5.0]],
        "list_string": [["foo"], ["xxx"], ["boo", "bar"]],
        "list_timestamp": [[datetime(2019, 1, 1), datetime(2019, 1, 2)], [datetime(2019, 1, 3)], [datetime(2019, 1,
                                                                                                           3)]],
        "partition": [0, 0, 1]
    })
    for i in range(100):
        path = f"s3://{bucket}/test_read_parquet_file_stress_{i}/"
        filepath = session.pandas.to_parquet(dataframe=df,
                                             path=path,
                                             mode="overwrite",
                                             preserve_index=False,
                                             procs_cpu_bound=8)
        df2 = session.pandas.read_parquet(path=filepath)
        assert len(list(df.columns)) == len(list(df2.columns))
        assert len(df.index) == len(df2.index)


def test_read_sql_athena_date(session, bucket, database):
    df = pd.DataFrame({"id": [1, 2, 3], "col_date": [date(194, 1, 12), date(2019, 1, 2), date(2049, 12, 30)]})
    path = f"s3://{bucket}/test_read_sql_athena_date/"
    session.pandas.to_parquet(dataframe=df,
                              database=database,
                              table="test",
                              path=path,
                              mode="overwrite",
                              preserve_index=False,
                              procs_cpu_bound=4)
    df2 = None
    for counter in range(10):  # Retrying to workaround s3 eventual consistency
        sleep(1)
        df2 = session.pandas.read_sql_athena(ctas_approach=False, sql="select * from test", database=database)
        if len(df.index) == len(df2.index):
            break
    session.s3.delete_objects(path=path)

    assert len(list(df.columns)) == len(list(df2.columns))
    assert len(df.index) == len(df2.index)

    assert df2[df2.id == 1].iloc[0].col_date == date(194, 1, 12)
    assert df2[df2.id == 2].iloc[0].col_date == date(2019, 1, 2)
    assert df2[df2.id == 3].iloc[0].col_date == date(2049, 12, 30)


def test_read_table(session, bucket, database):
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "decimal_2": [Decimal((0, (1, 9, 9), -2)), None, Decimal((0, (1, 9, 0), -2))],
        "decimal_5": [Decimal((0, (1, 9, 9, 9, 9, 9), -5)), None,
                      Decimal((0, (1, 9, 0, 0, 0, 0), -5))],
        "float": [1.1, None, 3.3],
        "list_int": [[1, 2], None, [3, 4, 5]],
        "list_float": [[1.0, 2.0, 3.0], None, [4.0, 5.0]],
        "list_string": [["foo"], None, ["boo", "bar"]],
        "list_timestamp": [[datetime(2019, 1, 1), datetime(2019, 1, 2)], None, [datetime(2019, 1, 3)]],
        "partition": [0, 0, 1]
    })
    path = f"s3://{bucket}/test_read_table/"
    session.pandas.to_parquet(dataframe=df,
                              database=database,
                              table="test",
                              path=path,
                              mode="overwrite",
                              preserve_index=False,
                              procs_cpu_bound=1)
    df2 = session.pandas.read_table(database=database, table="test")
    session.s3.delete_objects(path=path)
    assert len(list(df.columns)) == len(list(df2.columns))
    assert len(df.index) == len(df2.index)


def test_read_table2(session, bucket, database):
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "decimal_2": [Decimal((0, (1, 9, 9), -2)),
                      Decimal((0, (1, 9, 9), -2)),
                      Decimal((0, (1, 9, 0), -2))],
        "decimal_5": [
            Decimal((0, (1, 9, 9, 9, 9, 9), -5)),
            Decimal((0, (1, 9, 9, 9, 9, 9), -5)),
            Decimal((0, (1, 9, 0, 0, 0, 0), -5))
        ],
        "float": [1.1, 2.2, 3.3],
        "list_int": [[1, 2], [1], [3, 4, 5]],
        "list_float": [[1.0, 2.0, 3.0], [9.9], [4.0, 5.0]],
        "list_string": [["foo"], ["xxx"], ["boo", "bar"]],
        "list_timestamp": [[datetime(2019, 1, 1), datetime(2019, 1, 2)], [datetime(2019, 1, 3)], [datetime(2019, 1,
                                                                                                           3)]],
        "partition": [0, 0, 1]
    })
    path = f"s3://{bucket}/test_read_table2/"
    session.pandas.to_parquet(dataframe=df,
                              database=database,
                              table="test",
                              path=path,
                              mode="overwrite",
                              preserve_index=False,
                              procs_cpu_bound=4,
                              partition_cols=["partition"])
    sleep(15)
    df2 = session.pandas.read_table(database=database, table="test")
    session.s3.delete_objects(path=path)
    assert len(list(df.columns)) == len(list(df2.columns))
    assert len(df.index) == len(df2.index)


def test_read_sql_athena_ctas(session, bucket, database):
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "decimal_2": [Decimal((0, (1, 9, 9), -2)),
                      Decimal((0, (1, 9, 9), -2)),
                      Decimal((0, (1, 9, 0), -2))],
        "decimal_5": [
            Decimal((0, (1, 9, 9, 9, 9, 9), -5)),
            Decimal((0, (1, 9, 9, 9, 9, 9), -5)),
            Decimal((0, (1, 9, 0, 0, 0, 0), -5))
        ],
        "float": [1.1, 2.2, 3.3],
        "list_int": [[1, 2], [1], [3, 4, 5]],
        "list_float": [[1.0, 2.0, 3.0], [9.9], [4.0, 5.0]],
        "list_string": [["foo"], ["xxx"], ["boo", "bar"]],
        "list_timestamp": [[datetime(2019, 1, 1), datetime(2019, 1, 2)], [datetime(2019, 1, 3)], [datetime(2019, 1,
                                                                                                           3)]],
        "partition": [0, 0, 1]
    })
    path = f"s3://{bucket}/test_read_table/"
    session.pandas.to_parquet(dataframe=df,
                              database=database,
                              table="test",
                              path=path,
                              mode="overwrite",
                              preserve_index=False,
                              procs_cpu_bound=4,
                              partition_cols=["partition"])
    df2 = session.pandas.read_sql_athena(ctas_approach=True, sql="select * from test", database=database)
    session.s3.delete_objects(path=path)
    assert len(list(df.columns)) == len(list(df2.columns))
    assert len(df.index) == len(df2.index)


def test_read_sql_athena_s3_output_ctas(session, bucket, database):
    n: int = 1_000_000
    df = pd.DataFrame({"id": list((range(n))), "partition": list(["foo" if i % 2 == 0 else "boo" for i in range(n)])})
    path = f"s3://{bucket}/test_read_sql_athena_s3_output_ctas/"
    session.pandas.to_parquet(dataframe=df,
                              database=database,
                              table="test",
                              path=path,
                              mode="overwrite",
                              preserve_index=True,
                              procs_cpu_bound=4,
                              partition_cols=["partition"])
    path_ctas = f"s3://{bucket}/test_read_sql_athena_s3_output_ctas_metadata/"
    df2 = session.pandas.read_sql_athena(ctas_approach=True,
                                         sql="select * from test",
                                         database=database,
                                         s3_output=path_ctas)
    session.s3.delete_objects(path=path)
    assert len(list(df.columns)) + 1 == len(list(df2.columns))
    assert len(df.index) == len(df2.index)
    print(df2)


def test_to_csv_single_file(session, bucket, database):
    n: int = 1_000_000
    df = pd.DataFrame({"id": list((range(n))), "partition": list(["foo" if i % 2 == 0 else "boo" for i in range(n)])})
    path = f"s3://{bucket}/test_to_csv_single_file/"
    s3_path = session.pandas.to_csv(dataframe=df,
                                    database=database,
                                    table="test",
                                    path=path,
                                    mode="overwrite",
                                    preserve_index=True,
                                    procs_cpu_bound=1)
    print(f"s3_path: {s3_path}")
    assert len(s3_path) == 1
    path_ctas = f"s3://{bucket}/test_to_csv_single_file2/"
    df2 = session.pandas.read_sql_athena(ctas_approach=True,
                                         sql="select * from test",
                                         database=database,
                                         s3_output=path_ctas)
    session.s3.delete_objects(path=path)
    assert len(list(df.columns)) + 1 == len(list(df2.columns))
    assert len(df.index) == len(df2.index)
    print(df2)


def test_aurora_mysql_load_simple(bucket, mysql_parameters):
    df = pd.DataFrame({"id": [1, 2, 3], "value": ["foo", "boo", "bar"]})
    conn = Aurora.generate_connection(database="mysql",
                                      host=mysql_parameters["MysqlAddress"],
                                      port=3306,
                                      user="test",
                                      password=mysql_parameters["Password"],
                                      engine="mysql")
    path = f"s3://{bucket}/test_aurora_mysql_load_simple"
    wr.pandas.to_aurora(dataframe=df,
                        connection=conn,
                        schema="test",
                        table="test_aurora_mysql_load_simple",
                        mode="overwrite",
                        temp_s3_path=path)
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM test.test_aurora_mysql_load_simple")
        rows = cursor.fetchall()
        assert len(rows) == len(df.index)
        assert rows[0][0] == 1
        assert rows[1][0] == 2
        assert rows[2][0] == 3
        assert rows[0][1] == "foo"
        assert rows[1][1] == "boo"
        assert rows[2][1] == "bar"
    conn.close()


def test_aurora_postgres_load_simple(bucket, postgres_parameters):
    df = pd.DataFrame({"id": [1, 2, 3], "value": ["foo", "boo", "bar"]})
    conn = Aurora.generate_connection(database="postgres",
                                      host=postgres_parameters["PostgresAddress"],
                                      port=3306,
                                      user="test",
                                      password=postgres_parameters["Password"],
                                      engine="postgres")
    path = f"s3://{bucket}/test_aurora_postgres_load_simple"
    wr.pandas.to_aurora(
        dataframe=df,
        connection=conn,
        schema="public",
        table="test_aurora_postgres_load_simple",
        mode="overwrite",
        temp_s3_path=path,
        engine="postgres",
    )
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM public.test_aurora_postgres_load_simple")
        rows = cursor.fetchall()
        assert len(rows) == len(df.index)
        assert rows[0][0] == 1
        assert rows[1][0] == 2
        assert rows[2][0] == 3
        assert rows[0][1] == "foo"
        assert rows[1][1] == "boo"
        assert rows[2][1] == "bar"
    conn.close()


def test_aurora_mysql_unload_simple(bucket, mysql_parameters):
    df = pd.DataFrame({"id": [1, 2, 3], "value": ["foo", "boo", "bar"]})
    conn = Aurora.generate_connection(database="mysql",
                                      host=mysql_parameters["MysqlAddress"],
                                      port=3306,
                                      user="test",
                                      password=mysql_parameters["Password"],
                                      engine="mysql")
    path = f"s3://{bucket}/test_aurora_mysql_unload_simple"
    wr.pandas.to_aurora(dataframe=df,
                        connection=conn,
                        schema="test",
                        table="test_aurora_mysql_load_simple",
                        mode="overwrite",
                        temp_s3_path=path)
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM test.test_aurora_mysql_load_simple")
        rows = cursor.fetchall()
        assert len(rows) == len(df.index)
        assert rows[0][0] == 1
        assert rows[1][0] == 2
        assert rows[2][0] == 3
        assert rows[0][1] == "foo"
        assert rows[1][1] == "boo"
        assert rows[2][1] == "bar"
    path2 = f"s3://{bucket}/test_aurora_mysql_unload_simple2"
    df2 = wr.pandas.read_sql_aurora(sql="SELECT * FROM test.test_aurora_mysql_load_simple",
                                    connection=conn,
                                    col_names=["id", "value"],
                                    temp_s3_path=path2)
    assert len(df.index) == len(df2.index)
    assert len(df.columns) == len(df2.columns)
    df2 = wr.pandas.read_sql_aurora(sql="SELECT * FROM test.test_aurora_mysql_load_simple",
                                    connection=conn,
                                    temp_s3_path=path2)
    assert len(df.index) == len(df2.index)
    assert len(df.columns) == len(df2.columns)
    conn.close()


@pytest.mark.parametrize("sample, row_num", [("data_samples/micro.csv", 30), ("data_samples/small.csv", 100)])
def test_read_csv_list(bucket, sample, row_num):
    n = 10
    paths = []
    for i in range(n):
        key = f"{sample}_{i}"
        boto3.client("s3").upload_file(sample, bucket, key)
        paths.append(f"s3://{bucket}/{key}")
    dataframe = wr.pandas.read_csv_list(paths=paths)
    wr.s3.delete_listed_objects(objects_paths=paths)
    assert len(dataframe.index) == row_num * n


@pytest.mark.parametrize("sample, row_num", [("data_samples/micro.csv", 30), ("data_samples/small.csv", 100)])
def test_read_csv_list_iterator(bucket, sample, row_num):
    n = 10
    paths = []
    for i in range(n):
        key = f"{sample}_{i}"
        boto3.client("s3").upload_file(sample, bucket, key)
        paths.append(f"s3://{bucket}/{key}")

    df_iter = wr.pandas.read_csv_list(paths=paths, max_result_size=200)
    total_count = 0
    for df in df_iter:
        count = len(df.index)
        print(f"count: {count}")
        total_count += count
    wr.s3.delete_listed_objects(objects_paths=paths)
    assert total_count == row_num * n


def test_aurora_mysql_load_append(bucket, mysql_parameters):
    n: int = 10_000
    df = pd.DataFrame({"id": list((range(n))), "value": list(["foo" if i % 2 == 0 else "boo" for i in range(n)])})
    conn = Aurora.generate_connection(database="mysql",
                                      host=mysql_parameters["MysqlAddress"],
                                      port=3306,
                                      user="test",
                                      password=mysql_parameters["Password"],
                                      engine="mysql")
    path = f"s3://{bucket}/test_aurora_mysql_load_append"

    # LOAD
    wr.pandas.to_aurora(dataframe=df,
                        connection=conn,
                        schema="test",
                        table="test_aurora_mysql_load_append",
                        mode="overwrite",
                        temp_s3_path=path)
    with conn.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM test.test_aurora_mysql_load_append")
        count = cursor.fetchall()[0][0]
        assert count == len(df.index)

    # APPEND
    wr.pandas.to_aurora(dataframe=df,
                        connection=conn,
                        schema="test",
                        table="test_aurora_mysql_load_append",
                        mode="append",
                        temp_s3_path=path)
    with conn.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM test.test_aurora_mysql_load_append")
        count = cursor.fetchall()[0][0]
        assert count == len(df.index) * 2

    # RESET
    wr.pandas.to_aurora(dataframe=df,
                        connection=conn,
                        schema="test",
                        table="test_aurora_mysql_load_append",
                        mode="overwrite",
                        temp_s3_path=path)
    with conn.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM test.test_aurora_mysql_load_append")
        count = cursor.fetchall()[0][0]
        assert count == len(df.index)

    conn.close()


def test_aurora_postgres_load_append(bucket, postgres_parameters):
    df = pd.DataFrame({"id": [1, 2, 3], "value": ["foo", "boo", "bar"]})
    conn = Aurora.generate_connection(database="postgres",
                                      host=postgres_parameters["PostgresAddress"],
                                      port=3306,
                                      user="test",
                                      password=postgres_parameters["Password"],
                                      engine="postgres")
    path = f"s3://{bucket}/test_aurora_postgres_load_append"

    # LOAD
    wr.pandas.to_aurora(dataframe=df,
                        connection=conn,
                        schema="public",
                        table="test_aurora_postgres_load_append",
                        mode="overwrite",
                        temp_s3_path=path,
                        engine="postgres")
    with conn.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM public.test_aurora_postgres_load_append")
        count = cursor.fetchall()[0][0]
        assert count == len(df.index)

    # APPEND
    wr.pandas.to_aurora(dataframe=df,
                        connection=conn,
                        schema="public",
                        table="test_aurora_postgres_load_append",
                        mode="append",
                        temp_s3_path=path,
                        engine="postgres")
    with conn.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM public.test_aurora_postgres_load_append")
        count = cursor.fetchall()[0][0]
        assert count == len(df.index) * 2

    # RESET
    wr.pandas.to_aurora(dataframe=df,
                        connection=conn,
                        schema="public",
                        table="test_aurora_postgres_load_append",
                        mode="overwrite",
                        temp_s3_path=path,
                        engine="postgres")
    with conn.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM public.test_aurora_postgres_load_append")
        count = cursor.fetchall()[0][0]
        assert count == len(df.index)

    conn.close()
