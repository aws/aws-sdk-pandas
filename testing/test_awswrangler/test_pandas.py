from time import sleep
import logging
import csv
import datetime

import pytest
import boto3
import pandas
import numpy

from awswrangler import Session, Pandas
from awswrangler.exceptions import LineTerminatorNotFound

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)


@pytest.fixture(scope="module")
def cloudformation_outputs():
    response = boto3.client("cloudformation").describe_stacks(
        StackName="aws-data-wrangler-test-arena")
    outputs = {}
    for output in response.get("Stacks")[0].get("Outputs"):
        outputs[output.get("OutputKey")] = output.get("OutputValue")
    yield outputs


@pytest.fixture(scope="module")
def session():
    yield Session()


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
def database(cloudformation_outputs):
    if "GlueDatabaseName" in cloudformation_outputs:
        database = cloudformation_outputs.get("GlueDatabaseName")
    else:
        raise Exception("You must deploy the test infrastructure using SAM!")
    yield database


@pytest.mark.parametrize("sample, row_num", [("data_samples/micro.csv", 30),
                                             ("data_samples/small.csv", 100)])
def test_read_csv(session, bucket, sample, row_num):
    boto3.client("s3").upload_file(sample, bucket, sample)
    path = f"s3://{bucket}/{sample}"
    dataframe = session.pandas.read_csv(path=path)
    session.s3.delete_objects(path=path)
    assert len(dataframe.index) == row_num


@pytest.mark.parametrize("sample, row_num", [("data_samples/micro.csv", 30),
                                             ("data_samples/small.csv", 100)])
def test_read_csv_iterator(session, bucket, sample, row_num):
    boto3.client("s3").upload_file(sample, bucket, sample)
    path = f"s3://{bucket}/{sample}"
    dataframe_iter = session.pandas.read_csv(path=path, max_result_size=200)
    total_count = 0
    for dataframe in dataframe_iter:
        total_count += len(dataframe.index)
    session.s3.delete_objects(path=path)
    assert total_count == row_num


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
    dataframe = pandas.read_csv("data_samples/micro.csv")
    func = session.pandas.to_csv if file_format == "csv" else session.pandas.to_parquet
    objects_paths = func(
        dataframe=dataframe,
        database=database,
        path=f"s3://{bucket}/test/",
        preserve_index=preserve_index,
        mode=mode,
        partition_cols=partition_cols,
        procs_cpu_bound=procs_cpu_bound,
    )
    num_partitions = (len([keys for keys in dataframe.groupby(partition_cols)])
                      if partition_cols else 1)
    assert len(objects_paths) >= num_partitions
    dataframe2 = None
    for counter in range(10):
        dataframe2 = session.pandas.read_sql_athena(sql="select * from test",
                                                    database=database)
        if factor * len(dataframe.index) == len(dataframe2.index):
            break
        sleep(1)
    assert factor * len(dataframe.index) == len(dataframe2.index)


def test_to_parquet_with_cast(
        session,
        bucket,
        database,
):
    dataframe = pandas.read_csv("data_samples/nano.csv",
                                dtype={"id": "Int64"},
                                parse_dates=["date", "time"])
    print(dataframe.dtypes)
    session.pandas.to_parquet(dataframe=dataframe,
                              database=database,
                              path=f"s3://{bucket}/test/",
                              preserve_index=False,
                              mode="overwrite",
                              procs_cpu_bound=1,
                              cast_columns={"value": "int64"})
    dataframe2 = None
    for counter in range(10):
        dataframe2 = session.pandas.read_sql_athena(sql="select * from test",
                                                    database=database)
        if len(dataframe.index) == len(dataframe2.index):
            break
        sleep(1)
    assert len(dataframe.index) == len(dataframe2.index)
    print(dataframe2)
    print(dataframe2.dtypes)


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
def test_read_sql_athena_iterator(session, bucket, database, sample, row_num,
                                  max_result_size):
    parse_dates = []
    if sample == "data_samples/nano.csv":
        parse_dates.append("time")
        parse_dates.append("date")
    dataframe_sample = pandas.read_csv(sample, parse_dates=parse_dates)
    path = f"s3://{bucket}/test/"
    session.pandas.to_parquet(dataframe=dataframe_sample,
                              database=database,
                              path=path,
                              preserve_index=False,
                              mode="overwrite")
    total_count = 0
    for counter in range(10):
        dataframe_iter = session.pandas.read_sql_athena(
            sql="select * from test",
            database=database,
            max_result_size=max_result_size)
        total_count = 0
        for dataframe in dataframe_iter:
            total_count += len(dataframe.index)
            print(dataframe)
        if total_count == row_num:
            break
        sleep(1)
    session.s3.delete_objects(path=path)
    assert total_count == row_num


@pytest.mark.parametrize(
    "body, sep, quotechar, lineterminator, last_index, last_terminator_suspect_index,"
    "first_non_special_byte_index, sep_counter, quote_counter", [
        (b'"foo","boo"\n', ",", '"', "\n", None, 11, 9, 0, 1),
        (b'"foo","boo"\n"bar', ",", '"', "\n", None, 11, 9, 0, 1),
        (b'!foo!;!boo!@', ";", '!', "@", None, 11, 9, 0, 1),
        (b'"foo","boo"\n"bar\n', ",", '"', "\n", 16, 11, 9, 0, 1),
    ])
def test_extract_terminator_profile(body, sep, quotechar, lineterminator,
                                    last_index, last_terminator_suspect_index,
                                    first_non_special_byte_index, sep_counter,
                                    quote_counter):
    profile = Pandas._extract_terminator_profile(body=body,
                                                 sep=sep,
                                                 quotechar=quotechar,
                                                 lineterminator=lineterminator,
                                                 last_index=last_index)
    assert profile[
        "last_terminator_suspect_index"] == last_terminator_suspect_index
    assert profile[
        "first_non_special_byte_index"] == first_non_special_byte_index
    assert profile["sep_counter"] == sep_counter
    assert profile["quote_counter"] == quote_counter


@pytest.mark.parametrize(
    "body, sep, quoting, quotechar, lineterminator, ret", [
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
        (b'"foo"\n"boo","\n","\n","\n","\n","\n",,,,,,"\n",,,,', ",",
         csv.QUOTE_ALL, '"', "\n", 5),
        (b'012",\n"foo","\n\n\n\n","\n', ",", csv.QUOTE_ALL, '"', "\n", 5),
    ])
def test_find_terminator(body, sep, quoting, quotechar, lineterminator, ret):
    assert Pandas._find_terminator(body=body,
                                   sep=sep,
                                   quoting=quoting,
                                   quotechar=quotechar,
                                   lineterminator=lineterminator) == ret


@pytest.mark.parametrize(
    "body, sep, quoting, quotechar, lineterminator",
    [(b"jawdnkjawnd", ",", csv.QUOTE_MINIMAL, '"', "\n"),
     (b"jawdnkjawnd", ",", csv.QUOTE_ALL, '"', "\n"),
     (b"jawdnkj\nawnd", ",", csv.QUOTE_ALL, '"', "\n"),
     (b'jawdnkj"x\n\n"awnd', ",", csv.QUOTE_ALL, '"', "\n"),
     (b'jawdnkj""\n,,,,,,,,,,awnd', ",", csv.QUOTE_ALL, '"', "\n"),
     (b'jawdnkj,""""""\nawnd', ",", csv.QUOTE_ALL, '"', "\n")])
def test_find_terminator_exception(body, sep, quoting, quotechar,
                                   lineterminator):
    with pytest.raises(LineTerminatorNotFound):
        assert Pandas._find_terminator(body=body,
                                       sep=sep,
                                       quoting=quoting,
                                       quotechar=quotechar,
                                       lineterminator=lineterminator)


@pytest.mark.parametrize("max_result_size", [400, 700, 1000, 10000])
def test_etl_complex(session, bucket, database, max_result_size):
    dataframe = pandas.read_csv("data_samples/complex.csv",
                                dtype={"my_int_with_null": "Int64"},
                                parse_dates=["my_timestamp", "my_date"])
    session.pandas.to_parquet(dataframe=dataframe,
                              database=database,
                              path=f"s3://{bucket}/test/",
                              preserve_index=False,
                              mode="overwrite",
                              procs_cpu_bound=1)
    sleep(1)
    df_iter = session.pandas.read_sql_athena(sql="select * from test",
                                             database=database,
                                             max_result_size=max_result_size)
    count = 0
    for df in df_iter:
        count += len(df.index)
        for row in df.itertuples():
            assert isinstance(row.my_timestamp, datetime.datetime)
            assert isinstance(row.my_date, datetime.date)
            assert isinstance(row.my_float, float)
            assert isinstance(row.my_int, numpy.int64)
            assert isinstance(row.my_string, str)
            assert str(row.my_timestamp) == "2018-01-01 04:03:02.001000"
            assert str(row.my_date) == "2019-02-02 00:00:00"
            assert str(row.my_float) == "12345.6789"
            assert str(row.my_int) == "123456789"
            assert str(
                row.my_string
            ) == "foo\nboo\nbar\nFOO\nBOO\nBAR\nxxxxx\nÁÃÀÂÇ\n汉字汉字汉字汉字汉字汉字汉字æøåæøåæøåæøåæøåæøåæøåæøåæøåæøå汉字汉字汉字汉字汉字汉字汉字æøåæøåæøåæøåæøåæøåæøåæøåæøåæøå"
    assert count == len(dataframe.index)
