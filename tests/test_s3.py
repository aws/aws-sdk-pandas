import bz2
import datetime
import gzip
import itertools
import logging
import lzma
import time
from io import BytesIO, TextIOWrapper
from unittest.mock import patch

import boto3
import botocore
import pandas as pd
import pytest
import pytz

import awswrangler as wr

from ._utils import get_df_csv

API_CALL = botocore.client.BaseClient._make_api_call

logging.basicConfig(level=logging.INFO, format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)
logging.getLogger("botocore.credentials").setLevel(logging.CRITICAL)


def test_delete_internal_error(bucket):
    response = {
        "Errors": [
            {
                "Key": "foo/dt=2020-01-01 00%3A00%3A00/boo.txt",
                "Code": "InternalError",
                "Message": "We encountered an internal error. Please try again.",
            }
        ]
    }

    def mock_make_api_call(self, operation_name, kwarg):
        if operation_name == "DeleteObjects":
            return response
        return API_CALL(self, operation_name, kwarg)

    start = time.time()
    with patch("botocore.client.BaseClient._make_api_call", new=mock_make_api_call):
        path = f"s3://{bucket}/foo/dt=2020-01-01 00:00:00/boo.txt"
        with pytest.raises(wr.exceptions.ServiceApiError):
            wr.s3.delete_objects(path=[path])
    assert 15 <= (time.time() - start) <= 20


def test_delete_error(bucket):
    response = {"Errors": [{"Code": "AnyNonInternalError"}]}

    def mock_make_api_call(self, operation_name, kwarg):
        if operation_name == "DeleteObjects":
            return response
        return API_CALL(self, operation_name, kwarg)

    with patch("botocore.client.BaseClient._make_api_call", new=mock_make_api_call):
        path = f"s3://{bucket}/boo/dt=2020-01-01 00:00:00/bar.txt"
        with pytest.raises(wr.exceptions.ServiceApiError):
            wr.s3.delete_objects(path=[path])


def test_csv(path):
    session = boto3.Session()
    df = pd.DataFrame({"id": [1, 2, 3]})
    path0 = f"{path}test_csv0.csv"
    path1 = f"{path}test_csv1.csv"
    path2 = f"{path}test_csv2.csv"
    wr.s3.to_csv(df=df, path=path0, index=False)
    wr.s3.wait_objects_exist(paths=[path0])
    assert wr.s3.does_object_exist(path=path0) is True
    assert wr.s3.size_objects(path=[path0], use_threads=False)[path0] == 9
    assert wr.s3.size_objects(path=[path0], use_threads=True)[path0] == 9
    wr.s3.to_csv(df=df, path=path1, index=False, boto3_session=None)
    wr.s3.wait_objects_exist(paths=[path1])
    wr.s3.to_csv(df=df, path=path2, index=False, boto3_session=session)
    wr.s3.wait_objects_exist(paths=[path2])
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


def test_json(path):
    df0 = pd.DataFrame({"id": [1, 2, 3]})
    path0 = f"{path}test_json0.json"
    path1 = f"{path}test_json1.json"
    wr.s3.to_json(df=df0, path=path0)
    wr.s3.to_json(df=df0, path=path1)
    wr.s3.wait_objects_exist(paths=[path0, path1], use_threads=False)
    assert df0.equals(wr.s3.read_json(path=path0, use_threads=False))
    df1 = pd.concat(objs=[df0, df0], sort=False, ignore_index=False)
    assert df1.equals(wr.s3.read_json(path=[path0, path1], use_threads=True))


def test_fwf(path):
    text = "1 Herfelingen27-12-18\n2   Lambusart14-06-18\n3Spormaggiore15-04-18"
    client_s3 = boto3.client("s3")
    path0 = f"{path}0.txt"
    bucket, key = wr._utils.parse_path(path0)
    client_s3.put_object(Body=text, Bucket=bucket, Key=key)
    path1 = f"{path}1.txt"
    bucket, key = wr._utils.parse_path(path1)
    client_s3.put_object(Body=text, Bucket=bucket, Key=key)
    wr.s3.wait_objects_exist(paths=[path0, path1])
    df = wr.s3.read_fwf(path=path0, use_threads=False, widths=[1, 12, 8], names=["id", "name", "date"])
    assert len(df.index) == 3
    assert len(df.columns) == 3
    df = wr.s3.read_fwf(path=[path0, path1], use_threads=True, widths=[1, 12, 8], names=["id", "name", "date"])
    assert len(df.index) == 6
    assert len(df.columns) == 3


def test_list_by_last_modified_date(path):
    df = pd.DataFrame({"id": [1, 2, 3]})
    path0 = f"s3://{path}0.json"
    path1 = f"s3://{path}1.json"

    begin_utc = pytz.utc.localize(datetime.datetime.utcnow())
    time.sleep(2)
    wr.s3.to_json(df, path0)
    time.sleep(2)
    mid_utc = pytz.utc.localize(datetime.datetime.utcnow())
    time.sleep(2)
    wr.s3.to_json(df, path1)
    time.sleep(2)
    end_utc = pytz.utc.localize(datetime.datetime.utcnow())
    wr.s3.wait_objects_exist(paths=[path0, path1], use_threads=False)

    assert len(wr.s3.read_json(path).index) == 6
    assert len(wr.s3.read_json(path, last_modified_begin=mid_utc).index) == 3
    assert len(wr.s3.read_json(path, last_modified_end=mid_utc).index) == 3
    with pytest.raises(wr.exceptions.InvalidArgument):
        wr.s3.read_json(path, last_modified_begin=end_utc)
    with pytest.raises(wr.exceptions.InvalidArgument):
        wr.s3.read_json(path, last_modified_end=begin_utc)
    assert len(wr.s3.read_json(path, last_modified_begin=mid_utc, last_modified_end=end_utc).index) == 3
    assert len(wr.s3.read_json(path, last_modified_begin=begin_utc, last_modified_end=mid_utc).index) == 3
    assert len(wr.s3.read_json(path, last_modified_begin=begin_utc, last_modified_end=end_utc).index) == 6


def test_parquet(path):
    df_file = pd.DataFrame({"id": [1, 2, 3]})
    path_file = f"{path}test_parquet_file.parquet"
    df_dataset = pd.DataFrame({"id": [1, 2, 3], "partition": ["A", "A", "B"]})
    df_dataset["partition"] = df_dataset["partition"].astype("category")
    path_dataset = f"{path}test_parquet_dataset"
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
    assert len(wr.s3.read_parquet(path=[path_file], use_threads=False, boto3_session=boto3.DEFAULT_SESSION).index) == 3
    paths = wr.s3.to_parquet(df=df_dataset, path=path_dataset, dataset=True)["paths"]
    wr.s3.wait_objects_exist(paths=paths)
    assert len(wr.s3.read_parquet(path=paths, dataset=True).index) == 3
    assert len(wr.s3.read_parquet(path=path_dataset, use_threads=True, boto3_session=boto3.DEFAULT_SESSION).index) == 3
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


def test_s3_empty_dfs():
    df = pd.DataFrame()
    with pytest.raises(wr.exceptions.EmptyDataFrame):
        wr.s3.to_parquet(df=df, path="")
    with pytest.raises(wr.exceptions.EmptyDataFrame):
        wr.s3.to_csv(df=df, path="")


def test_absent_object(path):
    path_file = f"{path}test_absent_object"
    assert wr.s3.does_object_exist(path=path_file) is False
    assert len(wr.s3.size_objects(path=path_file)) == 0
    assert wr.s3.wait_objects_exist(paths=[]) is None


def test_parquet_validate_schema(path):
    df = pd.DataFrame({"id": [1, 2, 3]})
    path_file = f"{path}0.parquet"
    wr.s3.to_parquet(df=df, path=path_file)
    wr.s3.wait_objects_exist(paths=[path_file])
    df2 = pd.DataFrame({"id2": [1, 2, 3], "val": ["foo", "boo", "bar"]})
    path_file2 = f"{path}1.parquet"
    wr.s3.to_parquet(df=df2, path=path_file2)
    wr.s3.wait_objects_exist(paths=[path_file2], use_threads=False)
    df3 = wr.s3.read_parquet(path=path, validate_schema=False)
    assert len(df3.index) == 6
    assert len(df3.columns) == 3
    with pytest.raises(ValueError):
        wr.s3.read_parquet(path=path, validate_schema=True)


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


def test_parquet_uint64(path):
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
    assert len(df.index) == 3
    assert len(df.columns) == 5
    assert df.c0.max() == (2 ** 8) - 1
    assert df.c1.max() == (2 ** 16) - 1
    assert df.c2.max() == (2 ** 32) - 1
    assert df.c3.max() == (2 ** 64) - 1
    assert df.c4.astype("uint8").sum() == 3
    wr.s3.delete_objects(path=path)


def test_metadata_partitions(path):
    path = f"{path}0.parquet"
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": ["3", "4", "5"], "c2": [6.0, 7.0, 8.0]})
    paths = wr.s3.to_parquet(df=df, path=path, dataset=False)["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    columns_types, partitions_types = wr.s3.read_parquet_metadata(path=path, dataset=False)
    assert len(columns_types) == len(df.columns)
    assert columns_types.get("c0") == "bigint"
    assert columns_types.get("c1") == "string"
    assert columns_types.get("c2") == "double"


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
    wr.s3.to_parquet(df, path_file, dtype={"id": "string"}, sanitize_columns=False)
    wr.s3.wait_objects_exist([path_file])
    df2 = wr.s3.read_parquet(path_file)
    assert str(df2.id.dtypes) == "string"
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
    assert df.shape == df2.shape
    for col, row in tuple(itertools.product(df.columns, range(3))):
        assert df[col].iloc[row] == df2[col].iloc[row]


def test_to_parquet_file_sanitize(path):
    df = pd.DataFrame({"C0": [0, 1], "camelCase": [2, 3], "c**--2": [4, 5]})
    path_file = f"{path}0.parquet"
    wr.s3.to_parquet(df, path_file, sanitize_columns=True)
    wr.s3.wait_objects_exist([path_file])
    df2 = wr.s3.read_parquet(path_file)
    assert df.shape == df2.shape
    assert list(df2.columns) == ["c0", "camel_case", "c_2"]
    assert df2.c0.sum() == 1
    assert df2.camel_case.sum() == 5
    assert df2.c_2.sum() == 9


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


def test_list_wrong_path(path):
    wrong_path = path.replace("s3://", "")
    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.s3.list_objects(wrong_path)


@pytest.mark.parametrize("sanitize_columns,col", [(True, "foo_boo"), (False, "FooBoo")])
def test_sanitize_columns(path, sanitize_columns, col):
    df = pd.DataFrame({"FooBoo": [1, 2, 3]})

    # Parquet
    file_path = f"{path}0.parquet"
    wr.s3.to_parquet(df, path=file_path, sanitize_columns=sanitize_columns)
    wr.s3.wait_objects_exist([file_path])
    df = wr.s3.read_parquet(file_path)
    assert len(df.index) == 3
    assert len(df.columns) == 1
    assert df.columns == [col]

    # CSV
    file_path = f"{path}0.csv"
    wr.s3.to_csv(df, path=file_path, sanitize_columns=sanitize_columns, index=False)
    wr.s3.wait_objects_exist([file_path])
    df = wr.s3.read_csv(file_path)
    assert len(df.index) == 3
    assert len(df.columns) == 1
    assert df.columns == [col]


def test_s3_get_bucket_region(bucket, region):
    assert wr.s3.get_bucket_region(bucket=bucket) == region
    assert wr.s3.get_bucket_region(bucket=bucket, boto3_session=boto3.DEFAULT_SESSION) == region


def test_read_csv_index(path):
    df0 = pd.DataFrame({"id": [4, 5, 6], "value": ["foo", "boo", "bar"]})
    df1 = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
    path0 = f"{path}test_csv0.csv"
    path1 = f"{path}test_csv1.csv"
    paths = [path0, path1]
    wr.s3.to_csv(df=df0, path=path0, index=False)
    wr.s3.to_csv(df=df1, path=path1, index=False)
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df = wr.s3.read_csv(paths, index_col=["id"])
    assert df.shape == (6, 1)


def test_read_json_index(path):
    df0 = pd.DataFrame({"id": [4, 5, 6], "value": ["foo", "boo", "bar"]})
    df1 = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
    path0 = f"{path}test0.csv"
    path1 = f"{path}test1.csv"
    paths = [path0, path1]
    wr.s3.to_json(df=df0, path=path0, orient="index")
    wr.s3.to_json(df=df1, path=path1, orient="index")
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    df = wr.s3.read_json(paths, orient="index")
    assert df.shape == (6, 2)
