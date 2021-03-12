import datetime
import glob
import logging
import time
from unittest.mock import patch

import boto3
import botocore
import pandas as pd
import pytest
import pytz

import awswrangler as wr

API_CALL = botocore.client.BaseClient._make_api_call

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


@pytest.mark.parametrize("sanitize_columns,col", [(True, "foo_boo"), (False, "FooBoo")])
def test_sanitize_columns(path, sanitize_columns, col):
    df = pd.DataFrame({"FooBoo": [1, 2, 3]})

    # Parquet
    file_path = f"{path}0.parquet"
    wr.s3.to_parquet(df, path=file_path, sanitize_columns=sanitize_columns)
    df = wr.s3.read_parquet(file_path)
    assert len(df.index) == 3
    assert len(df.columns) == 1
    assert df.columns == [col]

    # CSV
    file_path = f"{path}0.csv"
    wr.s3.to_csv(df, path=file_path, sanitize_columns=sanitize_columns, index=False)
    df = wr.s3.read_csv(file_path)
    assert len(df.index) == 3
    assert len(df.columns) == 1
    assert df.columns == [col]


def test_list_by_last_modified_date(path):
    df = pd.DataFrame({"id": [1, 2, 3]})
    path0 = f"s3://{path}0.json"
    path1 = f"s3://{path}1.json"

    begin_utc = pytz.utc.localize(datetime.datetime.utcnow())
    time.sleep(5)
    wr.s3.to_json(df, path0)
    time.sleep(5)
    mid_utc = pytz.utc.localize(datetime.datetime.utcnow())
    time.sleep(5)
    wr.s3.to_json(df, path1)
    time.sleep(5)
    end_utc = pytz.utc.localize(datetime.datetime.utcnow())

    assert len(wr.s3.read_json(path).index) == 6
    assert len(wr.s3.read_json(path, last_modified_begin=mid_utc).index) == 3
    assert len(wr.s3.read_json(path, last_modified_end=mid_utc).index) == 3
    with pytest.raises(wr.exceptions.NoFilesFound):
        wr.s3.read_json(path, last_modified_begin=end_utc)
    with pytest.raises(wr.exceptions.NoFilesFound):
        wr.s3.read_json(path, last_modified_end=begin_utc)
    assert len(wr.s3.read_json(path, last_modified_begin=mid_utc, last_modified_end=end_utc).index) == 3
    assert len(wr.s3.read_json(path, last_modified_begin=begin_utc, last_modified_end=mid_utc).index) == 3
    assert len(wr.s3.read_json(path, last_modified_begin=begin_utc, last_modified_end=end_utc).index) == 6


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
    assert 15 <= (time.time() - start) <= 25


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


def test_missing_or_wrong_path(path, glue_database, glue_table):
    # Missing path
    df = pd.DataFrame({"FooBoo": [1, 2, 3]})
    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.s3.to_parquet(df=df)
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_parquet(df=df, dataset=True)
    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.s3.to_parquet(df=df, dataset=True, database=glue_database, table=glue_table)

    # Wrong path
    wr.s3.to_parquet(df=df, path=path, dataset=True, database=glue_database, table=glue_table)
    wrong_path = "s3://bucket/prefix"
    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.s3.to_parquet(df=df, path=wrong_path, dataset=True, database=glue_database, table=glue_table)


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


@pytest.mark.parametrize("use_threads", [True, False])
def test_merge(path, use_threads):
    path1 = f"{path}test_merge/"
    df = pd.DataFrame({"id": [1, 2, 3], "par": [1, 2, 3]})
    wr.s3.to_parquet(df=df, path=path1, dataset=True, partition_cols=["par"], mode="overwrite", use_threads=use_threads)
    df = wr.s3.read_parquet(path=path1, dataset=True, use_threads=use_threads)
    assert df.id.sum() == 6
    assert df.par.astype("int").sum() == 6

    path2 = f"{path}test_merge2/"
    df = pd.DataFrame({"id": [1, 2, 3], "par": [1, 2, 3]})
    wr.s3.to_parquet(df=df, path=path2, dataset=True, partition_cols=["par"], mode="overwrite", use_threads=use_threads)
    wr.s3.merge_datasets(source_path=path2, target_path=path1, mode="append", use_threads=use_threads)
    df = wr.s3.read_parquet(path=path1, dataset=True, use_threads=use_threads)
    assert df.id.sum() == 12
    assert df.par.astype("int").sum() == 12

    wr.s3.merge_datasets(source_path=path2, target_path=path1, mode="overwrite", use_threads=use_threads)
    df = wr.s3.read_parquet(path=path1, dataset=True, use_threads=use_threads)
    assert df.id.sum() == 6
    assert df.par.astype("int").sum() == 6

    df = pd.DataFrame({"id": [4], "par": [3]})
    wr.s3.to_parquet(df=df, path=path2, dataset=True, partition_cols=["par"], mode="overwrite", use_threads=use_threads)
    wr.s3.merge_datasets(source_path=path2, target_path=path1, mode="overwrite_partitions", use_threads=use_threads)
    df = wr.s3.read_parquet(path=path1, dataset=True, use_threads=use_threads)
    assert df.id.sum() == 7
    assert df.par.astype("int").sum() == 6

    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.s3.merge_datasets(source_path=path, target_path="bar", mode="WRONG")

    assert len(wr.s3.merge_datasets(source_path=f"{path}empty/", target_path="bar")) == 0


@pytest.mark.parametrize("use_threads", [True, False])
@pytest.mark.parametrize(
    "s3_additional_kwargs",
    [None, {"ServerSideEncryption": "AES256"}, {"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": None}],
)
def test_merge_additional_kwargs(path, kms_key_id, s3_additional_kwargs, use_threads):
    if s3_additional_kwargs is not None and "SSEKMSKeyId" in s3_additional_kwargs:
        s3_additional_kwargs["SSEKMSKeyId"] = kms_key_id

    path1 = f"{path}test_merge/"
    df = pd.DataFrame({"id": [1, 2, 3], "par": [1, 2, 3]})
    paths = wr.s3.to_parquet(
        df=df,
        path=path1,
        dataset=True,
        partition_cols=["par"],
        mode="overwrite",
        use_threads=use_threads,
        s3_additional_kwargs=s3_additional_kwargs,
    )
    df = wr.s3.read_parquet(path=path1, dataset=True, use_threads=use_threads)
    assert df.id.sum() == 6
    assert df.par.astype("int").sum() == 6

    path2 = f"{path}test_merge2/"
    df = pd.DataFrame({"id": [1, 2, 3], "par": [1, 2, 3]})
    paths = wr.s3.to_parquet(
        df=df, path=path2, dataset=True, partition_cols=["par"], mode="overwrite", use_threads=use_threads
    )
    paths = wr.s3.merge_datasets(
        source_path=path2,
        target_path=path1,
        mode="append",
        use_threads=use_threads,
        s3_additional_kwargs=s3_additional_kwargs,
    )
    df = wr.s3.read_parquet(path=path1, dataset=True, use_threads=use_threads)
    assert df.id.sum() == 12
    assert df.par.astype("int").sum() == 12

    paths = wr.s3.list_objects(path1)
    assert len(paths) == 6
    descs = wr.s3.describe_objects(paths, use_threads=use_threads)
    for desc in descs.values():
        if s3_additional_kwargs is None:
            assert desc.get("ServerSideEncryption") is None
        elif s3_additional_kwargs["ServerSideEncryption"] == "aws:kms":
            assert desc.get("ServerSideEncryption") == "aws:kms"
        elif s3_additional_kwargs["ServerSideEncryption"] == "AES256":
            assert desc.get("ServerSideEncryption") == "AES256"


@pytest.mark.parametrize("use_threads", [True, False])
def test_copy(path, path2, use_threads):
    df = pd.DataFrame({"id": [1, 2, 3], "par": [1, 2, 3]})
    wr.s3.to_parquet(df=df, path=path, dataset=True, partition_cols=["par"], mode="overwrite", use_threads=use_threads)
    df = wr.s3.read_parquet(path=path, dataset=True, use_threads=use_threads)
    assert df.id.sum() == 6
    assert df.par.astype("int").sum() == 6

    df = pd.DataFrame({"id": [1, 2, 3], "par": [1, 2, 3]})
    paths = wr.s3.to_parquet(
        df=df, path=path2, dataset=True, partition_cols=["par"], mode="overwrite", use_threads=use_threads
    )["paths"]
    paths = wr.s3.copy_objects(paths, source_path=path2, target_path=path, use_threads=use_threads)
    df = wr.s3.read_parquet(path=path, dataset=True, use_threads=use_threads)
    assert df.id.sum() == 12
    assert df.par.astype("int").sum() == 12

    assert len(wr.s3.copy_objects([], source_path="boo", target_path="bar")) == 0


@pytest.mark.parametrize("use_threads", [True, False])
@pytest.mark.parametrize(
    "s3_additional_kwargs",
    [None, {"ServerSideEncryption": "AES256"}, {"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": None}],
)
def test_copy_additional_kwargs(path, path2, kms_key_id, s3_additional_kwargs, use_threads):
    if s3_additional_kwargs is not None and "SSEKMSKeyId" in s3_additional_kwargs:
        s3_additional_kwargs["SSEKMSKeyId"] = kms_key_id
    file_path = f"{path}0.txt"
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [3, 4, 5]})
    wr.s3.to_csv(df, file_path, index=False, s3_additional_kwargs=s3_additional_kwargs)
    assert df.equals(wr.s3.read_csv([file_path]))
    paths = wr.s3.copy_objects(
        [file_path],
        source_path=path,
        target_path=path2,
        use_threads=use_threads,
        s3_additional_kwargs=s3_additional_kwargs,
    )
    file_path2 = paths[0]
    assert df.equals(wr.s3.read_csv(file_path2))
    desc = wr.s3.describe_objects([file_path2])[file_path2]
    if s3_additional_kwargs is None:
        assert desc.get("ServerSideEncryption") is None
    elif s3_additional_kwargs["ServerSideEncryption"] == "aws:kms":
        assert desc.get("ServerSideEncryption") == "aws:kms"
    elif s3_additional_kwargs["ServerSideEncryption"] == "AES256":
        assert desc.get("ServerSideEncryption") == "AES256"


@pytest.mark.parametrize("use_threads", [True, False])
def test_copy_replacing_filename(path, path2, use_threads):
    df = pd.DataFrame({"c0": [1, 2]})
    file_path = f"{path}myfile.parquet"
    wr.s3.to_parquet(df=df, path=file_path, use_threads=use_threads)
    wr.s3.copy_objects(
        paths=[file_path],
        source_path=path,
        target_path=path2,
        replace_filenames={"myfile.parquet": "myfile2.parquet"},
        use_threads=use_threads,
    )
    expected_file = f"{path2}myfile2.parquet"
    objs = wr.s3.list_objects(path=path2)
    assert objs[0] == expected_file


def test_list_wrong_path(path):
    wrong_path = path.replace("s3://", "")
    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.s3.list_objects(wrong_path)


def test_s3_get_bucket_region(bucket, region):
    assert wr.s3.get_bucket_region(bucket=bucket) == region
    assert wr.s3.get_bucket_region(bucket=bucket, boto3_session=boto3.DEFAULT_SESSION) == region


def test_prefix_cleanup():
    # regular
    assert wr.s3._list._prefix_cleanup("foo*") == "foo"
    assert wr.s3._list._prefix_cleanup("*foo") == ""
    assert wr.s3._list._prefix_cleanup("foo*boo") == "foo"
    assert wr.s3._list._prefix_cleanup("foo?") == "foo"
    assert wr.s3._list._prefix_cleanup("?foo") == ""
    assert wr.s3._list._prefix_cleanup("foo?boo") == "foo"
    assert wr.s3._list._prefix_cleanup("[]foo") == ""
    assert wr.s3._list._prefix_cleanup("foo[]") == "foo"
    assert wr.s3._list._prefix_cleanup("foo[]boo") == "foo"

    # escaped
    assert wr.s3._list._prefix_cleanup(glob.escape("foo*")) == "foo"
    assert wr.s3._list._prefix_cleanup(glob.escape("*foo")) == ""
    assert wr.s3._list._prefix_cleanup(glob.escape("foo*boo")) == "foo"
    assert wr.s3._list._prefix_cleanup(glob.escape("foo?")) == "foo"
    assert wr.s3._list._prefix_cleanup(glob.escape("?foo")) == ""
    assert wr.s3._list._prefix_cleanup(glob.escape("foo?boo")) == "foo"
    assert wr.s3._list._prefix_cleanup(glob.escape("[]foo")) == glob.escape("")
    assert wr.s3._list._prefix_cleanup(glob.escape("foo[]")) == glob.escape("foo")
    assert wr.s3._list._prefix_cleanup(glob.escape("foo[]boo")) == glob.escape("foo")


def test_prefix_list(path):
    df = pd.DataFrame({"c0": [0]})
    prefixes = ["foo1boo", "foo2boo", "foo3boo", "foo10boo", "foo*boo", "abc1boo", "foo1abc"]
    paths = [path + p for p in prefixes]
    for p in paths:
        wr.s3.to_parquet(df=df, path=p)
    assert len(wr.s3.list_objects(path + "*")) == 7
    assert len(wr.s3.list_objects(path + "foo*")) == 6
    assert len(wr.s3.list_objects(path + "*boo")) == 6
    assert len(wr.s3.list_objects(path + "foo?boo")) == 4
    assert len(wr.s3.list_objects(path + "foo*boo")) == 5
    assert len(wr.s3.list_objects(path + "foo[12]boo")) == 2
