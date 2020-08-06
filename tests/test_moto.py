import logging
import os
from unittest import mock
from unittest.mock import ANY

import boto3
import botocore
import moto
import pandas as pd
import pytest
from botocore.exceptions import ClientError

import awswrangler as wr
from awswrangler.exceptions import EmptyDataFrame, InvalidArgumentCombination

from ._utils import ensure_data_types, get_df_csv, get_df_list

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


@pytest.fixture(scope="module")
def moto_emr():
    with moto.mock_emr():
        yield True


@pytest.fixture(scope="module")
def moto_sts():
    with moto.mock_sts():
        yield True


@pytest.fixture(scope="module")
def moto_subnet():
    with moto.mock_ec2():
        ec2 = boto3.resource("ec2", region_name="us-west-1")
        vpc = ec2.create_vpc(CidrBlock="10.0.0.0/16")
        subnet = ec2.create_subnet(VpcId=vpc.id, CidrBlock="10.0.0.0/24", AvailabilityZone="us-west-1a")
        yield subnet.id


@pytest.fixture(scope="function")
def moto_s3():
    with moto.mock_s3():
        s3 = boto3.resource("s3")
        s3.create_bucket(Bucket="bucket")
        yield s3


@pytest.fixture(scope="module")
def moto_glue():
    with moto.mock_glue():
        region_name = "us-east-1"
        os.environ["AWS_DEFAULT_REGION"] = region_name
        glue = boto3.client("glue", region_name=region_name)
        yield glue


def get_content_md5(desc: dict):
    result = desc.get("ResponseMetadata").get("HTTPHeaders").get("content-md5")
    return result


def test_get_bucket_region_succeed(moto_s3):
    region = wr.s3.get_bucket_region("bucket", boto3_session=boto3.Session())
    assert region == "us-east-1"


def test_object_not_exist_succeed(moto_s3):
    result = wr.s3.does_object_exist("s3://bucket/test.csv")
    assert result is False


def test_object_exist_succeed(moto_s3):
    path = "s3://bucket/test.csv"
    wr.s3.to_csv(df=get_df_csv(), path=path, index=False)
    result = wr.s3.does_object_exist(path)
    assert result is True


def test_list_directories_succeed(moto_s3):
    path = "s3://bucket"
    s3_object1 = moto_s3.Object("bucket", "foo/foo.tmp")
    s3_object2 = moto_s3.Object("bucket", "bar/bar.tmp")
    s3_object1.put(Body=b"foo")
    s3_object2.put(Body=b"bar")

    dirs = wr.s3.list_directories(path)
    files = wr.s3.list_objects(path)

    assert sorted(dirs) == sorted(["s3://bucket/foo/", "s3://bucket/bar/"])
    assert sorted(files) == sorted(["s3://bucket/foo/foo.tmp", "s3://bucket/bar/bar.tmp"])


def test_describe_no_object_succeed(moto_s3):

    desc = wr.s3.describe_objects("s3://bucket")

    assert isinstance(desc, dict)
    assert desc == {}


def test_describe_one_object_succeed(moto_s3):
    bucket = "bucket"
    key = "foo/foo.tmp"
    s3_object = moto_s3.Object(bucket, key)
    s3_object.put(Body=b"foo")

    desc = wr.s3.describe_objects("s3://{}/{}".format(bucket, key))

    assert isinstance(desc, dict)
    assert list(desc.keys()) == ["s3://bucket/foo/foo.tmp"]


def test_describe_list_of_objects_succeed(moto_s3):
    bucket = "bucket"
    keys = ["foo/foo.tmp", "bar/bar.tmp"]

    for key in keys:
        s3_object = moto_s3.Object(bucket, key)
        s3_object.put(Body=b"test")

    desc = wr.s3.describe_objects(["s3://{}/{}".format(bucket, key) for key in keys])

    assert isinstance(desc, dict)
    assert sorted(list(desc.keys())) == sorted(["s3://bucket/foo/foo.tmp", "s3://bucket/bar/bar.tmp"])


def test_describe_list_of_objects_under_same_prefix_succeed(moto_s3):
    bucket = "bucket"
    keys = ["foo/foo.tmp", "bar/bar.tmp"]

    for key in keys:
        s3_object = moto_s3.Object(bucket, key)
        s3_object.put(Body=b"test")

    desc = wr.s3.describe_objects("s3://{}".format(bucket))

    assert isinstance(desc, dict)
    assert sorted(list(desc.keys())) == sorted(["s3://bucket/foo/foo.tmp", "s3://bucket/bar/bar.tmp"])


def test_size_objects_without_object_succeed(moto_s3):
    size = wr.s3.size_objects("s3://bucket")

    assert isinstance(size, dict)
    assert size == {}


def test_size_list_of_objects_succeed(moto_s3):
    bucket = "bucket"
    s3_object1 = moto_s3.Object(bucket, "foo/foo.tmp")
    s3_object2 = moto_s3.Object(bucket, "bar/bar.tmp")
    s3_object1.put(Body=b"foofoo")
    s3_object2.put(Body=b"bar")

    size = wr.s3.size_objects("s3://{}".format(bucket))

    assert isinstance(size, dict)
    assert size == {"s3://bucket/foo/foo.tmp": 6, "s3://bucket/bar/bar.tmp": 3}


def test_copy_one_object_without_replace_filename_succeed(moto_s3):
    bucket = "bucket"
    key = "foo/foo.tmp"
    s3_object = moto_s3.Object(bucket, key)
    s3_object.put(Body=b"foo")

    wr.s3.copy_objects(
        paths=["s3://{}/{}".format(bucket, key)],
        source_path="s3://{}/foo".format(bucket),
        target_path="s3://bucket/bar",
    )

    desc_source = wr.s3.describe_objects("s3://bucket/foo/foo.tmp")
    desc_target = wr.s3.describe_objects("s3://bucket/bar/foo.tmp")

    assert get_content_md5(desc_target.get("s3://bucket/bar/foo.tmp")) == get_content_md5(
        desc_source.get("s3://bucket/foo/foo.tmp")
    )


def test_copy_one_object_with_replace_filename_succeed(moto_s3):
    bucket = "bucket"
    key = "foo/foo.tmp"
    s3_object = moto_s3.Object(bucket, key)
    s3_object.put(Body=b"foo")

    wr.s3.copy_objects(
        paths=["s3://{}/{}".format(bucket, key)],
        source_path="s3://{}/foo".format(bucket),
        target_path="s3://bucket/bar",
        replace_filenames={"foo.tmp": "bar.tmp"},
    )

    desc_source = wr.s3.describe_objects("s3://bucket/foo/foo.tmp")
    desc_target = wr.s3.describe_objects("s3://bucket/bar/bar.tmp")

    assert get_content_md5(desc_target.get("s3://bucket/bar/bar.tmp")) == get_content_md5(
        desc_source.get("s3://bucket/foo/foo.tmp")
    )


def test_copy_objects_without_replace_filename_succeed(moto_s3):
    bucket = "bucket"
    keys = ["foo/foo1.tmp", "foo/foo2.tmp", "foo/foo3.tmp"]

    for key in keys:
        s3_object = moto_s3.Object(bucket, key)
        s3_object.put(Body=b"foo")

    wr.s3.copy_objects(
        paths=["s3://{}/{}".format(bucket, key) for key in keys],
        source_path="s3://{}/foo".format(bucket),
        target_path="s3://bucket/bar",
    )

    desc_source = wr.s3.describe_objects("s3://{}/foo".format(bucket))
    desc_target = wr.s3.describe_objects("s3://{}/bar".format(bucket))

    assert isinstance(desc_target, dict)
    assert len(desc_source) == 3
    assert len(desc_target) == 3
    assert sorted(list(desc_target.keys())) == sorted(
        ["s3://bucket/bar/foo1.tmp", "s3://bucket/bar/foo2.tmp", "s3://bucket/bar/foo3.tmp"]
    )


def test_csv(moto_s3):
    path = "s3://bucket/test.csv"
    wr.s3.to_csv(df=get_df_csv(), path=path, index=False)
    df = wr.s3.read_csv(path=path)
    assert len(df.index) == 3
    assert len(df.columns) == 10


def test_read_csv_with_chucksize_and_pandas_arguments(moto_s3):
    path = "s3://bucket/test.csv"
    wr.s3.to_csv(df=get_df_csv(), path=path, index=False)
    dfs = [dfs for dfs in wr.s3.read_csv(path=path, chunksize=1, usecols=["id", "string"])]
    assert len(dfs) == 3
    for df in dfs:
        assert len(df.columns) == 2


@mock.patch("pandas.read_csv")
@mock.patch("s3fs.S3FileSystem.open")
def test_read_csv_pass_pandas_arguments_and_encoding_succeed(mock_open, mock_read_csv, moto_s3):
    bucket = "bucket"
    key = "foo/foo.csv"
    path = "s3://{}/{}".format(bucket, key)
    s3_object = moto_s3.Object(bucket, key)
    s3_object.put(Body=b"foo")
    wr.s3.read_csv(path=path, encoding="ISO-8859-1", sep=",", lineterminator="\r\n")
    mock_open.assert_called_with(path="s3://bucket/foo/foo.csv", mode="r", encoding="ISO-8859-1", newline="\r\n")
    mock_read_csv.assert_called_with(ANY, compression=None, encoding="ISO-8859-1", sep=",", lineterminator="\r\n")


def test_to_csv_invalid_argument_combination_raise_when_dataset_false_succeed(moto_s3):
    path = "s3://bucket/test.csv"

    with pytest.raises(InvalidArgumentCombination):
        wr.s3.to_csv(df=get_df_csv(), path=path, index=False, dataset=False, partition_cols=["par0", "par1"])

    with pytest.raises(InvalidArgumentCombination):
        wr.s3.to_csv(df=get_df_csv(), path=path, index=False, dataset=False, mode="append")

    with pytest.raises(InvalidArgumentCombination):
        wr.s3.to_csv(df=get_df_csv(), path=path, index=False, dataset=False, partition_cols=["par0", "par1"])

    with pytest.raises(InvalidArgumentCombination):
        wr.s3.to_csv(df=get_df_csv(), path=path, index=False, dataset=False, database="default", table="test")

    with pytest.raises(InvalidArgumentCombination):
        wr.s3.to_csv(df=get_df_csv(), path=path, index=False, dataset=False, description="raise exception")

    with pytest.raises(InvalidArgumentCombination):
        wr.s3.to_csv(df=get_df_csv(), path=path, index=False, dataset=False, parameters={"key": "value"})

    with pytest.raises(InvalidArgumentCombination):
        wr.s3.to_csv(df=get_df_csv(), path=path, index=False, dataset=False, columns_comments={"col0": "test"})


def test_to_csv_valid_argument_combination_when_dataset_true_succeed(moto_s3):
    path = "s3://bucket/test.csv"
    wr.s3.to_csv(df=get_df_csv(), path=path, index=False)
    wr.s3.to_csv(df=get_df_csv(), path=path, index=False, dataset=True, partition_cols=["par0", "par1"])

    wr.s3.to_csv(df=get_df_csv(), path=path, index=False, dataset=True, mode="append")

    wr.s3.to_csv(df=get_df_csv(), path=path, index=False, dataset=True, description="raise exception")

    wr.s3.to_csv(df=get_df_csv(), path=path, index=False, dataset=True, parameters={"key": "value"})

    wr.s3.to_csv(df=get_df_csv(), path=path, index=False, dataset=True, columns_comments={"col0": "test"})


def test_to_csv_data_empty_raise_succeed(moto_s3):
    path = "s3://bucket/test.csv"
    with pytest.raises(EmptyDataFrame):
        wr.s3.to_csv(df=pd.DataFrame(), path=path, index=False)


def test_parquet(moto_s3):
    path = "s3://bucket/test.parquet"
    wr.s3.to_parquet(df=get_df_list(), path=path, index=False, dataset=True, partition_cols=["par0", "par1"])
    df = wr.s3.read_parquet(path=path, dataset=True)
    ensure_data_types(df, has_list=True)
    assert df.shape == (3, 19)


def test_s3_delete_object_success(moto_s3):
    path = "s3://bucket/test.parquet"
    wr.s3.to_parquet(df=get_df_list(), path=path, index=False, dataset=True, partition_cols=["par0", "par1"])
    df = wr.s3.read_parquet(path=path, dataset=True)
    ensure_data_types(df, has_list=True)
    wr.s3.delete_objects(path=path)
    with pytest.raises(wr.exceptions.NoFilesFound):
        wr.s3.read_parquet(path=path, dataset=True)


def test_s3_raise_delete_object_exception_success(moto_s3):
    path = "s3://bucket/test.parquet"
    wr.s3.to_parquet(df=get_df_list(), path=path, index=False, dataset=True, partition_cols=["par0", "par1"])
    df = wr.s3.read_parquet(path=path, dataset=True)
    ensure_data_types(df, has_list=True)

    call = botocore.client.BaseClient._make_api_call

    def mock_make_api_call(self, operation_name, kwarg):
        if operation_name == "DeleteObjects":
            parsed_response = {"Error": {"Code": "500", "Message": "Test Error"}}
            raise ClientError(parsed_response, operation_name)
        return call(self, operation_name, kwarg)

    with mock.patch("botocore.client.BaseClient._make_api_call", new=mock_make_api_call):
        with pytest.raises(ClientError):
            wr.s3.delete_objects(path=path)


def test_emr(moto_s3, moto_emr, moto_sts, moto_subnet):
    session = boto3.Session(region_name="us-west-1")
    cluster_id = wr.emr.create_cluster(
        cluster_name="wrangler_cluster",
        logging_s3_path="s3://bucket/emr-logs/",
        emr_release="emr-5.29.0",
        subnet_id=moto_subnet,
        emr_ec2_role="EMR_EC2_DefaultRole",
        emr_role="EMR_DefaultRole",
        instance_type_master="m5.xlarge",
        instance_type_core="m5.xlarge",
        instance_type_task="m5.xlarge",
        instance_ebs_size_master=50,
        instance_ebs_size_core=50,
        instance_ebs_size_task=50,
        instance_num_on_demand_master=1,
        instance_num_on_demand_core=0,
        instance_num_on_demand_task=0,
        instance_num_spot_master=0,
        instance_num_spot_core=0,
        instance_num_spot_task=0,
        spot_bid_percentage_of_on_demand_master=100,
        spot_bid_percentage_of_on_demand_core=100,
        spot_bid_percentage_of_on_demand_task=100,
        spot_provisioning_timeout_master=5,
        spot_provisioning_timeout_core=5,
        spot_provisioning_timeout_task=5,
        spot_timeout_to_on_demand_master=False,
        spot_timeout_to_on_demand_core=False,
        spot_timeout_to_on_demand_task=False,
        python3=False,
        spark_glue_catalog=False,
        hive_glue_catalog=False,
        presto_glue_catalog=False,
        consistent_view=True,
        consistent_view_retry_count=6,
        consistent_view_retry_seconds=15,
        consistent_view_table_name="EMRConsistentView",
        bootstraps_paths=None,
        debugging=False,
        applications=["Hadoop", "Spark", "Ganglia", "Hive"],
        visible_to_all_users=True,
        key_pair_name=None,
        spark_log_level="ERROR",
        spark_jars_path=["s3://bucket/jars/"],
        spark_defaults={"spark.default.parallelism": "400"},
        maximize_resource_allocation=True,
        keep_cluster_alive_when_no_steps=False,
        termination_protected=False,
        spark_pyarrow=False,
        tags={"foo": "boo", "bar": "xoo"},
        boto3_session=session,
    )
    wr.emr.get_cluster_state(cluster_id=cluster_id, boto3_session=session)
    steps = []
    for cmd in ['echo "Hello"', "ls -la"]:
        steps.append(wr.emr.build_step(name=cmd, command=cmd))
    wr.emr.submit_steps(cluster_id=cluster_id, steps=steps, boto3_session=session)
    wr.emr.terminate_cluster(cluster_id=cluster_id, boto3_session=session)
    wr.s3.delete_objects("s3://bucket/emr-logs/")


def test_glue_get_partition(moto_glue):
    database_name = "mydb"
    table_name = "mytable"
    values = {"s3://bucket/prefix/dt=2020-01-01": ["2020-01-01"]}

    wr.catalog.create_database(name=database_name)
    wr.catalog.create_parquet_table(
        database=database_name,
        table=table_name,
        path="s3://bucket/prefix/",
        columns_types={"col0": "bigint", "col1": "double"},
        partitions_types={"dt": "date"},
    )
    wr.catalog.add_parquet_partitions(database=database_name, table=table_name, partitions_values=values)

    partition_value = wr.catalog.get_partitions(database_name, table_name)
    assert partition_value == values
    parquet_partition_value = wr.catalog.get_parquet_partitions(database_name, table_name)
    assert parquet_partition_value == values
