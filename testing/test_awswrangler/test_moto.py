import boto3
import botocore
import mock
import moto
import pytest
from botocore.exceptions import ClientError

import awswrangler as wr

from ._utils import ensure_data_types, get_df_csv, get_df_list


@pytest.fixture(scope="module")
def s3():
    with moto.mock_s3():
        boto3.resource("s3").create_bucket(Bucket="bucket")
        yield True


@pytest.fixture(scope="module")
def emr():
    with moto.mock_emr():
        yield True


@pytest.fixture(scope="module")
def sts():
    with moto.mock_sts():
        yield True


@pytest.fixture(scope="module")
def subnet():
    with moto.mock_ec2():
        ec2 = boto3.resource("ec2", region_name="us-west-1")
        vpc = ec2.create_vpc(CidrBlock="10.0.0.0/16")
        subnet = ec2.create_subnet(VpcId=vpc.id, CidrBlock="10.0.0.0/24", AvailabilityZone="us-west-1a")
        yield subnet.id


def test_csv(s3):
    path = "s3://bucket/test.csv"
    wr.s3.to_csv(df=get_df_csv(), path=path, index=False)
    df = wr.s3.read_csv(path=path)
    assert len(df.index) == 3
    assert len(df.columns) == 10


def test_parquet(s3):
    path = "s3://bucket/test.parquet"
    wr.s3.to_parquet(df=get_df_list(), path=path, index=False, dataset=True, partition_cols=["par0", "par1"])
    df = wr.s3.read_parquet(path=path, dataset=True)
    ensure_data_types(df, has_list=True)
    assert len(df.index) == 3
    assert len(df.columns) == 18


def test_s3_delete_object_success(s3):
    path = "s3://bucket/test.parquet"
    wr.s3.to_parquet(df=get_df_list(), path=path, index=False, dataset=True, partition_cols=["par0", "par1"])
    df = wr.s3.read_parquet(path=path, dataset=True)
    ensure_data_types(df, has_list=True)

    wr.s3.delete_objects(path=path)
    with pytest.raises(OSError):
        wr.s3.read_parquet(path=path, dataset=True)


def test_s3_raise_delete_object_exception_success(s3):
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


def test_emr(s3, emr, sts, subnet):
    session = boto3.Session(region_name="us-west-1")
    cluster_id = wr.emr.create_cluster(
        cluster_name="wrangler_cluster",
        logging_s3_path="s3://bucket/emr-logs/",
        emr_release="emr-5.29.0",
        subnet_id=subnet,
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
