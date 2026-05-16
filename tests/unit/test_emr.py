import logging
import time

import pytest

import awswrangler as wr

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


def emr_terminate_cluster_and_wait(cluster_id: str, timeout: int = 300, poll_interval: int = 10):
    wr.emr.terminate_cluster(cluster_id=cluster_id)
    for i in range(int(timeout / poll_interval)):
        if "TERMINATED" not in wr.emr.get_cluster_state(cluster_id=cluster_id):
            time.sleep(poll_interval)
        else:
            return
    raise Exception(f"cluster: {cluster_id} failed to terminate")


def test_cluster(bucket, cloudformation_outputs, emr_security_configuration):
    steps = []
    for cmd in ['echo "Hello"', "ls -la"]:
        steps.append(wr.emr.build_step(name=cmd, command=cmd))
    cluster_id = wr.emr.create_cluster(
        cluster_name="wrangler_cluster",
        logging_s3_path=f"s3://{bucket}/emr-logs/",
        emr_release="emr-6.7.0",
        subnet_id=cloudformation_outputs["PublicSubnet1"],
        emr_ec2_role="EMR_EC2_DefaultRole",
        emr_role="EMR_DefaultRole",
        instance_type_master="m5.xlarge",
        instance_type_core="m5.xlarge",
        instance_type_task="m5.xlarge",
        instance_ebs_size_master=50,
        instance_ebs_size_core=50,
        instance_ebs_size_task=50,
        instance_num_on_demand_master=1,
        instance_num_on_demand_core=1,
        instance_num_on_demand_task=1,
        instance_num_spot_master=0,
        instance_num_spot_core=1,
        instance_num_spot_task=1,
        spot_bid_percentage_of_on_demand_master=100,
        spot_bid_percentage_of_on_demand_core=100,
        spot_bid_percentage_of_on_demand_task=100,
        spot_provisioning_timeout_master=5,
        spot_provisioning_timeout_core=5,
        spot_provisioning_timeout_task=5,
        spot_timeout_to_on_demand_master=True,
        spot_timeout_to_on_demand_core=True,
        spot_timeout_to_on_demand_task=True,
        python3=True,
        spark_glue_catalog=True,
        hive_glue_catalog=True,
        presto_glue_catalog=True,
        bootstraps_paths=None,
        debugging=True,
        applications=["Hadoop", "Spark", "Ganglia", "Hive"],
        visible_to_all_users=True,
        keep_cluster_alive_when_no_steps=False,
        key_pair_name=None,
        spark_pyarrow=True,
        steps=steps,
        security_configuration=emr_security_configuration,
    )
    time.sleep(10)
    cluster_state = wr.emr.get_cluster_state(cluster_id=cluster_id)
    assert cluster_state == "STARTING"
    step_id = wr.emr.submit_step(
        cluster_id=cluster_id, name="step_test", command="s3://...script.sh arg1 arg2", script=True
    )
    time.sleep(10)
    step_state = wr.emr.get_step_state(cluster_id=cluster_id, step_id=step_id)
    assert step_state == "PENDING"
    emr_terminate_cluster_and_wait(cluster_id=cluster_id)
    wr.s3.delete_objects(f"s3://{bucket}/emr-logs/")


def test_cluster_single_node(bucket, cloudformation_outputs, emr_security_configuration):
    cluster_id = wr.emr.create_cluster(
        cluster_name="wrangler_cluster",
        logging_s3_path=f"s3://{bucket}/emr-logs/",
        emr_release="emr-6.7.0",
        subnet_id=cloudformation_outputs["PublicSubnet1"],
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
        spark_jars_path=[f"s3://{bucket}/jars/"],
        spark_defaults={"spark.default.parallelism": "400"},
        maximize_resource_allocation=True,
        keep_cluster_alive_when_no_steps=False,
        termination_protected=False,
        spark_pyarrow=False,
        security_configuration=emr_security_configuration,
        tags={"foo": "boo", "bar": "xoo"},
    )
    time.sleep(10)
    cluster_state = wr.emr.get_cluster_state(cluster_id=cluster_id)
    assert cluster_state == "STARTING"
    steps = []
    for cmd in ['echo "Hello"', "ls -la"]:
        steps.append(wr.emr.build_step(name=cmd, command=cmd))
    wr.emr.submit_steps(cluster_id=cluster_id, steps=steps)
    emr_terminate_cluster_and_wait(cluster_id=cluster_id)
    wr.s3.delete_objects(f"s3://{bucket}/emr-logs/")


def test_default_logging_path(cloudformation_outputs):
    path = wr.emr._get_default_logging_path(subnet_id=cloudformation_outputs["PublicSubnet1"])
    assert path.startswith("s3://aws-logs-")
    assert path.endswith("/elasticmapreduce/")
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.emr._get_default_logging_path()


def test_docker(bucket, cloudformation_outputs, emr_security_configuration):
    cluster_id = wr.emr.create_cluster(
        subnet_id=cloudformation_outputs["PublicSubnet1"],
        docker=True,
        custom_classifications=[
            {
                "Classification": "livy-conf",
                "Properties": {
                    "livy.spark.master": "yarn",
                    "livy.spark.deploy-mode": "cluster",
                    "livy.server.session.timeout": "16h",
                },
            }
        ],
        steps=[wr.emr.build_step("spark-submit --deploy-mode cluster s3://bucket/emr.py")],
        security_configuration=emr_security_configuration,
    )
    wr.emr.submit_ecr_credentials_refresh(cluster_id, path=f"s3://{bucket}/emr/")
    wr.emr.submit_steps(
        cluster_id=cluster_id,
        steps=[
            wr.emr.build_spark_step(
                path=f"s3://{bucket}/emr/test_docker.py",
                docker_image="123456789123.dkr.ecr.us-east-1.amazonaws.com/docker-emr:docker-emr",
            )
        ],
    )
    wr.emr.submit_spark_step(cluster_id=cluster_id, path=f"s3://{bucket}/emr/test_docker.py")
    emr_terminate_cluster_and_wait(cluster_id=cluster_id)


@pytest.mark.parametrize(
    "version, result",
    [("emr-6.8.0", "spark-log4j2"), ("emr-6.0", "spark-log4j"), ("emr-8", "spark-log4j"), ("Emr-98", "spark-log4j")],
)
def test_get_emr_integer_version(version, result):
    assert wr.emr._get_emr_classification_lib(version) == result


def test_build_bootstrap_actions_paths_only():
    """Strings remain backward compatible: produce {Name, ScriptBootstrapAction:{Path}}."""
    actions = wr.emr._build_bootstrap_actions(["s3://bucket/a.sh", "s3://bucket/b.sh"])
    assert actions == [
        {"Name": "s3://bucket/a.sh", "ScriptBootstrapAction": {"Path": "s3://bucket/a.sh"}},
        {"Name": "s3://bucket/b.sh", "ScriptBootstrapAction": {"Path": "s3://bucket/b.sh"}},
    ]


def test_build_bootstrap_actions_dicts_with_args():
    """Dict entries pass through args and accept an optional name."""
    actions = wr.emr._build_bootstrap_actions(
        [
            {"path": "s3://bucket/a.sh", "args": ["--flag", "value"], "name": "install"},
            {"path": "s3://bucket/b.sh"},
        ]
    )
    assert actions == [
        {
            "Name": "install",
            "ScriptBootstrapAction": {"Path": "s3://bucket/a.sh", "Args": ["--flag", "value"]},
        },
        {
            "Name": "s3://bucket/b.sh",
            "ScriptBootstrapAction": {"Path": "s3://bucket/b.sh"},
        },
    ]


def test_build_bootstrap_actions_mixed_str_and_dict():
    """A list may freely mix legacy strings and new dict entries."""
    actions = wr.emr._build_bootstrap_actions(
        ["s3://bucket/legacy.sh", {"path": "s3://bucket/new.sh", "args": ["--x"]}]
    )
    assert actions == [
        {"Name": "s3://bucket/legacy.sh", "ScriptBootstrapAction": {"Path": "s3://bucket/legacy.sh"}},
        {"Name": "s3://bucket/new.sh", "ScriptBootstrapAction": {"Path": "s3://bucket/new.sh", "Args": ["--x"]}},
    ]


def test_build_bootstrap_actions_invalid_type():
    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.emr._build_bootstrap_actions([123])  # type: ignore[list-item]


def test_build_bootstrap_actions_missing_path():
    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.emr._build_bootstrap_actions([{"args": ["--x"]}])


def test_build_bootstrap_actions_invalid_args():
    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.emr._build_bootstrap_actions([{"path": "s3://bucket/a.sh", "args": "not-a-list"}])


def test_build_bootstrap_actions_accepts_capitalized_keys():
    """Allow the boto3-style keys (Path/Args/Name) to be passed straight through."""
    actions = wr.emr._build_bootstrap_actions([{"Path": "s3://bucket/a.sh", "Args": ["--x"], "Name": "boto-style"}])
    assert actions == [
        {"Name": "boto-style", "ScriptBootstrapAction": {"Path": "s3://bucket/a.sh", "Args": ["--x"]}},
    ]


def test_create_cluster_passes_bootstrap_args_to_run_job_flow():
    """End-to-end: ensure ``BootstrapActions`` are forwarded to ``run_job_flow`` with Args."""
    import boto3
    import moto

    with moto.mock_aws():
        ec2 = boto3.client("ec2", region_name="us-east-1")
        vpc_id = ec2.create_vpc(CidrBlock="10.0.0.0/16")["Vpc"]["VpcId"]
        subnet_id = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.0.0.0/24", AvailabilityZone="us-east-1a")["Subnet"][
            "SubnetId"
        ]
        session = boto3.Session(region_name="us-east-1")
        cluster_id = wr.emr.create_cluster(
            subnet_id=subnet_id,
            bootstraps_paths=[
                "s3://bucket/legacy.sh",
                {"path": "s3://bucket/new.sh", "args": ["--flag", "v"], "name": "with-args"},
            ],
            boto3_session=session,
        )

        emr_client = session.client("emr")
        described = emr_client.describe_cluster(ClusterId=cluster_id)
        assert described["Cluster"]["Id"] == cluster_id

        bootstrap_response = emr_client.list_bootstrap_actions(ClusterId=cluster_id)
        actions = {a["Name"]: a for a in bootstrap_response["BootstrapActions"]}
        assert actions["s3://bucket/legacy.sh"]["ScriptPath"] == "s3://bucket/legacy.sh"
        assert actions["s3://bucket/legacy.sh"].get("Args", []) == []
        assert actions["with-args"]["ScriptPath"] == "s3://bucket/new.sh"
        assert actions["with-args"]["Args"] == ["--flag", "v"]
