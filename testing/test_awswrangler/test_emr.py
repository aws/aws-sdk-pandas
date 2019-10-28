import logging
from time import sleep

import pytest
import boto3

from awswrangler import Session

logging.basicConfig(level=logging.INFO, format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)


@pytest.fixture(scope="module")
def cloudformation_outputs():
    response = boto3.client("cloudformation").describe_stacks(StackName="aws-data-wrangler-test-arena")
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
        bucket = cloudformation_outputs["BucketName"]
        session.s3.delete_objects(path=f"s3://{bucket}/")
    else:
        raise Exception("You must deploy the test infrastructure using Cloudformation!")
    yield bucket
    session.s3.delete_objects(path=f"s3://{bucket}/")


def test_cluster(session, bucket, cloudformation_outputs):
    steps = []
    for cmd in ['echo "Hello"', "ls -la"]:
        steps.append(session.emr.build_step(name=cmd, command=cmd))
    cluster_id = session.emr.create_cluster(cluster_name="wrangler_cluster",
                                            logging_s3_path=f"s3://{bucket}/emr-logs/",
                                            emr_release="emr-5.27.0",
                                            subnet_id=cloudformation_outputs["SubnetId"],
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
                                            key_pair_name=None,
                                            steps=steps)
    sleep(10)
    cluster_state = session.emr.get_cluster_state(cluster_id=cluster_id)
    print(f"cluster_state: {cluster_state}")
    assert cluster_state == "STARTING"
    step_id = session.emr.submit_step(cluster_id=cluster_id,
                                      name="step_test",
                                      command="s3://...script.sh arg1 arg2",
                                      script=True)
    sleep(10)
    step_state = session.emr.get_step_state(cluster_id=cluster_id, step_id=step_id)
    print(f"step_state: {step_state}")
    assert step_state == "PENDING"
    session.emr.terminate_cluster(cluster_id=cluster_id)


def test_cluster_single_node(session, bucket, cloudformation_outputs):
    cluster_id = session.emr.create_cluster(cluster_name="wrangler_cluster",
                                            logging_s3_path=f"s3://{bucket}/emr-logs/",
                                            emr_release="emr-5.27.0",
                                            subnet_id=cloudformation_outputs["SubnetId"],
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
                                            termination_protected=False)
    sleep(10)
    cluster_state = session.emr.get_cluster_state(cluster_id=cluster_id)
    print(f"cluster_state: {cluster_state}")
    assert cluster_state == "STARTING"
    steps = []
    for cmd in ['echo "Hello"', "ls -la"]:
        steps.append(session.emr.build_step(name=cmd, command=cmd))
    session.emr.submit_steps(cluster_id=cluster_id, steps=steps)
    session.emr.terminate_cluster(cluster_id=cluster_id)
