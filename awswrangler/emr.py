"""EMR (Elastic Map Reduce) module."""
# pylint: disable=line-too-long

import logging
import pprint
from typing import Any, Dict, List, Optional, Union, cast

import boto3

from awswrangler import _utils, exceptions, sts

_logger: logging.Logger = logging.getLogger(__name__)


def _get_ecr_credentials_refresh_content(region: str) -> str:
    return f"""
import subprocess
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("ECR Setup Job").getOrCreate()

COMMANDS = [
    "sudo -s eval $(aws ecr get-login --region {region} --no-include-email)",
    "sudo hdfs dfs -put -f /root/.docker/config.json /user/hadoop/"
]

for command in COMMANDS:
    subprocess.run(command.split(" "), timeout=6.0, check=True)

print("done!")
    """


def _get_default_logging_path(
    subnet_id: Optional[str] = None,
    account_id: Optional[str] = None,
    region: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> str:
    """Get EMR default logging path.

    E.g. "s3://aws-logs-{account_id}-{region}/elasticmapreduce/"

    Parameters
    ----------
    subnet_id : str, optional
        Subnet ID. If not provided, you must pass `account_id` and `region` explicit.
    account_id: str, optional
        Account ID.
    region: str, optional
        Region e.g. 'us-east-1'
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    str
        Default logging path.
        E.g. "s3://aws-logs-{account_id}-{region}/elasticmapreduce/"

    Examples
    --------
    >>> import awswrangler as wr
    >>> state = wr.emr._get_default_logging_path("subnet-id")
    's3://aws-logs-{account_id}-{region}/elasticmapreduce/'

    """
    if account_id is None:
        boto3_session = _utils.ensure_session(session=boto3_session)
        _account_id: str = sts.get_account_id(boto3_session=boto3_session)
    else:
        _account_id = account_id
    if (region is None) and (subnet_id is not None):
        _region: str = _utils.get_region_from_session(boto3_session=boto3_session)
    elif (region is None) and (subnet_id is None):
        raise exceptions.InvalidArgumentCombination("You must pass region or subnet_id or both.")
    else:
        _region = region  # type: ignore
    return f"s3://aws-logs-{_account_id}-{_region}/elasticmapreduce/"


def _build_cluster_args(**pars: Any) -> Dict[str, Any]:  # pylint: disable=too-many-branches,too-many-statements
    account_id: str = sts.get_account_id(boto3_session=pars["boto3_session"])
    region: str = _utils.get_region_from_session(boto3_session=pars["boto3_session"])

    # S3 Logging path
    if pars.get("logging_s3_path") is None:
        pars["logging_s3_path"] = _get_default_logging_path(
            subnet_id=None, account_id=account_id, region=region, boto3_session=pars["boto3_session"]
        )

    spark_env: Optional[Dict[str, str]] = None
    yarn_env: Optional[Dict[str, str]] = None
    livy_env: Optional[Dict[str, str]] = None

    if pars["spark_pyarrow"] is True:
        if pars["spark_defaults"] is None:
            pars["spark_defaults"] = {"spark.sql.execution.arrow.enabled": "true"}
        else:
            pars["spark_defaults"]["spark.sql.execution.arrow.enabled"] = "true"
        spark_env = {"ARROW_PRE_0_15_IPC_FORMAT": "1"}
        yarn_env = {"ARROW_PRE_0_15_IPC_FORMAT": "1"}
        livy_env = {"ARROW_PRE_0_15_IPC_FORMAT": "1"}

    if pars["python3"] is True:
        if spark_env is None:
            spark_env = {"PYSPARK_PYTHON": "/usr/bin/python3"}
        else:
            spark_env["PYSPARK_PYTHON"] = "/usr/bin/python3"

    if pars["spark_jars_path"] is not None:
        paths: str = ",".join(pars["spark_jars_path"])
        if pars["spark_defaults"] is None:
            pars["spark_defaults"] = {"spark.jars": paths}
        else:
            pars["spark_defaults"]["spark.jars"] = paths

    args: Dict[str, Any] = {
        "Name": pars["cluster_name"],
        "LogUri": pars["logging_s3_path"],
        "ReleaseLabel": pars["emr_release"],
        "VisibleToAllUsers": pars["visible_to_all_users"],
        "JobFlowRole": pars["emr_ec2_role"],
        "ServiceRole": pars["emr_role"],
        "Instances": {
            "KeepJobFlowAliveWhenNoSteps": pars["keep_cluster_alive_when_no_steps"],
            "TerminationProtected": pars["termination_protected"],
            "Ec2SubnetId": pars["subnet_id"],
            "InstanceFleets": [],
        },
    }

    # EC2 Key Pair
    if pars["key_pair_name"] is not None:
        args["Instances"]["Ec2KeyName"] = pars["key_pair_name"]

    # Security groups
    if pars["security_group_master"] is not None:
        args["Instances"]["EmrManagedMasterSecurityGroup"] = pars["security_group_master"]
    if pars["security_groups_master_additional"] is not None:
        args["Instances"]["AdditionalMasterSecurityGroups"] = pars["security_groups_master_additional"]
    if pars["security_group_slave"] is not None:
        args["Instances"]["EmrManagedSlaveSecurityGroup"] = pars["security_group_slave"]
    if pars["security_groups_slave_additional"] is not None:
        args["Instances"]["AdditionalSlaveSecurityGroups"] = pars["security_groups_slave_additional"]
    if pars["security_group_service_access"] is not None:
        args["Instances"]["ServiceAccessSecurityGroup"] = pars["security_group_service_access"]

    # Configurations
    args["Configurations"] = [
        {"Classification": "spark-log4j", "Properties": {"log4j.rootCategory": f"{pars['spark_log_level']}, console"}}
    ]
    if pars["docker"] is True:
        if pars.get("extra_registries") is None:
            extra_registries: List[str] = []
        else:
            extra_registries = pars["extra_registries"]
        registries: str = f"local,centos,{account_id}.dkr.ecr.{region}.amazonaws.com,{','.join(extra_registries)}"
        registries = registries[:-1] if registries.endswith(",") else registries
        args["Configurations"].append(
            {
                "Classification": "container-executor",
                "Properties": {},
                "Configurations": [
                    {
                        "Classification": "docker",
                        "Properties": {
                            "docker.privileged-containers.registries": registries,
                            "docker.trusted.registries": registries,
                        },
                        "Configurations": [],
                    }
                ],
            }
        )

    if spark_env is not None:
        args["Configurations"].append(
            {
                "Classification": "spark-env",
                "Properties": {},
                "Configurations": [{"Classification": "export", "Properties": spark_env, "Configurations": []}],
            }
        )
    if yarn_env is not None:
        args["Configurations"].append(
            {
                "Classification": "yarn-env",
                "Properties": {},
                "Configurations": [{"Classification": "export", "Properties": yarn_env, "Configurations": []}],
            }
        )
    if livy_env is not None:
        args["Configurations"].append(
            {
                "Classification": "livy-env",
                "Properties": {},
                "Configurations": [{"Classification": "export", "Properties": livy_env, "Configurations": []}],
            }
        )
    if pars["spark_glue_catalog"] is True:
        args["Configurations"].append(
            {
                "Classification": "spark-hive-site",
                "Properties": {
                    "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"  # noqa
                },
                "Configurations": [],
            }
        )
    if pars["hive_glue_catalog"] is True:
        hive_conf: Dict[str, Any] = {"Classification": "hive-site", "Properties": {}, "Configurations": []}
        hive_conf["Properties"][
            "hive.metastore.client.factory.class"
        ] = "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
        args["Configurations"].append(hive_conf)
    if pars["presto_glue_catalog"] is True:
        args["Configurations"].append(
            {
                "Classification": "presto-connector-hive",
                "Properties": {"hive.metastore.glue.datacatalog.enabled": "true"},
                "Configurations": [],
            }
        )
    if pars["consistent_view"] is True:
        args["Configurations"].append(
            {
                "Classification": "emrfs-site",
                "Properties": {
                    "fs.s3.consistent.retryPeriodSeconds": str(pars.get("consistent_view_retry_seconds", "10")),
                    "fs.s3.consistent": "true",
                    "fs.s3.consistent.retryCount": str(pars.get("consistent_view_retry_count", "5")),
                    "fs.s3.consistent.metadata.tableName": pars.get("consistent_view_table_name", "EmrFSMetadata"),
                },
            }
        )
    if pars["maximize_resource_allocation"] is True:
        args["Configurations"].append({"Classification": "spark", "Properties": {"maximizeResourceAllocation": "true"}})
    if pars["spark_defaults"] is not None:
        spark_defaults: Dict[str, Union[str, Dict[str, str]]] = {
            "Classification": "spark-defaults",
            "Properties": pars["spark_defaults"],
        }
        args["Configurations"].append(spark_defaults)
    if pars.get("custom_classifications") is not None:
        for c in pars["custom_classifications"]:
            args["Configurations"].append(c)

    # Applications
    if pars["applications"]:
        args["Applications"] = [{"Name": x} for x in pars["applications"]]

    # Bootstraps
    if pars["bootstraps_paths"]:
        args["BootstrapActions"] = [{"Name": x, "ScriptBootstrapAction": {"Path": x}} for x in pars["bootstraps_paths"]]

    # Debugging and Steps
    if (pars["debugging"] is True) or (pars["steps"] is not None):
        args["Steps"] = []
        if pars["debugging"] is True:
            args["Steps"].append(
                {
                    "Name": "Setup Hadoop Debugging",
                    "ActionOnFailure": "TERMINATE_CLUSTER",
                    "HadoopJarStep": {"Jar": "command-runner.jar", "Args": ["state-pusher-script"]},
                }
            )
        if pars["steps"] is not None:
            args["Steps"] += pars["steps"]

    # Master Instance Fleet
    timeout_action_master: str = "SWITCH_TO_ON_DEMAND" if pars[
        "spot_timeout_to_on_demand_master"
    ] else "TERMINATE_CLUSTER"
    fleet_master: Dict[str, Any] = {
        "Name": "MASTER",
        "InstanceFleetType": "MASTER",
        "TargetOnDemandCapacity": pars["instance_num_on_demand_master"],
        "TargetSpotCapacity": pars["instance_num_spot_master"],
        "InstanceTypeConfigs": [
            {
                "InstanceType": pars["instance_type_master"],
                "WeightedCapacity": 1,
                "BidPriceAsPercentageOfOnDemandPrice": pars["spot_bid_percentage_of_on_demand_master"],
                "EbsConfiguration": {
                    "EbsBlockDeviceConfigs": [
                        {
                            "VolumeSpecification": {"SizeInGB": pars["instance_ebs_size_master"], "VolumeType": "gp2"},
                            "VolumesPerInstance": 1,
                        }
                    ],
                    "EbsOptimized": True,
                },
            }
        ],
    }
    if pars["instance_num_spot_master"] > 0:
        fleet_master["LaunchSpecifications"] = {
            "SpotSpecification": {
                "TimeoutDurationMinutes": pars["spot_provisioning_timeout_master"],
                "TimeoutAction": timeout_action_master,
            }
        }
    args["Instances"]["InstanceFleets"].append(fleet_master)

    # Core Instance Fleet
    if (pars["instance_num_spot_core"] > 0) or pars["instance_num_on_demand_core"] > 0:
        timeout_action_core = "SWITCH_TO_ON_DEMAND" if pars["spot_timeout_to_on_demand_core"] else "TERMINATE_CLUSTER"
        fleet_core: Dict[str, Any] = {
            "Name": "CORE",
            "InstanceFleetType": "CORE",
            "TargetOnDemandCapacity": pars["instance_num_on_demand_core"],
            "TargetSpotCapacity": pars["instance_num_spot_core"],
            "InstanceTypeConfigs": [
                {
                    "InstanceType": pars["instance_type_core"],
                    "WeightedCapacity": 1,
                    "BidPriceAsPercentageOfOnDemandPrice": pars["spot_bid_percentage_of_on_demand_core"],
                    "EbsConfiguration": {
                        "EbsBlockDeviceConfigs": [
                            {
                                "VolumeSpecification": {
                                    "SizeInGB": pars["instance_ebs_size_core"],
                                    "VolumeType": "gp2",
                                },
                                "VolumesPerInstance": 1,
                            }
                        ],
                        "EbsOptimized": True,
                    },
                }
            ],
        }
        if pars["instance_num_spot_core"] > 0:
            fleet_core["LaunchSpecifications"] = {
                "SpotSpecification": {
                    "TimeoutDurationMinutes": pars["spot_provisioning_timeout_core"],
                    "TimeoutAction": timeout_action_core,
                }
            }
        args["Instances"]["InstanceFleets"].append(fleet_core)

    # Task Instance Fleet
    if (pars["instance_num_spot_task"] > 0) or pars["instance_num_on_demand_task"] > 0:
        timeout_action_task: str = "SWITCH_TO_ON_DEMAND" if pars[
            "spot_timeout_to_on_demand_task"
        ] else "TERMINATE_CLUSTER"
        fleet_task: Dict[str, Any] = {
            "Name": "TASK",
            "InstanceFleetType": "TASK",
            "TargetOnDemandCapacity": pars["instance_num_on_demand_task"],
            "TargetSpotCapacity": pars["instance_num_spot_task"],
            "InstanceTypeConfigs": [
                {
                    "InstanceType": pars["instance_type_task"],
                    "WeightedCapacity": 1,
                    "BidPriceAsPercentageOfOnDemandPrice": pars["spot_bid_percentage_of_on_demand_task"],
                    "EbsConfiguration": {
                        "EbsBlockDeviceConfigs": [
                            {
                                "VolumeSpecification": {
                                    "SizeInGB": pars["instance_ebs_size_task"],
                                    "VolumeType": "gp2",
                                },
                                "VolumesPerInstance": 1,
                            }
                        ],
                        "EbsOptimized": True,
                    },
                }
            ],
        }
        if pars["instance_num_spot_task"] > 0:
            fleet_task["LaunchSpecifications"] = {
                "SpotSpecification": {
                    "TimeoutDurationMinutes": pars["spot_provisioning_timeout_task"],
                    "TimeoutAction": timeout_action_task,
                }
            }
        args["Instances"]["InstanceFleets"].append(fleet_task)

    # Tags
    if pars["tags"] is not None:
        args["Tags"] = [{"Key": k, "Value": v} for k, v in pars["tags"].items()]

    _logger.debug("args: \n%s", pprint.pformat(args))
    return args


def create_cluster(  # pylint: disable=too-many-arguments,too-many-locals,unused-argument
    subnet_id: str,
    cluster_name: str = "my-emr-cluster",
    logging_s3_path: Optional[str] = None,
    emr_release: str = "emr-6.0.0",
    emr_ec2_role: str = "EMR_EC2_DefaultRole",
    emr_role: str = "EMR_DefaultRole",
    instance_type_master: str = "r5.xlarge",
    instance_type_core: str = "r5.xlarge",
    instance_type_task: str = "r5.xlarge",
    instance_ebs_size_master: int = 64,
    instance_ebs_size_core: int = 64,
    instance_ebs_size_task: int = 64,
    instance_num_on_demand_master: int = 1,
    instance_num_on_demand_core: int = 0,
    instance_num_on_demand_task: int = 0,
    instance_num_spot_master: int = 0,
    instance_num_spot_core: int = 0,
    instance_num_spot_task: int = 0,
    spot_bid_percentage_of_on_demand_master: int = 100,
    spot_bid_percentage_of_on_demand_core: int = 100,
    spot_bid_percentage_of_on_demand_task: int = 100,
    spot_provisioning_timeout_master: int = 5,
    spot_provisioning_timeout_core: int = 5,
    spot_provisioning_timeout_task: int = 5,
    spot_timeout_to_on_demand_master: bool = True,
    spot_timeout_to_on_demand_core: bool = True,
    spot_timeout_to_on_demand_task: bool = True,
    python3: bool = True,
    spark_glue_catalog: bool = True,
    hive_glue_catalog: bool = True,
    presto_glue_catalog: bool = True,
    consistent_view: bool = False,
    consistent_view_retry_seconds: int = 10,
    consistent_view_retry_count: int = 5,
    consistent_view_table_name: str = "EmrFSMetadata",
    bootstraps_paths: Optional[List[str]] = None,
    debugging: bool = True,
    applications: Optional[List[str]] = None,
    visible_to_all_users: bool = True,
    key_pair_name: Optional[str] = None,
    security_group_master: Optional[str] = None,
    security_groups_master_additional: Optional[List[str]] = None,
    security_group_slave: Optional[str] = None,
    security_groups_slave_additional: Optional[List[str]] = None,
    security_group_service_access: Optional[str] = None,
    docker: bool = False,
    extra_public_registries: Optional[List[str]] = None,
    spark_log_level: str = "WARN",
    spark_jars_path: Optional[List[str]] = None,
    spark_defaults: Optional[Dict[str, str]] = None,
    spark_pyarrow: bool = False,
    custom_classifications: Optional[List[Dict[str, Any]]] = None,
    maximize_resource_allocation: bool = False,
    steps: Optional[List[Dict[str, Any]]] = None,
    keep_cluster_alive_when_no_steps: bool = True,
    termination_protected: bool = False,
    tags: Optional[Dict[str, str]] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> str:
    """Create a EMR cluster with instance fleets configuration.

    https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-instance-fleet.html

    Parameters
    ----------
    subnet_id : str
        VPC subnet ID.
    cluster_name : str
        Cluster name.
    logging_s3_path : str, optional
        Logging s3 path (e.g. s3://BUCKET_NAME/DIRECTORY_NAME/).
        If None, the default is `s3://aws-logs-{AccountId}-{RegionId}/elasticmapreduce/`
    emr_release : str
        EMR release (e.g. emr-5.28.0).
    emr_ec2_role : str
        IAM role name.
    emr_role : str
        IAM role name.
    instance_type_master : str
        EC2 instance type.
    instance_type_core : str
        EC2 instance type.
    instance_type_task : str
        EC2 instance type.
    instance_ebs_size_master : int
        Size of EBS in GB.
    instance_ebs_size_core : int
        Size of EBS in GB.
    instance_ebs_size_task : int
        Size of EBS in GB.
    instance_num_on_demand_master : int
        Number of on demand instances.
    instance_num_on_demand_core : int
        Number of on demand instances.
    instance_num_on_demand_task : int
        Number of on demand instances.
    instance_num_spot_master : int
        Number of spot instances.
    instance_num_spot_core : int
        Number of spot instances.
    instance_num_spot_task : int
        Number of spot instances.
    spot_bid_percentage_of_on_demand_master : int
        The bid price, as a percentage of On-Demand price.
    spot_bid_percentage_of_on_demand_core : int
        The bid price, as a percentage of On-Demand price.
    spot_bid_percentage_of_on_demand_task : int
        The bid price, as a percentage of On-Demand price.
    spot_provisioning_timeout_master : int
        The spot provisioning timeout period in minutes.
        If Spot instances are not provisioned within this time period,
        the TimeOutAction is taken. Minimum value is 5 and maximum value is 1440.
        The timeout applies only during initial provisioning,
        when the cluster is first created.
    spot_provisioning_timeout_core : int
        The spot provisioning timeout period in minutes.
        If Spot instances are not provisioned within this time period,
        the TimeOutAction is taken. Minimum value is 5 and maximum value is 1440.
        The timeout applies only during initial provisioning,
        when the cluster is first created.
    spot_provisioning_timeout_task : int
        The spot provisioning timeout period in minutes.
        If Spot instances are not provisioned within this time period,
        the TimeOutAction is taken. Minimum value is 5 and maximum value is 1440.
        The timeout applies only during initial provisioning,
        when the cluster is first created.
    spot_timeout_to_on_demand_master : bool
        After a provisioning timeout should the cluster switch to
        on demand or shutdown?
    spot_timeout_to_on_demand_core : bool
        After a provisioning timeout should the cluster switch to
        on demand or shutdown?
    spot_timeout_to_on_demand_task : bool
        After a provisioning timeout should the cluster switch to
        on demand or shutdown?
    python3 : bool
        Python 3 Enabled?
    spark_glue_catalog : bool
        Spark integration with Glue Catalog?
    hive_glue_catalog : bool
        Hive integration with Glue Catalog?
    presto_glue_catalog : bool
        Presto integration with Glue Catalog?
    consistent_view : bool
        Consistent view allows EMR clusters to check for
        list and read-after-write consistency for
        Amazon S3 objects written by or synced with EMRFS.
        https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-consistent-view.html
    consistent_view_retry_seconds : int
        Delay between the tries (seconds).
    consistent_view_retry_count : int
        Number of tries.
    consistent_view_table_name : str
        Name of the DynamoDB table to store the consistent view data.
    bootstraps_paths : List[str], optional
        Bootstraps paths (e.g ["s3://BUCKET_NAME/script.sh"]).
    debugging : bool
        Debugging enabled?
    applications : List[str], optional
        List of applications (e.g ["Hadoop", "Spark", "Ganglia", "Hive"]).
        If None, ["Spark"] will be considered.
    visible_to_all_users : bool
        True or False.
    key_pair_name : str, optional
        Key pair name.
    security_group_master : str, optional
        The identifier of the Amazon EC2 security group for the master node.
    security_groups_master_additional : str, optional
        A list of additional Amazon EC2 security group IDs for the master node.
    security_group_slave : str, optional
        The identifier of the Amazon EC2 security group for
        the core and task nodes.
    security_groups_slave_additional : str, optional
        A list of additional Amazon EC2 security group IDs for
        the core and task nodes.
    security_group_service_access : str, optional
        The identifier of the Amazon EC2 security group for the Amazon EMR
        service to access clusters in VPC private subnets.
    docker : bool
        Enable Docker Hub and ECR registries access.
    extra_public_registries: List[str], optional
        Additional docker registries.
    spark_log_level : str
        log4j.rootCategory log level (ALL, DEBUG, INFO, WARN, ERROR, FATAL, OFF, TRACE).
    spark_jars_path : List[str], optional
        spark.jars e.g. [s3://.../foo.jar, s3://.../boo.jar]
        https://spark.apache.org/docs/latest/configuration.html
    spark_defaults : Dict[str, str], optional
        https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-configure.html#spark-defaults
    spark_pyarrow : bool
        Enable PySpark to use PyArrow behind the scenes.
        P.S. You must install pyarrow by your self via bootstrap
    custom_classifications: List[Dict[str, Any]], optional
        Extra classifications.
    maximize_resource_allocation : bool
        Configure your executors to utilize the maximum resources possible
        https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-configure.html#emr-spark-maximizeresourceallocation
    steps : List[Dict[str, Any]], optional
        Steps definitions (Obs : str Use EMR.build_step() to build it)
    keep_cluster_alive_when_no_steps : bool
        Specifies whether the cluster should
        remain available after completing all steps
    termination_protected : bool
        Specifies whether the Amazon EC2 instances in the cluster are
        protected from termination by API calls, user intervention,
        or in the event of a job-flow error.
    tags : Dict[str, str], optional
        Key/Value collection to put on the Cluster.
        e.g. {"foo": "boo", "bar": "xoo"})
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    str
        Cluster ID.

    Examples
    --------
    Minimal Example

    >>> import awswrangler as wr
    >>> cluster_id = wr.emr.create_cluster("SUBNET_ID")

    Minimal Example With Custom Classification

    >>> import awswrangler as wr
    >>> cluster_id = wr.emr.create_cluster(
    >>> subnet_id="SUBNET_ID",
    >>> custom_classifications=[
    >>>         {
    >>>             "Classification": "livy-conf",
    >>>             "Properties": {
    >>>                 "livy.spark.master": "yarn",
    >>>                 "livy.spark.deploy-mode": "cluster",
    >>>                 "livy.server.session.timeout": "16h",
    >>>             },
    >>>         }
    >>>     ],
    >>> )

    Full Example

    >>> import awswrangler as wr
    >>> cluster_id = wr.emr.create_cluster(
    ...     cluster_name="wrangler_cluster",
    ...     logging_s3_path=f"s3://BUCKET_NAME/emr-logs/",
    ...     emr_release="emr-5.28.0",
    ...     subnet_id="SUBNET_ID",
    ...     emr_ec2_role="EMR_EC2_DefaultRole",
    ...     emr_role="EMR_DefaultRole",
    ...     instance_type_master="m5.xlarge",
    ...     instance_type_core="m5.xlarge",
    ...     instance_type_task="m5.xlarge",
    ...     instance_ebs_size_master=50,
    ...     instance_ebs_size_core=50,
    ...     instance_ebs_size_task=50,
    ...     instance_num_on_demand_master=1,
    ...     instance_num_on_demand_core=1,
    ...     instance_num_on_demand_task=1,
    ...     instance_num_spot_master=0,
    ...     instance_num_spot_core=1,
    ...     instance_num_spot_task=1,
    ...     spot_bid_percentage_of_on_demand_master=100,
    ...     spot_bid_percentage_of_on_demand_core=100,
    ...     spot_bid_percentage_of_on_demand_task=100,
    ...     spot_provisioning_timeout_master=5,
    ...     spot_provisioning_timeout_core=5,
    ...     spot_provisioning_timeout_task=5,
    ...     spot_timeout_to_on_demand_master=True,
    ...     spot_timeout_to_on_demand_core=True,
    ...     spot_timeout_to_on_demand_task=True,
    ...     python3=True,
    ...     spark_glue_catalog=True,
    ...     hive_glue_catalog=True,
    ...     presto_glue_catalog=True,
    ...     bootstraps_paths=None,
    ...     debugging=True,
    ...     applications=["Hadoop", "Spark", "Ganglia", "Hive"],
    ...     visible_to_all_users=True,
    ...     key_pair_name=None,
    ...     spark_jars_path=[f"s3://...jar"],
    ...     maximize_resource_allocation=True,
    ...     keep_cluster_alive_when_no_steps=True,
    ...     termination_protected=False,
    ...     spark_pyarrow=True,
    ...     tags={
    ...         "foo": "boo"
    ...     })

    """
    applications = ["Spark"] if applications is None else applications
    boto3_session = _utils.ensure_session(session=boto3_session)
    args: Dict[str, Any] = _build_cluster_args(**locals())
    client_emr: boto3.client = _utils.client(service_name="emr", session=boto3_session)
    response: Dict[str, Any] = client_emr.run_job_flow(**args)
    _logger.debug("response: \n%s", pprint.pformat(response))
    return cast(str, response["JobFlowId"])


def get_cluster_state(cluster_id: str, boto3_session: Optional[boto3.Session] = None) -> str:
    """Get the EMR cluster state.

    Possible states: 'STARTING', 'BOOTSTRAPPING', 'RUNNING',
    'WAITING', 'TERMINATING',
    'TERMINATED', 'TERMINATED_WITH_ERRORS'

    Parameters
    ----------
    cluster_id : str
        Cluster ID.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    str
        State.

    Examples
    --------
    >>> import awswrangler as wr
    >>> state = wr.emr.get_cluster_state("cluster-id")

    """
    client_emr: boto3.client = _utils.client(service_name="emr", session=boto3_session)
    response: Dict[str, Any] = client_emr.describe_cluster(ClusterId=cluster_id)
    _logger.debug("response: \n%s", pprint.pformat(response))
    return cast(str, response["Cluster"]["Status"]["State"])


def terminate_cluster(cluster_id: str, boto3_session: Optional[boto3.Session] = None) -> None:
    """Terminate EMR cluster.

    Parameters
    ----------
    cluster_id : str
        Cluster ID.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    None
        None.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.emr.terminate_cluster("cluster-id")

    """
    client_emr: boto3.client = _utils.client(service_name="emr", session=boto3_session)
    response: Dict[str, Any] = client_emr.terminate_job_flows(JobFlowIds=[cluster_id])
    _logger.debug("response: \n%s", pprint.pformat(response))


def submit_steps(
    cluster_id: str, steps: List[Dict[str, Any]], boto3_session: Optional[boto3.Session] = None
) -> List[str]:
    """Submit a list of steps.

    Parameters
    ----------
    cluster_id : str
        Cluster ID.
    steps: List[Dict[str, Any]]
        Steps definitions (Obs: Use EMR.build_step() to build it).
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    List[str]
        List of step IDs.

    Examples
    --------
    >>> import awswrangler as wr
    >>> for cmd in ['echo "Hello"', "ls -la"]:
    ...     steps.append(wr.emr.build_step(name=cmd, command=cmd))
    >>> wr.emr.submit_steps(cluster_id="cluster-id", steps=steps)

    """
    client_emr: boto3.client = _utils.client(service_name="emr", session=boto3_session)
    response: Dict[str, Any] = client_emr.add_job_flow_steps(JobFlowId=cluster_id, Steps=steps)
    _logger.debug("response: \n%s", pprint.pformat(response))
    return cast(List[str], response["StepIds"])


def submit_step(
    cluster_id: str,
    command: str,
    name: str = "my-step",
    action_on_failure: str = "CONTINUE",
    script: bool = False,
    boto3_session: Optional[boto3.Session] = None,
) -> str:
    """Submit new job in the EMR Cluster.

    Parameters
    ----------
    cluster_id : str
        Cluster ID.
    command : str
        e.g. 'echo "Hello!"'
        e.g. for script 's3://.../script.sh arg1 arg2'
    name : str, optional
        Step name.
    action_on_failure : str
        'TERMINATE_JOB_FLOW', 'TERMINATE_CLUSTER', 'CANCEL_AND_WAIT', 'CONTINUE'
    script : bool
        True for raw command or False for script runner.
        https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-commandrunner.html
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    str
        Step ID.

    Examples
    --------
    >>> import awswrangler as wr
    >>> step_id = wr.emr.submit_step(
    ...     cluster_id=cluster_id,
    ...     name="step_test",
    ...     command="s3://...script.sh arg1 arg2",
    ...     script=True)

    """
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    step: Dict[str, Any] = build_step(
        name=name, command=command, action_on_failure=action_on_failure, script=script, boto3_session=session
    )
    client_emr: boto3.client = _utils.client(service_name="emr", session=session)
    response: Dict[str, Any] = client_emr.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step])
    _logger.debug("response: \n%s", pprint.pformat(response))
    return cast(str, response["StepIds"][0])


def build_step(
    command: str,
    name: str = "my-step",
    action_on_failure: str = "CONTINUE",
    script: bool = False,
    region: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> Dict[str, Any]:
    """Build the Step structure (dictionary).

    Parameters
    ----------
    command : str
        e.g. 'echo "Hello!"'
        e.g. for script 's3://.../script.sh arg1 arg2'
    name : str, optional
        Step name.
    action_on_failure : str
        'TERMINATE_JOB_FLOW', 'TERMINATE_CLUSTER', 'CANCEL_AND_WAIT', 'CONTINUE'
    script : bool
        False for raw command or True for script runner.
        https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-commandrunner.html
    region: str, optional
        Region name to not get it from boto3.Session. (e.g. `us-east-1`)
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Dict[str, Any]
        Step structure.

    Examples
    --------
    >>> import awswrangler as wr
    >>> steps = []
    >>> for cmd in ['echo "Hello"', "ls -la"]:
    ...     steps.append(wr.emr.build_step(name=cmd, command=cmd))
    >>> wr.emr.submit_steps(cluster_id="cluster-id", steps=steps)

    """
    jar: str = "command-runner.jar"
    if script is True:
        if region is not None:
            _region: str = region
        else:
            _region = _utils.get_region_from_session(boto3_session=boto3_session, default_region="us-east-1")
        jar = f"s3://{_region}.elasticmapreduce/libs/script-runner/script-runner.jar"
    step: Dict[str, Any] = {
        "Name": name,
        "ActionOnFailure": action_on_failure,
        "HadoopJarStep": {"Jar": jar, "Args": command.split(" ")},
    }
    return step


def get_step_state(cluster_id: str, step_id: str, boto3_session: Optional[boto3.Session] = None) -> str:
    """Get EMR step state.

    Possible states: 'PENDING', 'CANCEL_PENDING', 'RUNNING',
    'COMPLETED', 'CANCELLED', 'FAILED', 'INTERRUPTED'

    Parameters
    ----------
    cluster_id : str
        Cluster ID.
    step_id : str
        Step ID.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    str
        State.

    Examples
    --------
    >>> import awswrangler as wr
    >>> state = wr.emr.get_step_state("cluster-id", "step-id")

    """
    client_emr: boto3.client = _utils.client(service_name="emr", session=boto3_session)
    response: Dict[str, Any] = client_emr.describe_step(ClusterId=cluster_id, StepId=step_id)
    _logger.debug("response: \n%s", pprint.pformat(response))
    return cast(str, response["Step"]["Status"]["State"])


def submit_ecr_credentials_refresh(
    cluster_id: str, path: str, action_on_failure: str = "CONTINUE", boto3_session: Optional[boto3.Session] = None
) -> str:
    """Update internal ECR credentials.

    Parameters
    ----------
    cluster_id : str
        Cluster ID.
    path : str
        Amazon S3 path where Wrangler will stage the script ecr_credentials_refresh.py (e.g. s3://bucket/emr/)
    action_on_failure : str
        'TERMINATE_JOB_FLOW', 'TERMINATE_CLUSTER', 'CANCEL_AND_WAIT', 'CONTINUE'
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    str
        Step ID.

    Examples
    --------
    >>> import awswrangler as wr
    >>> step_id = wr.emr.submit_ecr_credentials_refresh("cluster_id", "s3://bucket/emr/")

    """
    path = path[:-1] if path.endswith("/") else path
    path_script: str = f"{path}/ecr_credentials_refresh.py"
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    client_s3: boto3.client = _utils.client(service_name="s3", session=session)
    bucket, key = _utils.parse_path(path=path_script)
    region: str = _utils.get_region_from_session(boto3_session=boto3_session)
    client_s3.put_object(
        Body=_get_ecr_credentials_refresh_content(region=region).encode(encoding="utf-8"), Bucket=bucket, Key=key
    )
    command: str = f"spark-submit --deploy-mode cluster {path_script}"
    name: str = "ECR Credentials Refresh"
    step: Dict[str, Any] = build_step(
        name=name, command=command, action_on_failure=action_on_failure, script=False, boto3_session=session
    )
    client_emr: boto3.client = _utils.client(service_name="emr", session=session)
    response: Dict[str, Any] = client_emr.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step])
    _logger.debug("response: \n%s", pprint.pformat(response))
    return cast(str, response["StepIds"][0])


def build_spark_step(
    path: str,
    deploy_mode: str = "cluster",
    docker_image: Optional[str] = None,
    name: str = "my-step",
    action_on_failure: str = "CONTINUE",
    region: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> Dict[str, Any]:
    """Build the Step structure (dictionary).

    Parameters
    ----------
    path : str
        Script path. (e.g. s3://bucket/app.py)
    deploy_mode : str
        "cluster" | "client"
    docker_image : str, optional
        e.g. "{ACCOUNT_ID}.dkr.ecr.{REGION}.amazonaws.com/{IMAGE_NAME}:{TAG}"
    name : str, optional
        Step name.
    action_on_failure : str
        'TERMINATE_JOB_FLOW', 'TERMINATE_CLUSTER', 'CANCEL_AND_WAIT', 'CONTINUE'
    region: str, optional
        Region name to not get it from boto3.Session. (e.g. `us-east-1`)
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Dict[str, Any]
        Step structure.

    Examples
    --------
    >>> import awswrangler as wr
    >>> step_id = wr.emr.submit_steps(
    >>>     cluster_id="cluster-id",
    >>>     steps=[
    >>>         wr.emr.build_spark_step(path="s3://bucket/app.py")
    >>>     ]
    >>> )

    """
    if docker_image is None:
        cmd: str = f"spark-submit --deploy-mode {deploy_mode} {path}"
    else:
        config: str = "hdfs:///user/hadoop/config.json"
        cmd = (
            f"spark-submit --deploy-mode cluster "
            f"--conf spark.executorEnv.YARN_CONTAINER_RUNTIME_TYPE=docker "
            f"--conf spark.executorEnv.YARN_CONTAINER_RUNTIME_DOCKER_IMAGE={docker_image} "
            f"--conf spark.executorEnv.YARN_CONTAINER_RUNTIME_DOCKER_CLIENT_CONFIG={config} "
            f"--conf spark.executorEnv.YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=/etc/passwd:/etc/passwd:ro "
            f"--conf spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_TYPE=docker "
            f"--conf spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_DOCKER_IMAGE={docker_image} "
            f"--conf spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_DOCKER_CLIENT_CONFIG={config} "
            f"--conf spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=/etc/passwd:/etc/passwd:ro "
            f"{path}"
        )
    return build_step(
        command=cmd,
        name=name,
        action_on_failure=action_on_failure,
        script=False,
        region=region,
        boto3_session=boto3_session,
    )


def submit_spark_step(
    cluster_id: str,
    path: str,
    deploy_mode: str = "cluster",
    docker_image: Optional[str] = None,
    name: str = "my-step",
    action_on_failure: str = "CONTINUE",
    region: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> str:
    """Submit Spark Step.

    Parameters
    ----------
    cluster_id : str
        Cluster ID.
    path : str
        Script path. (e.g. s3://bucket/app.py)
    deploy_mode : str
        "cluster" | "client"
    docker_image : str, optional
        e.g. "{ACCOUNT_ID}.dkr.ecr.{REGION}.amazonaws.com/{IMAGE_NAME}:{TAG}"
    name : str, optional
        Step name.
    action_on_failure : str
        'TERMINATE_JOB_FLOW', 'TERMINATE_CLUSTER', 'CANCEL_AND_WAIT', 'CONTINUE'
    region: str, optional
        Region name to not get it from boto3.Session. (e.g. `us-east-1`)
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    str
        Step ID.

    Examples
    --------
    >>> import awswrangler as wr
    >>> step_id = wr.emr.submit_spark_step(
    >>>     cluster_id="cluster-id",
    >>>     path="s3://bucket/emr/app.py"
    >>> )

    """
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    step = build_spark_step(
        path=path,
        deploy_mode=deploy_mode,
        docker_image=docker_image,
        name=name,
        action_on_failure=action_on_failure,
        region=region,
        boto3_session=session,
    )
    return submit_steps(cluster_id=cluster_id, steps=[step], boto3_session=session)[0]
