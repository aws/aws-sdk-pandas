"""
Module to handle all utilities related to EMR (Elastic Map Reduce).

https://aws.amazon.com/emr/
"""

import json
from logging import Logger, getLogger
from typing import TYPE_CHECKING, Any, Collection, Dict, List, Optional, Union

from boto3 import client  # type: ignore

if TYPE_CHECKING:
    from awswrangler.session import Session

logger: Logger = getLogger(__name__)


class EMR:
    """Amazon EMR Class."""
    def __init__(self, session: "Session"):
        """
        Amazon EMR Class Constructor.

        Don't use it directly, call through a Session().
        e.g. wr.redshift.your_method()

        :param session: awswrangler.Session()
        """
        self._session: "Session" = session
        self._client_emr: client = session.boto3_session.client(service_name="emr", config=session.botocore_config)

    @staticmethod
    def _build_cluster_args(**pars):

        spark_env: Optional[Dict[str, str]] = None
        yarn_env: Optional[Dict[str, str]] = None
        livy_env: Optional[Dict[str, str]] = None

        if pars["spark_pyarrow"] is True:
            if pars["spark_defaults"] is None:
                pars["spark_defaults"]: Dict[str, str] = {"spark.sql.execution.arrow.enabled": "true"}
            else:
                pars["spark_defaults"]["spark.sql.execution.arrow.enabled"]: str = "true"
            spark_env = {"ARROW_PRE_0_15_IPC_FORMAT": "1"}
            yarn_env = {"ARROW_PRE_0_15_IPC_FORMAT": "1"}
            livy_env = {"ARROW_PRE_0_15_IPC_FORMAT": "1"}

        if pars["python3"] is True:
            if spark_env is None:
                spark_env: Dict[str, str] = {"PYSPARK_PYTHON": "/usr/bin/python3"}
            else:
                spark_env["PYSPARK_PYTHON"]: str = "/usr/bin/python3"

        if pars["spark_jars_path"] is not None:
            paths: str = ",".join(pars["spark_jars_path"])
            if pars["spark_defaults"] is None:
                pars["spark_defaults"]: Dict[str, str] = {"spark.jars": paths}
            else:
                pars["spark_defaults"]["spark.jars"]: str = paths

        args: Dict = {
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
                "InstanceFleets": []
            }
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
        args["Configurations"]: List[Dict[str, Any]] = [{
            "Classification": "spark-log4j",
            "Properties": {
                "log4j.rootCategory": f"{pars['spark_log_level']}, console"
            }
        }]
        if spark_env is not None:
            args["Configurations"].append({
                "Classification":
                "spark-env",
                "Properties": {},
                "Configurations": [{
                    "Classification": "export",
                    "Properties": spark_env,
                    "Configurations": []
                }]
            })
        if yarn_env is not None:
            args["Configurations"].append({
                "Classification":
                "yarn-env",
                "Properties": {},
                "Configurations": [{
                    "Classification": "export",
                    "Properties": yarn_env,
                    "Configurations": []
                }]
            })
        if livy_env is not None:
            args["Configurations"].append({
                "Classification":
                "livy-env",
                "Properties": {},
                "Configurations": [{
                    "Classification": "export",
                    "Properties": livy_env,
                    "Configurations": []
                }]
            })
        if pars["spark_glue_catalog"] is True:
            args["Configurations"].append({
                "Classification": "spark-hive-site",
                "Properties": {
                    "hive.metastore.client.factory.class":
                    "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
                },
                "Configurations": []
            })
        if pars["hive_glue_catalog"] is True:
            args["Configurations"].append({
                "Classification": "hive-site",
                "Properties": {
                    "hive.metastore.client.factory.class":
                    "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                },
                "Configurations": []
            })
        if pars["presto_glue_catalog"] is True:
            args["Configurations"].append({
                "Classification": "presto-connector-hive",
                "Properties": {
                    "hive.metastore.glue.datacatalog.enabled": "true"
                },
                "Configurations": []
            })
        if pars["consistent_view"] is True:
            args["Configurations"].append({
                "Classification": "emrfs-site",
                "Properties": {
                    "fs.s3.consistent.retryPeriodSeconds": str(pars.get("consistent_view_retry_seconds", "10")),
                    "fs.s3.consistent": "true",
                    "fs.s3.consistent.retryCount": str(pars.get("consistent_view_retry_count", "5")),
                    "fs.s3.consistent.metadata.tableName": pars.get("consistent_view_table_name", "EmrFSMetadata")
                }
            })
        if pars["maximize_resource_allocation"] is True:
            args["Configurations"].append({
                "Classification": "spark",
                "Properties": {
                    "maximizeResourceAllocation": "true"
                }
            })
        if pars["spark_defaults"] is not None:
            spark_defaults: Dict[str, Union[str, Dict[str, str]]] = {
                "Classification": "spark-defaults",
                "Properties": pars["spark_defaults"]
            }
            args["Configurations"].append(spark_defaults)

        # Applications
        if pars["applications"]:
            args["Applications"]: List[Dict[str, str]] = [{"Name": x} for x in pars["applications"]]

        # Bootstraps
        if pars["bootstraps_paths"]:
            args["BootstrapActions"]: List[Dict] = [{
                "Name": x,
                "ScriptBootstrapAction": {
                    "Path": x
                }
            } for x in pars["bootstraps_paths"]]

        # Debugging and Steps
        if (pars["debugging"] is True) or (pars["steps"] is not None):
            args["Steps"]: List[Dict[str, Collection[str]]] = []
            if pars["debugging"] is True:
                args["Steps"].append({
                    "Name": "Setup Hadoop Debugging",
                    "ActionOnFailure": "TERMINATE_CLUSTER",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": ["state-pusher-script"]
                    }
                })
            if pars["steps"] is not None:
                args["Steps"] += pars["steps"]

        # Master Instance Fleet
        timeout_action_master: str = "SWITCH_TO_ON_DEMAND" if pars[
            "spot_timeout_to_on_demand_master"] else "TERMINATE_CLUSTER"
        fleet_master: Dict = {
            "Name":
            "MASTER",
            "InstanceFleetType":
            "MASTER",
            "TargetOnDemandCapacity":
            pars["instance_num_on_demand_master"],
            "TargetSpotCapacity":
            pars["instance_num_spot_master"],
            "InstanceTypeConfigs": [
                {
                    "InstanceType": pars["instance_type_master"],
                    "WeightedCapacity": 1,
                    "BidPriceAsPercentageOfOnDemandPrice": pars["spot_bid_percentage_of_on_demand_master"],
                    "EbsConfiguration": {
                        "EbsBlockDeviceConfigs": [{
                            "VolumeSpecification": {
                                "SizeInGB": pars["instance_ebs_size_master"],
                                "VolumeType": "gp2"
                            },
                            "VolumesPerInstance": 1
                        }],
                        "EbsOptimized":
                        True
                    },
                },
            ],
        }
        if pars["instance_num_spot_master"] > 0:
            fleet_master["LaunchSpecifications"]: Dict = {
                "SpotSpecification": {
                    "TimeoutDurationMinutes": pars["spot_provisioning_timeout_master"],
                    "TimeoutAction": timeout_action_master,
                }
            }
        args["Instances"]["InstanceFleets"].append(fleet_master)

        # Core Instance Fleet
        if (pars["instance_num_spot_core"] > 0) or pars["instance_num_on_demand_core"] > 0:
            timeout_action_core = "SWITCH_TO_ON_DEMAND" if pars[
                "spot_timeout_to_on_demand_core"] else "TERMINATE_CLUSTER"
            fleet_core: Dict = {
                "Name":
                "CORE",
                "InstanceFleetType":
                "CORE",
                "TargetOnDemandCapacity":
                pars["instance_num_on_demand_core"],
                "TargetSpotCapacity":
                pars["instance_num_spot_core"],
                "InstanceTypeConfigs": [
                    {
                        "InstanceType": pars["instance_type_core"],
                        "WeightedCapacity": 1,
                        "BidPriceAsPercentageOfOnDemandPrice": pars["spot_bid_percentage_of_on_demand_core"],
                        "EbsConfiguration": {
                            "EbsBlockDeviceConfigs": [{
                                "VolumeSpecification": {
                                    "SizeInGB": pars["instance_ebs_size_core"],
                                    "VolumeType": "gp2"
                                },
                                "VolumesPerInstance": 1
                            }],
                            "EbsOptimized":
                            True
                        },
                    },
                ],
            }
            if pars["instance_num_spot_core"] > 0:
                fleet_core["LaunchSpecifications"]: Dict = {
                    "SpotSpecification": {
                        "TimeoutDurationMinutes": pars["spot_provisioning_timeout_core"],
                        "TimeoutAction": timeout_action_core,
                    }
                }
            args["Instances"]["InstanceFleets"].append(fleet_core)

        # Task Instance Fleet
        if (pars["instance_num_spot_task"] > 0) or pars["instance_num_on_demand_task"] > 0:
            timeout_action_task: str = "SWITCH_TO_ON_DEMAND" if pars[
                "spot_timeout_to_on_demand_task"] else "TERMINATE_CLUSTER"
            fleet_task: Dict = {
                "Name":
                "TASK",
                "InstanceFleetType":
                "TASK",
                "TargetOnDemandCapacity":
                pars["instance_num_on_demand_task"],
                "TargetSpotCapacity":
                pars["instance_num_spot_task"],
                "InstanceTypeConfigs": [
                    {
                        "InstanceType": pars["instance_type_task"],
                        "WeightedCapacity": 1,
                        "BidPriceAsPercentageOfOnDemandPrice": pars["spot_bid_percentage_of_on_demand_task"],
                        "EbsConfiguration": {
                            "EbsBlockDeviceConfigs": [{
                                "VolumeSpecification": {
                                    "SizeInGB": pars["instance_ebs_size_task"],
                                    "VolumeType": "gp2"
                                },
                                "VolumesPerInstance": 1
                            }],
                            "EbsOptimized":
                            True
                        },
                    },
                ],
            }
            if pars["instance_num_spot_task"] > 0:
                fleet_task["LaunchSpecifications"]: Dict = {
                    "SpotSpecification": {
                        "TimeoutDurationMinutes": pars["spot_provisioning_timeout_task"],
                        "TimeoutAction": timeout_action_task,
                    }
                }
            args["Instances"]["InstanceFleets"].append(fleet_task)

        # Tags
        if pars["tags"] is not None:
            args["Tags"] = [{"Key": k, "Value": v} for k, v in pars["tags"].items()]

        logger.info(f"args: \n{json.dumps(args, default=str, indent=4)}")
        return args

    def create_cluster(self,
                       cluster_name: str,
                       logging_s3_path: str,
                       emr_release: str,
                       subnet_id: str,
                       emr_ec2_role: str,
                       emr_role: str,
                       instance_type_master: str,
                       instance_type_core: str,
                       instance_type_task: str,
                       instance_ebs_size_master: int,
                       instance_ebs_size_core: int,
                       instance_ebs_size_task: int,
                       instance_num_on_demand_master: int,
                       instance_num_on_demand_core: int,
                       instance_num_on_demand_task: int,
                       instance_num_spot_master: int,
                       instance_num_spot_core: int,
                       instance_num_spot_task: int,
                       spot_bid_percentage_of_on_demand_master: int,
                       spot_bid_percentage_of_on_demand_core: int,
                       spot_bid_percentage_of_on_demand_task: int,
                       spot_provisioning_timeout_master: int,
                       spot_provisioning_timeout_core: int,
                       spot_provisioning_timeout_task: int,
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
                       spark_log_level: str = "WARN",
                       spark_jars_path: Optional[List[str]] = None,
                       spark_defaults: Optional[Dict[str, str]] = None,
                       spark_pyarrow: bool = False,
                       maximize_resource_allocation: bool = False,
                       steps: Optional[List[Dict[str, Collection[str]]]] = None,
                       keep_cluster_alive_when_no_steps: bool = True,
                       termination_protected: bool = False,
                       tags: Optional[Dict[str, str]] = None) -> str:
        """
        Create a EMR cluster with instance fleets configuration.

        https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-instance-fleet.html

        :param cluster_name: Cluster name
        :param logging_s3_path: Logging s3 path (e.g. s3://BUCKET_NAME/DIRECTORY_NAME/)
        :param emr_release: EMR release (e.g. emr-5.28.0)
        :param subnet_id: VPC subnet ID
        :param emr_ec2_role: IAM role name
        :param emr_role: IAM role name
        :param instance_type_master: EC2 instance type
        :param instance_type_core: EC2 instance type
        :param instance_type_task: EC2 instance type
        :param instance_ebs_size_master: Size of EBS in GB
        :param instance_ebs_size_core: Size of EBS in GB
        :param instance_ebs_size_task: Size of EBS in GB
        :param instance_num_on_demand_master: Number of on demand instances
        :param instance_num_on_demand_core: Number of on demand instances
        :param instance_num_on_demand_task: Number of on demand instances
        :param instance_num_spot_master: Number of spot instances
        :param instance_num_spot_core: Number of spot instances
        :param instance_num_spot_task: Number of spot instances
        :param spot_bid_percentage_of_on_demand_master: The bid price, as a percentage of On-Demand price
        :param spot_bid_percentage_of_on_demand_core: The bid price, as a percentage of On-Demand price
        :param spot_bid_percentage_of_on_demand_task: The bid price, as a percentage of On-Demand price
        :param spot_provisioning_timeout_master: The spot provisioning timeout period in minutes. If Spot instances are not provisioned within this time period, the TimeOutAction is taken. Minimum value is 5 and maximum value is 1440. The timeout applies only during initial provisioning, when the cluster is first created.
        :param spot_provisioning_timeout_core: The spot provisioning timeout period in minutes. If Spot instances are not provisioned within this time period, the TimeOutAction is taken. Minimum value is 5 and maximum value is 1440. The timeout applies only during initial provisioning, when the cluster is first created.
        :param spot_provisioning_timeout_task: The spot provisioning timeout period in minutes. If Spot instances are not provisioned within this time period, the TimeOutAction is taken. Minimum value is 5 and maximum value is 1440. The timeout applies only during initial provisioning, when the cluster is first created.
        :param spot_timeout_to_on_demand_master: After a provisioning timeout should the cluster switch to on demand or shutdown?
        :param spot_timeout_to_on_demand_core: After a provisioning timeout should the cluster switch to on demand or shutdown?
        :param spot_timeout_to_on_demand_task: After a provisioning timeout should the cluster switch to on demand or shutdown?
        :param python3: Python 3 Enabled?
        :param spark_glue_catalog: Spark integration with Glue Catalog?
        :param hive_glue_catalog: Hive integration with Glue Catalog?
        :param presto_glue_catalog: Presto integration with Glue Catalog?
        :param consistent_view: Consistent view allows EMR clusters to check for list and read-after-write consistency for Amazon S3 objects written by or synced with EMRFS. (https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-consistent-view.html)
        :param consistent_view_retry_seconds: Delay between the tries (seconds)
        :param consistent_view_retry_count: Number of tries
        :param consistent_view_table_name: Name of the DynamoDB table to store the consistent view data
        :param bootstraps_paths: Bootstraps paths (e.g ["s3://BUCKET_NAME/script.sh"])
        :param debugging: Debugging enabled?
        :param applications: List of applications (e.g ["Hadoop", "Spark", "Ganglia", "Hive"])
        :param visible_to_all_users: True or False
        :param key_pair_name: Key pair name (string)
        :param security_group_master: The identifier of the Amazon EC2 security group for the master node.
        :param security_groups_master_additional: A list of additional Amazon EC2 security group IDs for the master node.
        :param security_group_slave: The identifier of the Amazon EC2 security group for the core and task nodes.
        :param security_groups_slave_additional: A list of additional Amazon EC2 security group IDs for the core and task nodes.
        :param security_group_service_access: The identifier of the Amazon EC2 security group for the Amazon EMR service to access clusters in VPC private subnets.
        :param spark_log_level: log4j.rootCategory log level (ALL, DEBUG, INFO, WARN, ERROR, FATAL, OFF, TRACE)
        :param spark_jars_path: spark.jars (e.g. [s3://.../foo.jar, s3://.../boo.jar]) (https://spark.apache.org/docs/latest/configuration.html)
        :param spark_defaults: (https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-configure.html#spark-defaults)
        :param spark_pyarrow: Enable PySpark to use PyArrow behind the scenes. (P.S. You must install pyarrow by your self via bootstrap)
        :param maximize_resource_allocation: Configure your executors to utilize the maximum resources possible (https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-configure.html#emr-spark-maximizeresourceallocation)
        :param steps: Steps definitions (Obs: Use EMR.build_step() to build that)
        :param keep_cluster_alive_when_no_steps: Specifies whether the cluster should remain available after completing all steps
        :param termination_protected: Specifies whether the Amazon EC2 instances in the cluster are protected from termination by API calls, user intervention, or in the event of a job-flow error.
        :param tags: Key/Value collection to put on the Cluster. (e.g. {"foo": "boo", "bar": "xoo"})
        :return: Cluster ID (string)
        """
        args = EMR._build_cluster_args(**locals())
        response = self._client_emr.run_job_flow(**args)
        logger.info(f"response: \n{json.dumps(response, default=str, indent=4)}")
        return response["JobFlowId"]

    def get_cluster_state(self, cluster_id: str) -> str:
        """
        Get the EMR cluster state.

        Possible states: 'STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING', 'TERMINATING', 'TERMINATED', 'TERMINATED_WITH_ERRORS'

        :param cluster_id: EMR Cluster ID
        :return: State (string)
        """
        response: Dict = self._client_emr.describe_cluster(ClusterId=cluster_id)
        logger.info(f"response: \n{json.dumps(response, default=str, indent=4)}")
        return response["Cluster"]["Status"]["State"]

    def terminate_cluster(self, cluster_id: str) -> None:
        """
        Terminate the cluster.

        :param cluster_id: EMR Cluster ID
        :return: None
        """
        response: Dict = self._client_emr.terminate_job_flows(JobFlowIds=[
            cluster_id,
        ])
        logger.info(f"response: \n{json.dumps(response, default=str, indent=4)}")

    def submit_steps(self, cluster_id: str, steps: List[Dict[str, Collection[str]]]) -> List[str]:
        """
        Submit a list of steps.

        :param cluster_id: EMR Cluster ID
        :param steps: Steps definitions (Obs: Use EMR.build_step() to build that)
        :return: List of step IDs
        """
        response: Dict = self._client_emr.add_job_flow_steps(JobFlowId=cluster_id, Steps=steps)
        logger.info(f"response: \n{json.dumps(response, default=str, indent=4)}")
        return response["StepIds"]

    def submit_step(self,
                    cluster_id: str,
                    name: str,
                    command: str,
                    action_on_failure: str = "CONTINUE",
                    script: bool = False) -> str:
        """
        Submit new job in the EMR Cluster.

        :param cluster_id: EMR Cluster ID
        :param name: Step name
        :param command: e.g. 'echo "Hello!"' | e.g. for script 's3://.../script.sh arg1 arg2'
        :param action_on_failure: 'TERMINATE_JOB_FLOW', 'TERMINATE_CLUSTER', 'CANCEL_AND_WAIT', 'CONTINUE'
        :param script: True for raw command or False for script runner (https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-commandrunner.html)
        :return: Step ID
        """
        step = EMR.build_step(self, name=name, command=command, action_on_failure=action_on_failure, script=script)
        response: Dict = self._client_emr.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step])
        logger.info(f"response: \n{json.dumps(response, default=str, indent=4)}")
        return response["StepIds"][0]

    def build_step(self,
                   name: str,
                   command: str,
                   action_on_failure: str = "CONTINUE",
                   script: bool = False) -> Dict[str, Collection[str]]:
        """
        Build the Step dictionary.

        :param name: Step name
        :param command: e.g. 'echo "Hello!"' | e.g. for script 's3://.../script.sh arg1 arg2'
        :param action_on_failure: 'TERMINATE_JOB_FLOW', 'TERMINATE_CLUSTER', 'CANCEL_AND_WAIT', 'CONTINUE'
        :param script: True for raw command or False for script runner (https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-commandrunner.html)
        :return: Step Dict
        """
        jar: str = "command-runner.jar"
        if script is True:
            if self._session.region_name:
                region: str = self._session.region_name
            else:
                region = "us-east-1"
            jar = f"s3://{region}.elasticmapreduce/libs/script-runner/script-runner.jar"
        step = {
            "Name": name,
            "ActionOnFailure": action_on_failure,
            "HadoopJarStep": {
                "Jar": jar,
                "Args": command.split(" ")
            }
        }
        return step

    def get_step_state(self, cluster_id: str, step_id: str) -> str:
        """
        Get the EMR step state.

        Possible states: 'PENDING', 'CANCEL_PENDING', 'RUNNING', 'COMPLETED', 'CANCELLED', 'FAILED', 'INTERRUPTED',
        :param cluster_id: EMR Cluster ID
        :param step_id: EMR Step ID
        :return: State (string)
        """
        response: Dict = self._client_emr.describe_step(ClusterId=cluster_id, StepId=step_id)
        logger.info(f"response: \n{json.dumps(response, default=str, indent=4)}")
        return response["Step"]["Status"]["State"]
