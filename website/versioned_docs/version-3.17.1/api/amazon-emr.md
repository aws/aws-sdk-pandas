---
id: amazon-emr
title: "Amazon EMR"
sidebar_position: 17
---

# Amazon EMR

Module: `wr.emr`

### build_spark_step

```python
wr.emr.build_spark_step(
    path: 'str',
    args: 'list[str] | None' = None,
    deploy_mode: "Literal['cluster', 'client']" = 'cluster',
    docker_image: 'str | None' = None,
    name: 'str' = 'my-step',
    action_on_failure: '_ActionOnFailureLiteral' = 'CONTINUE',
    region: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'dict[str, Any]'
```

Build the Step structure (dictionary).

**Parameters**

- **`path`** — Script path. (e.g. s3://bucket/app.py)
- **`args`** — CLI args to use with script
- **`deploy_mode`** — "cluster" | "client"
- **`docker_image`** — e.g. "{ACCOUNT_ID}.dkr.ecr.{REGION}.amazonaws.com/{IMAGE_NAME}:{TAG}"
- **`name`** — Step name.
- **`action_on_failure`** — 'TERMINATE_JOB_FLOW', 'TERMINATE_CLUSTER', 'CANCEL_AND_WAIT', 'CONTINUE'
- **`region`** — Region name to not get it from boto3.Session. (e.g. `us-east-1`)
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Step structure.

**Examples**

```python
>>> import awswrangler as wr
>>> step_id = wr.emr.submit_steps(
>>>     cluster_id="cluster-id",
>>>     steps=[
>>>         wr.emr.build_spark_step(path="s3://bucket/app.py")
>>>     ]
>>> )
```

---

### build_step

```python
wr.emr.build_step(
    command: 'str',
    name: 'str' = 'my-step',
    action_on_failure: '_ActionOnFailureLiteral' = 'CONTINUE',
    script: 'bool' = False,
    region: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'dict[str, Any]'
```

Build the Step structure (dictionary).

**Parameters**

- **`command`** — e.g. 'echo "Hello!"' e.g. for script 's3://.../script.sh arg1 arg2'
- **`name`** — Step name.
- **`action_on_failure`** — 'TERMINATE_JOB_FLOW', 'TERMINATE_CLUSTER', 'CANCEL_AND_WAIT', 'CONTINUE'
- **`script`** — False for raw command or True for script runner. https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-commandrunner.html
- **`region`** — Region name to not get it from boto3.Session. (e.g. `us-east-1`)
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Step structure.

**Examples**

```python
>>> import awswrangler as wr
>>> steps = []
>>> for cmd in ['echo "Hello"', "ls -la"]:
...     steps.append(wr.emr.build_step(name=cmd, command=cmd))
>>> wr.emr.submit_steps(cluster_id="cluster-id", steps=steps)
```

---

### create_cluster

```python
wr.emr.create_cluster(
    subnet_id: 'str',
    cluster_name: 'str' = 'my-emr-cluster',
    logging_s3_path: 'str | None' = None,
    emr_release: 'str' = 'emr-6.7.0',
    emr_ec2_role: 'str' = 'EMR_EC2_DefaultRole',
    emr_role: 'str' = 'EMR_DefaultRole',
    instance_type_master: 'str' = 'r5.xlarge',
    instance_type_core: 'str' = 'r5.xlarge',
    instance_type_task: 'str' = 'r5.xlarge',
    instance_ebs_size_master: 'int' = 64,
    instance_ebs_size_core: 'int' = 64,
    instance_ebs_size_task: 'int' = 64,
    instance_num_on_demand_master: 'int' = 1,
    instance_num_on_demand_core: 'int' = 0,
    instance_num_on_demand_task: 'int' = 0,
    instance_num_spot_master: 'int' = 0,
    instance_num_spot_core: 'int' = 0,
    instance_num_spot_task: 'int' = 0,
    spot_bid_percentage_of_on_demand_master: 'int' = 100,
    spot_bid_percentage_of_on_demand_core: 'int' = 100,
    spot_bid_percentage_of_on_demand_task: 'int' = 100,
    spot_provisioning_timeout_master: 'int' = 5,
    spot_provisioning_timeout_core: 'int' = 5,
    spot_provisioning_timeout_task: 'int' = 5,
    spot_timeout_to_on_demand_master: 'bool' = True,
    spot_timeout_to_on_demand_core: 'bool' = True,
    spot_timeout_to_on_demand_task: 'bool' = True,
    python3: 'bool' = True,
    spark_glue_catalog: 'bool' = True,
    hive_glue_catalog: 'bool' = True,
    presto_glue_catalog: 'bool' = True,
    consistent_view: 'bool' = False,
    consistent_view_retry_seconds: 'int' = 10,
    consistent_view_retry_count: 'int' = 5,
    consistent_view_table_name: 'str' = 'EmrFSMetadata',
    bootstraps_paths: 'list[str] | None' = None,
    debugging: 'bool' = True,
    applications: 'list[str] | None' = None,
    visible_to_all_users: 'bool' = True,
    key_pair_name: 'str | None' = None,
    security_group_master: 'str | None' = None,
    security_groups_master_additional: 'list[str] | None' = None,
    security_group_slave: 'str | None' = None,
    security_groups_slave_additional: 'list[str] | None' = None,
    security_group_service_access: 'str | None' = None,
    security_configuration: 'str | None' = None,
    docker: 'bool' = False,
    extra_public_registries: 'list[str] | None' = None,
    spark_log_level: 'str' = 'WARN',
    spark_jars_path: 'list[str] | None' = None,
    spark_defaults: 'dict[str, str] | None' = None,
    spark_pyarrow: 'bool' = False,
    custom_classifications: 'list[dict[str, Any]] | None' = None,
    maximize_resource_allocation: 'bool' = False,
    steps: 'list[dict[str, Any]] | None' = None,
    custom_ami_id: 'str | None' = None,
    step_concurrency_level: 'int' = 1,
    keep_cluster_alive_when_no_steps: 'bool' = True,
    termination_protected: 'bool' = False,
    auto_termination_policy: 'dict[str, int] | None' = None,
    tags: 'dict[str, str] | None' = None,
    boto3_session: 'boto3.Session | None' = None,
    configurations: 'list[dict[str, Any]] | None' = None
) -> 'str'
```

Create a EMR cluster with instance fleets configuration.

https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-instance-fleet.html

**Parameters**

- **`subnet_id`** — VPC subnet ID.
- **`cluster_name`** — Cluster name.
- **`logging_s3_path`** — Logging s3 path (e.g. s3://BUCKET_NAME/DIRECTORY_NAME/). If None, the default is `s3://aws-logs-{AccountId}-{RegionId}/elasticmapreduce/`
- **`emr_release`** — EMR release (e.g. emr-5.28.0).
- **`emr_ec2_role`** — IAM role name.
- **`emr_role`** — IAM role name.
- **`instance_type_master`** — EC2 instance type.
- **`instance_type_core`** — EC2 instance type.
- **`instance_type_task`** — EC2 instance type.
- **`instance_ebs_size_master`** — Size of EBS in GB.
- **`instance_ebs_size_core`** — Size of EBS in GB.
- **`instance_ebs_size_task`** — Size of EBS in GB.
- **`instance_num_on_demand_master`** — Number of on demand instances.
- **`instance_num_on_demand_core`** — Number of on demand instances.
- **`instance_num_on_demand_task`** — Number of on demand instances.
- **`instance_num_spot_master`** — Number of spot instances.
- **`instance_num_spot_core`** — Number of spot instances.
- **`instance_num_spot_task`** — Number of spot instances.
- **`spot_bid_percentage_of_on_demand_master`** — The bid price, as a percentage of On-Demand price.
- **`spot_bid_percentage_of_on_demand_core`** — The bid price, as a percentage of On-Demand price.
- **`spot_bid_percentage_of_on_demand_task`** — The bid price, as a percentage of On-Demand price.
- **`spot_provisioning_timeout_master`** — The spot provisioning timeout period in minutes. If Spot instances are not provisioned within this time period, the TimeOutAction is taken. Minimum value is 5 and maximum value is 1440. The timeout applies only during initial provisioning, when the cluster is first created.
- **`spot_provisioning_timeout_core`** — The spot provisioning timeout period in minutes. If Spot instances are not provisioned within this time period, the TimeOutAction is taken. Minimum value is 5 and maximum value is 1440. The timeout applies only during initial provisioning, when the cluster is first created.
- **`spot_provisioning_timeout_task`** — The spot provisioning timeout period in minutes. If Spot instances are not provisioned within this time period, the TimeOutAction is taken. Minimum value is 5 and maximum value is 1440. The timeout applies only during initial provisioning, when the cluster is first created.
- **`spot_timeout_to_on_demand_master`** — After a provisioning timeout should the cluster switch to on demand or shutdown?
- **`spot_timeout_to_on_demand_core`** — After a provisioning timeout should the cluster switch to on demand or shutdown?
- **`spot_timeout_to_on_demand_task`** — After a provisioning timeout should the cluster switch to on demand or shutdown?
- **`python3`** — Python 3 Enabled?
- **`spark_glue_catalog`** — Spark integration with Glue Catalog?
- **`hive_glue_catalog`** — Hive integration with Glue Catalog?
- **`presto_glue_catalog`** — Presto integration with Glue Catalog?
- **`consistent_view`** — Consistent view allows EMR clusters to check for list and read-after-write consistency for Amazon S3 objects written by or synced with EMRFS. https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-consistent-view.html
- **`consistent_view_retry_seconds`** — Delay between the tries (seconds).
- **`consistent_view_retry_count`** — Number of tries.
- **`consistent_view_table_name`** — Name of the DynamoDB table to store the consistent view data.
- **`bootstraps_paths`** — Bootstraps paths (e.g ["s3://BUCKET_NAME/script.sh"]).
- **`debugging`** — Debugging enabled?
- **`applications`** — List of applications (e.g ["Hadoop", "Spark", "Ganglia", "Hive"]). If None, ["Spark"] will be considered.
- **`visible_to_all_users`** — True or False.
- **`key_pair_name`** — Key pair name.
- **`security_group_master`** — The identifier of the Amazon EC2 security group for the master node.
- **`security_groups_master_additional`** — A list of additional Amazon EC2 security group IDs for the master node.
- **`security_group_slave`** — The identifier of the Amazon EC2 security group for the core and task nodes.
- **`security_groups_slave_additional`** — A list of additional Amazon EC2 security group IDs for the core and task nodes.
- **`security_group_service_access`** — The identifier of the Amazon EC2 security group for the Amazon EMR service to access clusters in VPC private subnets.
- **`security_configuration:str, optional`** — The name of a security configuration to apply to the cluster.
- **`docker`** — Enable Docker Hub and ECR registries access.
- **`extra_public_registries`** — Additional docker registries.
- **`spark_log_level`** — log4j.rootCategory log level (ALL, DEBUG, INFO, WARN, ERROR, FATAL, OFF, TRACE).
- **`spark_jars_path`** — spark.jars e.g. [s3://.../foo.jar, s3://.../boo.jar] https://spark.apache.org/docs/latest/configuration.html
- **`spark_defaults`** — https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-configure.html#spark-defaults
- **`spark_pyarrow`** — Enable PySpark to use PyArrow behind the scenes. P.S. You must install pyarrow by your self via bootstrap
- **`custom_classifications`** — Extra classifications.
- **`maximize_resource_allocation`** — Configure your executors to utilize the maximum resources possible https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-configure.html#emr-spark-maximizeresourceallocation
- **`custom_ami_id`** — The custom AMI ID to use for the provisioned instance group
- **`steps`** — Steps definitions (Obs : str Use EMR.build_step() to build it)
- **`keep_cluster_alive_when_no_steps`** — Specifies whether the cluster should remain available after completing all steps
- **`termination_protected`** — Specifies whether the Amazon EC2 instances in the cluster are protected from termination by API calls, user intervention, or in the event of a job-flow error.
- **`auto_termination_policy`** — Specifies the auto-termination policy that is attached to an Amazon EMR cluster eg. auto_termination_policy = {'IdleTimeout': 123} IdleTimeout specifies the amount of idle time in seconds after which the cluster automatically terminates. You can specify a minimum of 60 seconds and a maximum of 604800 seconds (seven days).
- **`tags`** — Key/Value collection to put on the Cluster. e.g. {"foo": "boo", "bar": "xoo"})
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.
- **`configurations`** — The list of configurations supplied for an EMR cluster instance group. By default, adds log4j config as follows: `{"Classification": "spark-log4j", "Properties": {"log4j.rootCategory": f"{pars['spark_log_level']}, console"}}`

**Returns**

- Cluster ID.

**Examples**

Minimal Example

```python
>>> import awswrangler as wr
>>> cluster_id = wr.emr.create_cluster("SUBNET_ID")
```

Minimal Example With Custom Classification

```python
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
```

Full Example

```python
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
```

---

### get_cluster_state

```python
wr.emr.get_cluster_state(
    cluster_id: 'str',
    boto3_session: 'boto3.Session | None' = None
) -> 'str'
```

Get the EMR cluster state.

Possible states: 'STARTING', 'BOOTSTRAPPING', 'RUNNING',
'WAITING', 'TERMINATING',
'TERMINATED', 'TERMINATED_WITH_ERRORS'

**Parameters**

- **`cluster_id`** — Cluster ID.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- State.

**Examples**

```python
>>> import awswrangler as wr
>>> state = wr.emr.get_cluster_state("cluster-id")
```

---

### get_step_state

```python
wr.emr.get_step_state(
    cluster_id: 'str',
    step_id: 'str',
    boto3_session: 'boto3.Session | None' = None
) -> 'str'
```

Get EMR step state.

Possible states: 'PENDING', 'CANCEL_PENDING', 'RUNNING',
'COMPLETED', 'CANCELLED', 'FAILED', 'INTERRUPTED'

**Parameters**

- **`cluster_id`** — Cluster ID.
- **`step_id`** — Step ID.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- State.

**Examples**

```python
>>> import awswrangler as wr
>>> state = wr.emr.get_step_state("cluster-id", "step-id")
```

---

### submit_ecr_credentials_refresh

```python
wr.emr.submit_ecr_credentials_refresh(
    cluster_id: 'str',
    path: 'str',
    action_on_failure: '_ActionOnFailureLiteral' = 'CONTINUE',
    boto3_session: 'boto3.Session | None' = None
) -> 'str'
```

Update internal ECR credentials.

**Parameters**

- **`cluster_id`** — Cluster ID.
- **`path`** — Amazon S3 path where awswrangler will stage the script ecr_credentials_refresh.py (e.g. s3://bucket/emr/)
- **`action_on_failure`** — 'TERMINATE_JOB_FLOW', 'TERMINATE_CLUSTER', 'CANCEL_AND_WAIT', 'CONTINUE'
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Step ID.

**Examples**

```python
>>> import awswrangler as wr
>>> step_id = wr.emr.submit_ecr_credentials_refresh("cluster_id", "s3://bucket/emr/")
```

---

### submit_spark_step

```python
wr.emr.submit_spark_step(
    cluster_id: 'str',
    path: 'str',
    args: 'list[str] | None' = None,
    deploy_mode: "Literal['cluster', 'client']" = 'cluster',
    docker_image: 'str | None' = None,
    name: 'str' = 'my-step',
    action_on_failure: '_ActionOnFailureLiteral' = 'CONTINUE',
    region: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'str'
```

Submit Spark Step.

**Parameters**

- **`cluster_id`** — Cluster ID.
- **`path`** — Script path. (e.g. s3://bucket/app.py)
- **`args`** — CLI args to use with script eg. args = ["--name", "hello-world"]
- **`deploy_mode`** — "cluster" | "client"
- **`docker_image`** — e.g. "{ACCOUNT_ID}.dkr.ecr.{REGION}.amazonaws.com/{IMAGE_NAME}:{TAG}"
- **`name`** — Step name.
- **`action_on_failure`** — 'TERMINATE_JOB_FLOW', 'TERMINATE_CLUSTER', 'CANCEL_AND_WAIT', 'CONTINUE'
- **`region`** — Region name to not get it from boto3.Session. (e.g. `us-east-1`)
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Step ID.

**Examples**

```python
>>> import awswrangler as wr
>>> step_id = wr.emr.submit_spark_step(
>>>     cluster_id="cluster-id",
>>>     path="s3://bucket/emr/app.py"
>>> )
```

---

### submit_step

```python
wr.emr.submit_step(
    cluster_id: 'str',
    command: 'str',
    name: 'str' = 'my-step',
    action_on_failure: '_ActionOnFailureLiteral' = 'CONTINUE',
    script: 'bool' = False,
    boto3_session: 'boto3.Session | None' = None
) -> 'str'
```

Submit new job in the EMR Cluster.

**Parameters**

- **`cluster_id`** — Cluster ID.
- **`command`** — e.g. 'echo "Hello!"' e.g. for script 's3://.../script.sh arg1 arg2'
- **`name`** — Step name.
- **`action_on_failure`** — 'TERMINATE_JOB_FLOW', 'TERMINATE_CLUSTER', 'CANCEL_AND_WAIT', 'CONTINUE'
- **`script`** — True for raw command or False for script runner. https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-commandrunner.html
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Step ID.

**Examples**

```python
>>> import awswrangler as wr
>>> step_id = wr.emr.submit_step(
...     cluster_id=cluster_id,
...     name="step_test",
...     command="s3://...script.sh arg1 arg2",
...     script=True,
... )
```

---

### submit_steps

```python
wr.emr.submit_steps(
    cluster_id: 'str',
    steps: 'list[dict[str, Any]]',
    boto3_session: 'boto3.Session | None' = None
) -> 'list[str]'
```

Submit a list of steps.

**Parameters**

- **`cluster_id`** — Cluster ID.
- **`steps`** — Steps definitions (Obs: Use EMR.build_step() to build it).
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- List of step IDs.

**Examples**

```python
>>> import awswrangler as wr
>>> for cmd in ['echo "Hello"', "ls -la"]:
...     steps.append(wr.emr.build_step(name=cmd, command=cmd))
>>> wr.emr.submit_steps(cluster_id="cluster-id", steps=steps)
```

---

### terminate_cluster

```python
wr.emr.terminate_cluster(
    cluster_id: 'str',
    boto3_session: 'boto3.Session | None' = None
) -> 'None'
```

Terminate EMR cluster.

**Parameters**

- **`cluster_id`** — Cluster ID.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Examples**

```python
>>> import awswrangler as wr
>>> wr.emr.terminate_cluster("cluster-id")
```

---
