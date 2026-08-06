---
id: 035-distributing-calls-on-ray-remote-cluster
title: "Distributing Calls on Ray Remote Cluster"
sidebar_position: 35
sidebar_label: "35 - Distributing Calls on Ray Remote Cluster"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/035%20-%20Distributing%20Calls%20on%20Ray%20Remote%20Cluster.ipynb
---

# Distributing Calls on Ray Remote Cluster

> This page is generated from [`tutorials/035 - Distributing Calls on Ray Remote Cluster.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/035%20-%20Distributing%20Calls%20on%20Ray%20Remote%20Cluster.ipynb). Open it in Jupyter to run it yourself.
AWS SDK for pandas supports distribution of specific calls on a cluster of EC2s using [ray](https://docs.ray.io/).

<div class="alert alert-block alert-warning">
Note that this tutorial creates a cluster of EC2 nodes which will incur a charge in your account. Please make sure to delete the cluster at the end.</div>

#### Install the library

```python
!pip install "awswrangler[modin,ray]"
```

## Configure and Build Ray Cluster on AWS

#### Build Prerequisite Infrastructure

Click on the link below to provision an AWS CloudFormation stack. It builds a security group and IAM instance profile for the Ray Cluster to use. A valid CIDR range (encompassing your local machine IP) and a VPC ID are required.

[<img src="https://s3.amazonaws.com/cloudformation-examples/cloudformation-launch-stack.png">](https://console.aws.amazon.com/cloudformation/home#/stacks/new?stackName=RayPrerequisiteInfra&templateURL=https://aws-data-wrangler-public-artifacts.s3.amazonaws.com/cloudformation/ray-prerequisite-infra.json)

#### Configure Ray Cluster Configuration
Start with a cluster configuration file (YAML).

```python
!touch config.yml
```

Replace all values to match your desired region, account number and name of resources deployed by the above CloudFormation Stack.

A limited set of AWS regions is currently supported (Python 3.8 and above). The example configuration below uses the AMI for `us-east-1`.

Then edit `config.yml` file with your custom configuration.

```yaml
cluster_name: pandas-sdk-cluster

min_workers: 2
max_workers: 2

provider:
    type: aws
    region: us-east-1 # Change AWS region as necessary
    availability_zone: us-east-1a,us-east-1b,us-east-1c # Change as necessary
    security_group:
        GroupName: ray-cluster
    cache_stopped_nodes: False

available_node_types:
  ray.head.default:
    node_config:
      InstanceType: m4.xlarge
      IamInstanceProfile:
        # Replace with your account id and profile name if you did not use the default value
        Arn: arn:aws:iam::{ACCOUNT ID}:instance-profile/ray-cluster
      # Replace ImageId if using a different region / python version
      ImageId: ami-02ea7c238b7ba36af
      TagSpecifications:  # Optional tags
        - ResourceType: "instance"
          Tags:
              - Key: Platform
                Value: "ray"

  ray.worker.default:
      min_workers: 2
      max_workers: 2
      node_config:
        InstanceType: m4.xlarge
        IamInstanceProfile:
          # Replace with your account id and profile name if you did not use the default value
          Arn: arn:aws:iam::{ACCOUNT ID}:instance-profile/ray-cluster
        # Replace ImageId if using a different region / python version
        ImageId: ami-02ea7c238b7ba36af
        TagSpecifications:  # Optional tags
          - ResourceType: "instance"
            Tags:
                - Key: Platform
                  Value: "ray"


setup_commands:
- pip install "awswrangler[modin,ray]"
```

#### Provision Ray Cluster

The command below creates a Ray cluster in your account based on the aforementioned config file. It consists of one head node and 2 workers (m4xlarge EC2s). The command takes a few minutes to complete.

```python
!ray up -y config.yml
```

Once the cluster is up and running, we set the `RAY_ADDRESS` environment variable to the head node Ray Cluster Address

```python
import os
import subprocess

head_node_ip = subprocess.check_output(["ray", "get-head-ip", "config.yml"]).decode("utf-8").split("\n")[-2]
os.environ["RAY_ADDRESS"] = f"ray://{head_node_ip}:10001"
```

As a result, `awswrangler` API calls now run on the cluster, not on your local machine. The SDK detects the required dependencies for its distributed mode and parallelizes supported methods on the cluster.

```python
import modin.pandas as pd

import awswrangler as wr

print(f"Execution engine: {wr.engine.get()}")
print(f"Memory format: {wr.memory_format.get()}")
```

Enter bucket Name

```python
bucket = "BUCKET_NAME"
```

#### Read & write some data at scale on the cluster

```python
# Read last 3 months of Taxi parquet compressed data (400 Mb)
df = wr.s3.read_parquet(path="s3://ursa-labs-taxi-data/2018/1*.parquet")
df["month"] = df["pickup_at"].dt.month

# Write it back to S3 partitioned by month
path = f"s3://{bucket}/taxi-data/"
database = "ray_test"
wr.catalog.create_database(name=database, exist_ok=True)
table = "nyc_taxi"

wr.s3.to_parquet(
    df=df,
    path=path,
    dataset=True,
    database=database,
    table=table,
    partition_cols=["month"],
)
```

#### Read it back via Athena UNLOAD

The [UNLOAD](https://docs.aws.amazon.com/athena/latest/ug/unload.html) command distributes query processing in Athena to dump results in S3 which are then read in parallel into a dataframe

```python
unload_path = f"s3://{bucket}/unload/nyc_taxi/"

# Athena UNLOAD requires that the S3 path is empty
# Note that s3.delete_objects is also a distributed call
wr.s3.delete_objects(unload_path)

wr.athena.read_sql_query(
    f"SELECT * FROM {table}",
    database=database,
    ctas_approach=False,
    unload_approach=True,
    s3_output=unload_path,
)
```

<div class="alert alert-block alert-warning">
The EC2 cluster must be terminated or it will incur a charge.
</div>

```python
!ray down -y ./config.yml
```

[More Info on Ray Clusters on AWS](https://docs.ray.io/en/latest/cluster/vms/getting-started.html#launch-a-cluster-on-a-cloud-provider)
