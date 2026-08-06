---
id: 016-emr-and-docker
title: "EMR & Docker"
sidebar_position: 16
sidebar_label: "16 - EMR & Docker"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/016%20-%20EMR%20%26%20Docker.ipynb
---

# EMR & Docker

> This page is generated from [`tutorials/016 - EMR & Docker.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/016%20-%20EMR%20%26%20Docker.ipynb). Open it in Jupyter to run it yourself.
```python
import getpass

import boto3

import awswrangler as wr
```

## Enter your bucket name:

```python
bucket = getpass.getpass()
```

```text
 ··········································
```

## Enter your Subnet ID:

```python
subnet = getpass.getpass()
```

```text
 ························
```

## Build and Upload Docker Image to ECR repository

Replace the `{ACCOUNT_ID}` placeholder.

```python
%%writefile Dockerfile

FROM amazoncorretto:8

RUN yum -y update
RUN yum -y install yum-utils
RUN yum -y groupinstall development

RUN yum list python3*
RUN yum -y install python3 python3-dev python3-pip python3-virtualenv

RUN python -V
RUN python3 -V

ENV PYSPARK_DRIVER_PYTHON python3
ENV PYSPARK_PYTHON python3

RUN pip3 install --upgrade pip
RUN pip3 install awswrangler

RUN python3 -c "import awswrangler as wr"
```

```python
%%bash

docker build -t 'local/emr-wrangler' .
aws ecr create-repository --repository-name emr-wrangler
docker tag local/emr-wrangler {ACCOUNT_ID}.dkr.ecr.us-east-1.amazonaws.com/emr-wrangler:emr-wrangler
eval $(aws ecr get-login --region us-east-1 --no-include-email)
docker push {ACCOUNT_ID}.dkr.ecr.us-east-1.amazonaws.com/emr-wrangler:emr-wrangler
```

## Creating EMR Cluster

```python
cluster_id = wr.emr.create_cluster(subnet, docker=True)
```

## Refresh ECR credentials in the cluster (expiration time: 12h )

```python
wr.emr.submit_ecr_credentials_refresh(cluster_id, path=f"s3://{bucket}/")
```

```text
's-1B0O45RWJL8CL'
```

## Uploading application script to Amazon S3 (PySpark)

```python
script = """
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("docker-awswrangler").getOrCreate()
sc = spark.sparkContext

print("Spark Initialized")

import awswrangler as wr

print(f"awswrangler version: {wr.__version__}")
"""

boto3.client("s3").put_object(Body=script, Bucket=bucket, Key="test_docker.py")
```

## Submit PySpark step

```python
DOCKER_IMAGE = f"{wr.get_account_id()}.dkr.ecr.us-east-1.amazonaws.com/emr-wrangler:emr-wrangler"

step_id = wr.emr.submit_spark_step(cluster_id, f"s3://{bucket}/test_docker.py", docker_image=DOCKER_IMAGE)
```

## Wait Step

```python
while wr.emr.get_step_state(cluster_id, step_id) != "COMPLETED":
    pass
```

## Terminate Cluster

```python
wr.emr.terminate_cluster(cluster_id)
```

## Another example with custom configurations

```python
cluster_id = wr.emr.create_cluster(
    cluster_name="my-demo-cluster-v2",
    logging_s3_path=f"s3://{bucket}/emr-logs/",
    emr_release="emr-6.7.0",
    subnet_id=subnet,
    emr_ec2_role="EMR_EC2_DefaultRole",
    emr_role="EMR_DefaultRole",
    instance_type_master="m5.2xlarge",
    instance_type_core="m5.2xlarge",
    instance_ebs_size_master=50,
    instance_ebs_size_core=50,
    instance_num_on_demand_master=0,
    instance_num_on_demand_core=0,
    instance_num_spot_master=1,
    instance_num_spot_core=2,
    spot_bid_percentage_of_on_demand_master=100,
    spot_bid_percentage_of_on_demand_core=100,
    spot_provisioning_timeout_master=5,
    spot_provisioning_timeout_core=5,
    spot_timeout_to_on_demand_master=False,
    spot_timeout_to_on_demand_core=False,
    python3=True,
    docker=True,
    spark_glue_catalog=True,
    hive_glue_catalog=True,
    presto_glue_catalog=True,
    debugging=True,
    applications=["Hadoop", "Spark", "Hive", "Zeppelin", "Livy"],
    visible_to_all_users=True,
    maximize_resource_allocation=True,
    keep_cluster_alive_when_no_steps=True,
    termination_protected=False,
    spark_pyarrow=True,
)

wr.emr.submit_ecr_credentials_refresh(cluster_id, path=f"s3://{bucket}/emr/")

DOCKER_IMAGE = f"{wr.get_account_id()}.dkr.ecr.us-east-1.amazonaws.com/emr-wrangler:emr-wrangler"

step_id = wr.emr.submit_spark_step(cluster_id, f"s3://{bucket}/test_docker.py", docker_image=DOCKER_IMAGE)
```

```python
while wr.emr.get_step_state(cluster_id, step_id) != "COMPLETED":
    pass

wr.emr.terminate_cluster(cluster_id)
```
