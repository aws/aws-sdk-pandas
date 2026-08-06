---
id: 002-sessions
title: "Sessions"
sidebar_position: 2
sidebar_label: "2 - Sessions"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/002%20-%20Sessions.ipynb
---

# Sessions

> This page is generated from [`tutorials/002 - Sessions.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/002%20-%20Sessions.ipynb). Open it in Jupyter to run it yourself.
## How awswrangler handles Sessions and AWS credentials?

After version 1.0.0 awswrangler relies on [Boto3.Session()](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html) to manage AWS credentials and configurations.

awswrangler will not store any kind of state internally. Users are in charge of managing Sessions.

Most awswrangler functions receive the optional `boto3_session` argument. If None is received, the default boto3 Session will be used.

```python
import boto3

import awswrangler as wr
```

## Using the default Boto3 Session

```python
wr.s3.does_object_exist("s3://noaa-ghcn-pds/fake")
```

```text
False
```

## Customizing and using the default Boto3 Session

```python
boto3.setup_default_session(region_name="us-east-2")

wr.s3.does_object_exist("s3://noaa-ghcn-pds/fake")
```

```text
False
```

## Using a new custom Boto3 Session

```python
my_session = boto3.Session(region_name="us-east-2")

wr.s3.does_object_exist("s3://noaa-ghcn-pds/fake", boto3_session=my_session)
```

```text
False
```
