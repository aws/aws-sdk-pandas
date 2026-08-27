---
id: 021-global-configurations
title: "Global Configurations"
sidebar_position: 21
sidebar_label: "21 - Global Configurations"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb
---

# Global Configurations

> This page is generated from [`tutorials/021 - Global Configurations.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb). Open it in Jupyter to run it yourself.
[awswrangler](https://github.com/aws/aws-sdk-pandas) has two ways to set global configurations that will override the regular default arguments configured in functions signatures.

- **Environment variables**
- **wr.config**

*P.S. Check the [function API doc](https://aws-sdk-pandas.readthedocs.io/en/3.17.1/api.html) to see if your function has some argument that can be configured through Global configurations.*

*P.P.S. One exception to the above mentioned rules is the `botocore_config` property. It cannot be set through environment variables
but only via `wr.config`. It will be used as the `botocore.config.Config` for all underlying `boto3` calls.
The default config is `botocore.config.Config(retries={"max_attempts": 5}, connect_timeout=10, max_pool_connections=10)`.
If you only want to change the retry behavior, you can use the environment variables `AWS_MAX_ATTEMPTS` and `AWS_RETRY_MODE`.
(see [Boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#using-environment-variables))*

## Environment Variables

```python
%env WR_DATABASE=default
%env WR_CTAS_APPROACH=False
%env WR_MAX_CACHE_SECONDS=900
%env WR_MAX_CACHE_QUERY_INSPECTIONS=500
%env WR_MAX_REMOTE_CACHE_ENTRIES=50
%env WR_MAX_LOCAL_CACHE_ENTRIES=100
```

```text
env: WR_DATABASE=default
env: WR_CTAS_APPROACH=False
env: WR_MAX_CACHE_SECONDS=900
env: WR_MAX_CACHE_QUERY_INSPECTIONS=500
env: WR_MAX_REMOTE_CACHE_ENTRIES=50
env: WR_MAX_LOCAL_CACHE_ENTRIES=100
```

```python
import botocore

import awswrangler as wr
```

```python
wr.athena.read_sql_query("SELECT 1 AS FOO")
```

```text
   foo
0    1
```

## Resetting

```python
# Specific
wr.config.reset("database")
# All
wr.config.reset()
```

## wr.config

```python
wr.config.database = "default"
wr.config.ctas_approach = False
wr.config.max_cache_seconds = 900
wr.config.max_cache_query_inspections = 500
wr.config.max_remote_cache_entries = 50
wr.config.max_local_cache_entries = 100
# Set botocore.config.Config that will be used for all boto3 calls
wr.config.botocore_config = botocore.config.Config(
    retries={"max_attempts": 10}, connect_timeout=20, max_pool_connections=20
)
```

```python
wr.athena.read_sql_query("SELECT 1 AS FOO")
```

```text
   foo
0    1
```

## Visualizing

```python
wr.config
```

```text
<awswrangler._config._Config at 0x1376ece80>
```
