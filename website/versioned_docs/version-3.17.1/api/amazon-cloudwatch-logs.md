---
id: amazon-cloudwatch-logs
title: "Amazon CloudWatch Logs"
sidebar_position: 19
---

# Amazon CloudWatch Logs

Module: `wr.cloudwatch`

### read_logs

```python
wr.cloudwatch.read_logs(
    query: 'str',
    log_group_names: 'list[str]',
    start_time: 'datetime.datetime | None' = None,
    end_time: 'datetime.datetime | None' = None,
    limit: 'int | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'pd.DataFrame'
```

Run a query against AWS CloudWatchLogs Insights and convert the results to Pandas DataFrame.

https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CWL_QuerySyntax.html

**Parameters**

- **`query:`** — The query string.
- **`log_group_names`** — The list of log group names or ARNs to be queried. You can include up to 50 log groups.
- **`start_time`** — The beginning of the time range to query.
- **`end_time`** — The end of the time range to query.
- **`limit`** — The maximum number of log events to return in the query.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Result as a Pandas DataFrame.

**Examples**

```python
>>> import awswrangler as wr
>>> df = wr.cloudwatch.read_logs(
...     log_group_names=["loggroup"],
...     query="fields @timestamp, @message | sort @timestamp desc | limit 5",
... )
```

---

### run_query

```python
wr.cloudwatch.run_query(
    query: 'str',
    log_group_names: 'list[str]',
    start_time: 'datetime.datetime | None' = None,
    end_time: 'datetime.datetime | None' = None,
    limit: 'int | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'list[list[dict[str, str]]]'
```

Run a query against AWS CloudWatchLogs Insights and wait the results.

https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CWL_QuerySyntax.html

**Parameters**

- **`query`** — The query string.
- **`log_group_names`** — The list of log group names or ARNs to be queried. You can include up to 50 log groups.
- **`start_time`** — The beginning of the time range to query.
- **`end_time`** — The end of the time range to query.
- **`limit`** — The maximum number of log events to return in the query.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Result.

**Examples**

```python
>>> import awswrangler as wr
>>> result = wr.cloudwatch.run_query(
...     log_group_names=["loggroup"],
...     query="fields @timestamp, @message | sort @timestamp desc | limit 5",
... )
```

---

### start_query

```python
wr.cloudwatch.start_query(
    query: 'str',
    log_group_names: 'list[str]',
    start_time: 'datetime.datetime | None' = None,
    end_time: 'datetime.datetime | None' = None,
    limit: 'int | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'str'
```

Run a query against AWS CloudWatchLogs Insights.

https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CWL_QuerySyntax.html

**Parameters**

- **`query`** — The query string.
- **`log_group_names`** — The list of log group names or ARNs to be queried. You can include up to 50 log groups.
- **`start_time`** — The beginning of the time range to query.
- **`end_time`** — The end of the time range to query.
- **`limit`** — The maximum number of log events to return in the query.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Query ID.

**Examples**

```python
>>> import awswrangler as wr
>>> query_id = wr.cloudwatch.start_query(
...     log_group_names=["loggroup"],
...     query="fields @timestamp, @message | sort @timestamp desc | limit 5",
... )
```

---

### wait_query

```python
wr.cloudwatch.wait_query(
    query_id: 'str',
    boto3_session: 'boto3.Session | None' = None,
    cloudwatch_query_wait_polling_delay: 'float' = 1.0
) -> 'dict[str, Any]'
```

Wait query ends.

https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CWL_QuerySyntax.html


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- cloudwatch_query_wait_polling_delay

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`query_id`** — Query ID.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.
- **`cloudwatch_query_wait_polling_delay`** — Interval in seconds for how often the function will check if the CloudWatch query has completed.

**Returns**

- Query result payload.

**Examples**

```python
>>> import awswrangler as wr
>>> query_id = wr.cloudwatch.start_query(
...     log_group_names=["loggroup"],
...     query="fields @timestamp, @message | sort @timestamp desc | limit 5",
... )
... response = wr.cloudwatch.wait_query(query_id=query_id)
```

---

### describe_log_streams

```python
wr.cloudwatch.describe_log_streams(
    log_group_name: 'str',
    log_stream_name_prefix: 'str | None' = None,
    order_by: 'str | None' = 'LogStreamName',
    descending: 'bool | None' = False,
    limit: 'int | None' = 50,
    boto3_session: 'boto3.Session | None' = None
) -> 'pd.DataFrame'
```

List the log streams for the specified log group, return results as a Pandas DataFrame.

https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/logs.html#CloudWatchLogs.Client.describe_log_streams

**Parameters**

- **`log_group_name`** — The name of the log group.
- **`log_stream_name_prefix`** — The prefix to match log streams' name
- **`order_by`** — If the value is LogStreamName , the results are ordered by log stream name. If the value is LastEventTime , the results are ordered by the event time. The default value is LogStreamName .
- **`descending`** — If the value is True, results are returned in descending order. If the value is to False, results are returned in ascending order. The default value is False.
- **`limit`** — The maximum number of items returned. The default is up to 50 items.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Result as a Pandas DataFrame.

**Examples**

```python
>>> import awswrangler as wr
>>> df = wr.cloudwatch.describe_log_streams(
...     log_group_name="aws_sdk_pandas_log_group",
...     log_stream_name_prefix="aws_sdk_pandas_log_stream",
... )
```

---

### filter_log_events

```python
wr.cloudwatch.filter_log_events(
    log_group_name: 'str',
    log_stream_name_prefix: 'str | None' = None,
    log_stream_names: 'list[str] | None' = None,
    filter_pattern: 'str | None' = None,
    start_time: 'datetime.datetime | None' = None,
    end_time: 'datetime.datetime | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'pd.DataFrame'
```

List log events from the specified log group. The results are returned as Pandas DataFrame.

https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/logs.html#CloudWatchLogs.Client.filter_log_events

:::note
Cannot call `filter_log_events` with both `log_stream_names` and `log_stream_name_prefix`.
:::

**Parameters**

- **`log_group_name`** — The name of the log group.
- **`log_stream_name_prefix`** — Filters the results to include only events from log streams that have names starting with this prefix.
- **`log_stream_names`** — Filters the results to only logs from the log streams in this list.
- **`filter_pattern`** — The filter pattern to use. If not provided, all the events are matched.
- **`start_time`** — Events with a timestamp before this time are not returned.
- **`end_time`** — Events with a timestamp later than this time are not returned.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Result as a Pandas DataFrame.

**Examples**

Get all log events from log group 'aws_sdk_pandas_log_group' that have log stream prefix 'aws_sdk_pandas_log_stream'

```python
>>> import awswrangler as wr
>>> df = wr.cloudwatch.filter_log_events(
...     log_group_name="aws_sdk_pandas_log_group",
...     log_stream_name_prefix="aws_sdk_pandas_log_stream",
... )
```

Get all log events contains 'REPORT' from log stream
'aws_sdk_pandas_log_stream_one' and 'aws_sdk_pandas_log_stream_two'
from log group 'aws_sdk_pandas_log_group'

```python
>>> import awswrangler as wr
>>> df = wr.cloudwatch.filter_log_events(
...     log_group_name="aws_sdk_pandas_log_group",
...     log_stream_names=["aws_sdk_pandas_log_stream_one","aws_sdk_pandas_log_stream_two"],
...     filter_pattern="REPORT",
... )
```

---
