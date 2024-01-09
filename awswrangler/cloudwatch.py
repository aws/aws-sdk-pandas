"""CloudWatch Logs module."""

from __future__ import annotations

import datetime
import logging
import time
from typing import Any, Dict, List, cast

import boto3

import awswrangler.pandas as pd
from awswrangler import _utils, exceptions
from awswrangler._config import apply_configs

_logger: logging.Logger = logging.getLogger(__name__)

_QUERY_WAIT_POLLING_DELAY: float = 1.0  # SECONDS


def _validate_args(
    start_timestamp: int,
    end_timestamp: int,
) -> None:
    if start_timestamp < 0:
        raise exceptions.InvalidArgument("`start_time` cannot be a negative value.")
    if start_timestamp >= end_timestamp:
        raise exceptions.InvalidArgumentCombination("`start_time` must be inferior to `end_time`.")


def start_query(
    query: str,
    log_group_names: list[str],
    start_time: datetime.datetime | None = None,
    end_time: datetime.datetime | None = None,
    limit: int | None = None,
    boto3_session: boto3.Session | None = None,
) -> str:
    """Run a query against AWS CloudWatchLogs Insights.

    https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CWL_QuerySyntax.html

    Parameters
    ----------
    query: str
        The query string.
    log_group_names: List[str]
        The list of log group names or ARNs to be queried. You can include up to 50 log groups.
    start_time: datetime.datetime
        The beginning of the time range to query.
    end_time: datetime.datetime
        The end of the time range to query.
    limit: int, optional
        The maximum number of log events to return in the query.
    boto3_session: boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    str
        Query ID.

    Examples
    --------
    >>> import awswrangler as wr
    >>> query_id = wr.cloudwatch.start_query(
    ...     log_group_names=["loggroup"],
    ...     query="fields @timestamp, @message | sort @timestamp desc | limit 5",
    ... )

    """
    _logger.debug("log_group_names: %s", log_group_names)

    start_time = (
        start_time if start_time else datetime.datetime(year=1970, month=1, day=1, tzinfo=datetime.timezone.utc)
    )
    end_time = end_time if end_time else datetime.datetime.utcnow()

    start_timestamp: int = int(1000 * start_time.timestamp())
    end_timestamp: int = int(1000 * end_time.timestamp())
    _logger.debug("start_timestamp: %s", start_timestamp)
    _logger.debug("end_timestamp: %s", end_timestamp)

    _validate_args(
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )
    args: dict[str, Any] = {
        "logGroupIdentifiers": log_group_names,
        "startTime": start_timestamp,
        "endTime": end_timestamp,
        "queryString": query,
    }
    if limit is not None:
        args["limit"] = limit

    client_logs = _utils.client(service_name="logs", session=boto3_session)
    response = client_logs.start_query(**args)
    return response["queryId"]


@apply_configs
def wait_query(
    query_id: str,
    boto3_session: boto3.Session | None = None,
    cloudwatch_query_wait_polling_delay: float = _QUERY_WAIT_POLLING_DELAY,
) -> dict[str, Any]:
    """Wait query ends.

    https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CWL_QuerySyntax.html

    Parameters
    ----------
    query_id : str
        Query ID.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    cloudwatch_query_wait_polling_delay: float, default: 0.2 seconds
        Interval in seconds for how often the function will check if the CloudWatch query has completed.

    Returns
    -------
    Dict[str, Any]
        Query result payload.

    Examples
    --------
    >>> import awswrangler as wr
    >>> query_id = wr.cloudwatch.start_query(
    ...     log_group_names=["loggroup"],
    ...     query="fields @timestamp, @message | sort @timestamp desc | limit 5",
    ... )
    ... response = wr.cloudwatch.wait_query(query_id=query_id)

    """
    final_states: list[str] = ["Complete", "Failed", "Cancelled"]
    client_logs = _utils.client(service_name="logs", session=boto3_session)
    response = client_logs.get_query_results(queryId=query_id)
    status = response["status"]
    while status not in final_states:
        time.sleep(cloudwatch_query_wait_polling_delay)
        response = client_logs.get_query_results(queryId=query_id)
        status = response["status"]
    _logger.debug("status: %s", status)
    if status == "Failed":
        raise exceptions.QueryFailed(f"query ID: {query_id}")
    if status == "Cancelled":
        raise exceptions.QueryCancelled(f"query ID: {query_id}")
    return cast(Dict[str, Any], response)


def run_query(
    query: str,
    log_group_names: list[str],
    start_time: datetime.datetime | None = None,
    end_time: datetime.datetime | None = None,
    limit: int | None = None,
    boto3_session: boto3.Session | None = None,
) -> list[list[dict[str, str]]]:
    """Run a query against AWS CloudWatchLogs Insights and wait the results.

    https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CWL_QuerySyntax.html

    Parameters
    ----------
    query : str
        The query string.
    log_group_names: List[str]
        The list of log group names or ARNs to be queried. You can include up to 50 log groups.
    start_time : datetime.datetime
        The beginning of the time range to query.
    end_time : datetime.datetime
        The end of the time range to query.
    limit : int, optional
        The maximum number of log events to return in the query.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    List[List[Dict[str, str]]]
        Result.

    Examples
    --------
    >>> import awswrangler as wr
    >>> result = wr.cloudwatch.run_query(
    ...     log_group_names=["loggroup"],
    ...     query="fields @timestamp, @message | sort @timestamp desc | limit 5",
    ... )

    """
    query_id: str = start_query(
        query=query,
        log_group_names=log_group_names,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
        boto3_session=boto3_session,
    )
    response: dict[str, Any] = wait_query(query_id=query_id, boto3_session=boto3_session)
    return cast(List[List[Dict[str, str]]], response["results"])


def read_logs(
    query: str,
    log_group_names: list[str],
    start_time: datetime.datetime | None = None,
    end_time: datetime.datetime | None = None,
    limit: int | None = None,
    boto3_session: boto3.Session | None = None,
) -> pd.DataFrame:
    """Run a query against AWS CloudWatchLogs Insights and convert the results to Pandas DataFrame.

    https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CWL_QuerySyntax.html

    Parameters
    ----------
    query: str
        The query string.
    log_group_names: List[str]
        The list of log group names or ARNs to be queried. You can include up to 50 log groups.
    start_time: datetime.datetime
        The beginning of the time range to query.
    end_time: datetime.datetime
        The end of the time range to query.
    limit: int, optional
        The maximum number of log events to return in the query.
    boto3_session: boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    pandas.DataFrame
        Result as a Pandas DataFrame.

    Examples
    --------
    >>> import awswrangler as wr
    >>> df = wr.cloudwatch.read_logs(
    ...     log_group_names=["loggroup"],
    ...     query="fields @timestamp, @message | sort @timestamp desc | limit 5",
    ... )

    """
    results: list[list[dict[str, str]]] = run_query(
        query=query,
        log_group_names=log_group_names,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
        boto3_session=boto3_session,
    )
    pre_df: list[dict[str, str]] = []
    for row in results:
        new_row: dict[str, str] = {}
        for col in row:
            if col["field"].startswith("@"):
                col_name = col["field"].replace("@", "", 1)
            else:
                col_name = col["field"]
            new_row[col_name] = col["value"]
        pre_df.append(new_row)
    df: pd.DataFrame = pd.DataFrame(pre_df, dtype="string")
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


def describe_log_streams(
    log_group_name: str,
    log_stream_name_prefix: str | None = None,
    order_by: str | None = "LogStreamName",
    descending: bool | None = False,
    limit: int | None = 50,
    boto3_session: boto3.Session | None = None,
) -> pd.DataFrame:
    """List the log streams for the specified log group, return results as a Pandas DataFrame.

    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/logs.html#CloudWatchLogs.Client.describe_log_streams

    Parameters
    ----------
    log_group_name : str
        The name of the log group.
    log_stream_name_prefix : str
        The prefix to match log streams' name
    order_by : str
        If the value is LogStreamName , the results are ordered by log stream name.
        If the value is LastEventTime , the results are ordered by the event time.
        The default value is LogStreamName .
    descending : bool
        If the value is True, results are returned in descending order.
        If the value is to False, results are returned in ascending order.
        The default value is False.
    limit : int, optional
        The maximum number of items returned. The default is up to 50 items.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    pandas.DataFrame
        Result as a Pandas DataFrame.

    Examples
    --------
    >>> import awswrangler as wr
    >>> df = wr.cloudwatch.describe_log_streams(
    ...     log_group_name="aws_sdk_pandas_log_group",
    ...     log_stream_name_prefix="aws_sdk_pandas_log_stream",
    ... )

    """
    client_logs = _utils.client(service_name="logs", session=boto3_session)
    args: dict[str, Any] = {
        "logGroupName": log_group_name,
        "descending": descending,
        "orderBy": order_by,
        "limit": limit,
    }
    if log_stream_name_prefix and order_by == "LogStreamName":
        args["logStreamNamePrefix"] = log_stream_name_prefix
    elif log_stream_name_prefix and order_by == "LastEventTime":
        raise exceptions.InvalidArgumentCombination(
            "Cannot call describe_log_streams with both `log_stream_name_prefix` and order_by equal 'LastEventTime'"
        )
    log_streams: list[dict[str, Any]] = []
    response = client_logs.describe_log_streams(**args)

    log_streams += cast(List[Dict[str, Any]], response["logStreams"])
    while "nextToken" in response:
        response = client_logs.describe_log_streams(
            **args,
            nextToken=response["nextToken"],
        )
        log_streams += cast(List[Dict[str, Any]], response["logStreams"])
    if log_streams:
        df: pd.DataFrame = pd.DataFrame(log_streams)
        df["logGroupName"] = log_group_name
        return df
    return pd.DataFrame()


def _filter_log_events(
    log_group_name: str,
    log_stream_names: list[str],
    start_timestamp: int | None = None,
    end_timestamp: int | None = None,
    filter_pattern: str | None = None,
    limit: int | None = 10000,
    boto3_session: boto3.Session | None = None,
) -> list[dict[str, Any]]:
    client_logs = _utils.client(service_name="logs", session=boto3_session)
    events: list[dict[str, Any]] = []
    args: dict[str, Any] = {
        "logGroupName": log_group_name,
        "logStreamNames": log_stream_names,
        "limit": limit,
    }
    if start_timestamp:
        args["startTime"] = start_timestamp
    if end_timestamp:
        args["endTime"] = end_timestamp
    if filter_pattern:
        args["filterPattern"] = filter_pattern
    response = client_logs.filter_log_events(**args)
    events += cast(List[Dict[str, Any]], response["events"])
    while "nextToken" in response:
        response = client_logs.filter_log_events(
            **args,
            nextToken=response["nextToken"],
        )
        events += cast(List[Dict[str, Any]], response["events"])
    return events


def filter_log_events(
    log_group_name: str,
    log_stream_name_prefix: str | None = None,
    log_stream_names: list[str] | None = None,
    filter_pattern: str | None = None,
    start_time: datetime.datetime | None = None,
    end_time: datetime.datetime | None = None,
    boto3_session: boto3.Session | None = None,
) -> pd.DataFrame:
    """List log events from the specified log group. The results are returned as Pandas DataFrame.

    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/logs.html#CloudWatchLogs.Client.filter_log_events

    Note
    ----
    Cannot call ``filter_log_events`` with both ``log_stream_names`` and ``log_stream_name_prefix``.

    Parameters
    ----------
    log_group_name: str
        The name of the log group.
    log_stream_name_prefix: str, optional
        Filters the results to include only events from log streams that have names starting with this prefix.
    log_stream_names: List[str], optional
        Filters the results to only logs from the log streams in this list.
    filter_pattern : str
        The filter pattern to use. If not provided, all the events are matched.
    start_time : datetime.datetime
        Events with a timestamp before this time are not returned.
    end_time : datetime.datetime
        Events with a timestamp later than this time are not returned.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    pandas.DataFrame
        Result as a Pandas DataFrame.

    Examples
    --------
    Get all log events from log group 'aws_sdk_pandas_log_group' that have log stream prefix 'aws_sdk_pandas_log_stream'

    >>> import awswrangler as wr
    >>> df = wr.cloudwatch.filter_log_events(
    ...     log_group_name="aws_sdk_pandas_log_group",
    ...     log_stream_name_prefix="aws_sdk_pandas_log_stream",
    ... )

    Get all log events contains 'REPORT' from log stream
    'aws_sdk_pandas_log_stream_one' and 'aws_sdk_pandas_log_stream_two'
    from log group 'aws_sdk_pandas_log_group'

    >>> import awswrangler as wr
    >>> df = wr.cloudwatch.filter_log_events(
    ...     log_group_name="aws_sdk_pandas_log_group",
    ...     log_stream_names=["aws_sdk_pandas_log_stream_one","aws_sdk_pandas_log_stream_two"],
    ...     filter_pattern='REPORT',
    ... )

    """
    if log_stream_name_prefix and log_stream_names:
        raise exceptions.InvalidArgumentCombination(
            "Cannot call `filter_log_events` with both `log_stream_names` and `log_stream_name_prefix`"
        )
    _logger.debug("log_group_name: %s", log_group_name)

    events: list[dict[str, Any]] = []
    if not log_stream_names:
        describe_log_streams_args: dict[str, Any] = {
            "log_group_name": log_group_name,
        }
        if boto3_session:
            describe_log_streams_args["boto3_session"] = boto3_session
        if log_stream_name_prefix:
            describe_log_streams_args["log_stream_name_prefix"] = log_stream_name_prefix
        log_streams = describe_log_streams(**describe_log_streams_args)
        log_stream_names = log_streams["logStreamName"].tolist() if len(log_streams.index) else []

    args: dict[str, Any] = {
        "log_group_name": log_group_name,
    }
    if start_time:
        args["start_timestamp"] = int(1000 * start_time.timestamp())
    if end_time:
        args["end_timestamp"] = int(1000 * end_time.timestamp())
    if filter_pattern:
        args["filter_pattern"] = filter_pattern
    if boto3_session:
        args["boto3_session"] = boto3_session
    chunked_log_streams_size: int = 50

    for i in range(0, len(log_stream_names), chunked_log_streams_size):
        log_streams = log_stream_names[i : i + chunked_log_streams_size]
        events += _filter_log_events(**args, log_stream_names=log_streams)
    if events:
        df: pd.DataFrame = pd.DataFrame(events)
        df["logGroupName"] = log_group_name
        return df
    return pd.DataFrame()
