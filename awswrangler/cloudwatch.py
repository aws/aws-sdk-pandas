"""CloudWatch Logs module."""

import datetime
import logging
import time
from typing import Any, Dict, List, Optional, cast

import boto3
import pandas as pd

from awswrangler import _utils, exceptions

_logger: logging.Logger = logging.getLogger(__name__)

_QUERY_WAIT_POLLING_DELAY: float = 0.2  # SECONDS


def start_query(
    query: str,
    log_group_names: List[str],
    start_time: datetime.datetime = datetime.datetime(year=1970, month=1, day=1),
    end_time: datetime.datetime = datetime.datetime.now(),
    limit: Optional[int] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> str:
    """Run a query against AWS CloudWatchLogs Insights.

    https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CWL_QuerySyntax.html

    Parameters
    ----------
    query : str
        The query string.
    log_group_names : str
        The list of log groups to be queried. You can include up to 20 log groups.
    start_time : datetime.datetime
        The beginning of the time range to query.
    end_time : datetime.datetime
        The end of the time range to query.
    limit : Optional[int]
        The maximum number of log events to return in the query.
    boto3_session : boto3.Session(), optional
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
    start_timestamp: int = int(1000 * start_time.timestamp())
    end_timestamp: int = int(1000 * end_time.timestamp())
    _logger.debug("start_timestamp: %s", start_timestamp)
    _logger.debug("end_timestamp: %s", end_timestamp)
    args: Dict[str, Any] = {
        "logGroupNames": log_group_names,
        "startTime": start_timestamp,
        "endTime": end_timestamp,
        "queryString": query,
    }
    if limit is not None:
        args["limit"] = limit
    client_logs: boto3.client = _utils.client(service_name="logs", session=boto3_session)
    response: Dict[str, Any] = client_logs.start_query(**args)
    return cast(str, response["queryId"])


def wait_query(query_id: str, boto3_session: Optional[boto3.Session] = None) -> Dict[str, Any]:
    """Wait query ends.

    https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CWL_QuerySyntax.html

    Parameters
    ----------
    query_id : str
        Query ID.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

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
    final_states: List[str] = ["Complete", "Failed", "Cancelled"]
    client_logs: boto3.client = _utils.client(service_name="logs", session=boto3_session)
    response: Dict[str, Any] = client_logs.get_query_results(queryId=query_id)
    status: str = response["status"]
    while status not in final_states:
        time.sleep(_QUERY_WAIT_POLLING_DELAY)
        response = client_logs.get_query_results(queryId=query_id)
        status = response["status"]
    _logger.debug("status: %s", status)
    if status == "Failed":
        raise exceptions.QueryFailed(f"query ID: {query_id}")
    if status == "Cancelled":
        raise exceptions.QueryCancelled(f"query ID: {query_id}")
    return response


def run_query(
    query: str,
    log_group_names: List[str],
    start_time: datetime.datetime = datetime.datetime(year=1970, month=1, day=1),
    end_time: datetime.datetime = datetime.datetime.now(),
    limit: Optional[int] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> List[List[Dict[str, str]]]:
    """Run a query against AWS CloudWatchLogs Insights and wait the results.

    https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CWL_QuerySyntax.html

    Parameters
    ----------
    query : str
        The query string.
    log_group_names : str
        The list of log groups to be queried. You can include up to 20 log groups.
    start_time : datetime.datetime
        The beginning of the time range to query.
    end_time : datetime.datetime
        The end of the time range to query.
    limit : Optional[int]
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
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    query_id: str = start_query(
        query=query,
        log_group_names=log_group_names,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
        boto3_session=session,
    )
    response: Dict[str, Any] = wait_query(query_id=query_id, boto3_session=session)
    return cast(List[List[Dict[str, str]]], response["results"])


def read_logs(
    query: str,
    log_group_names: List[str],
    start_time: datetime.datetime = datetime.datetime(year=1970, month=1, day=1),
    end_time: datetime.datetime = datetime.datetime.now(),
    limit: Optional[int] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> pd.DataFrame:
    """Run a query against AWS CloudWatchLogs Insights and convert the results to Pandas DataFrame.

    https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CWL_QuerySyntax.html

    Parameters
    ----------
    query : str
        The query string.
    log_group_names : str
        The list of log groups to be queried. You can include up to 20 log groups.
    start_time : datetime.datetime
        The beginning of the time range to query.
    end_time : datetime.datetime
        The end of the time range to query.
    limit : Optional[int]
        The maximum number of log events to return in the query.
    boto3_session : boto3.Session(), optional
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
    results: List[List[Dict[str, str]]] = run_query(
        query=query,
        log_group_names=log_group_names,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
        boto3_session=boto3_session,
    )
    pre_df: List[Dict[str, str]] = []
    for row in results:
        new_row: Dict[str, str] = {}
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
