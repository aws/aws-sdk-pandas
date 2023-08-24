import logging
from datetime import datetime

import boto3
import pytest

import awswrangler as wr
from awswrangler import exceptions

logging.getLogger("awswrangler").setLevel(logging.DEBUG)

pytestmark = pytest.mark.distributed


def test_query_cancelled(loggroup: str) -> None:
    client_logs = boto3.client("logs")
    with pytest.raises(exceptions.QueryCancelled):
        while True:
            query_id = wr.cloudwatch.start_query(
                log_group_names=[loggroup], query="fields @timestamp, @message | sort @timestamp desc"
            )
            try:
                client_logs.stop_query(queryId=query_id)
                break
            except Exception as ex:
                if "is not in Running or Scheduled state" not in str(ex):
                    raise ex
        wr.cloudwatch.wait_query(query_id=query_id)


def test_start_and_wait_query(loggroup: str) -> None:
    query_id = wr.cloudwatch.start_query(
        log_group_names=[loggroup], query="fields @timestamp, @message | sort @timestamp desc | limit 5"
    )
    response = wr.cloudwatch.wait_query(query_id=query_id)
    results = response["results"]
    assert len(results) == 5
    assert len(results[0]) == 3


def test_query(loggroup: str) -> None:
    results = wr.cloudwatch.run_query(
        log_group_names=[loggroup], query="fields @timestamp, @message | sort @timestamp desc | limit 5"
    )
    assert len(results) == 5
    assert len(results[0]) == 3


def test_read_logs(loggroup: str) -> None:
    df = wr.cloudwatch.read_logs(
        log_group_names=[loggroup], query="fields @timestamp, @message | sort @timestamp desc | limit 5", limit=5
    )
    assert len(df.index) == 5
    assert len(df.columns) == 3


def test_read_logs_by_log_group_arn(loggroup: str, account_id: str, region: str) -> None:
    loggroup_arn = f"arn:aws:logs:{region}:{account_id}:log-group:{loggroup}"
    df = wr.cloudwatch.read_logs(
        log_group_names=[loggroup_arn], query="fields @timestamp, @message | sort @timestamp desc | limit 5", limit=5
    )
    assert len(df.index) == 5
    assert len(df.columns) == 3


def test_describe_log_streams_and_filter_log_events(loggroup: str) -> None:
    cloudwatch_log_client = boto3.client("logs")
    log_stream_names = [
        "aws_sdk_pandas_log_stream_one",
        "aws_sdk_pandas_log_stream_two",
        "aws_sdk_pandas_log_stream_three",
        "aws_sdk_pandas_log_stream_four",
    ]
    for log_stream in log_stream_names:
        try:
            cloudwatch_log_client.create_log_stream(logGroupName=loggroup, logStreamName=log_stream)
        except cloudwatch_log_client.exceptions.ResourceAlreadyExistsException:
            continue

    with pytest.raises(exceptions.InvalidArgumentCombination):
        wr.cloudwatch.describe_log_streams(
            log_group_name=loggroup,
            log_stream_name_prefix="aws_sdk_pandas_log_stream",
            order_by="LastEventTime",
            descending=False,
        )

    log_streams_df = wr.cloudwatch.describe_log_streams(
        log_group_name=loggroup, order_by="LastEventTime", descending=False
    )

    assert len(log_streams_df.index) >= 4
    assert "logGroupName" in log_streams_df.columns

    log_streams_df.dropna(inplace=True)
    for log_stream in log_streams_df.to_dict("records"):
        events = []
        token = log_stream.get("uploadSequenceToken")
        for i, event_message in enumerate(["REPORT", "DURATION", "key:value", "START", "END"]):
            events.append({"timestamp": int(1000 * datetime.now().timestamp()), "message": f"{i}_{event_message}"})
        args = {
            "logGroupName": log_stream["logGroupName"],
            "logStreamName": log_stream["logStreamName"],
            "logEvents": events,
        }
        if token:
            args["sequenceToken"] = token
        try:
            cloudwatch_log_client.put_log_events(**args)
        except cloudwatch_log_client.exceptions.DataAlreadyAcceptedException:
            pass

    with pytest.raises(exceptions.InvalidArgumentCombination):
        wr.cloudwatch.filter_log_events(
            log_group_name=loggroup,
            log_stream_name_prefix="aws_sdk_pandas_log_stream",
            log_stream_names=log_streams_df["logStreamName"].tolist(),
        )

    log_events_df = wr.cloudwatch.filter_log_events(log_group_name=loggroup)
    assert len(log_events_df.index) >= 4
    assert "logGroupName" in log_events_df.columns

    filtered_log_events_df = wr.cloudwatch.filter_log_events(
        log_group_name=loggroup, log_stream_names=log_streams_df["logStreamName"].tolist(), filter_pattern="REPORT"
    )
    assert len(filtered_log_events_df.index) >= 4
    assert "logStreamName" in log_events_df.columns
