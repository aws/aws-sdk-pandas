import logging
from datetime import datetime

import boto3
import pytest

import awswrangler as wr
from awswrangler import exceptions

from ._utils import CFN_VALID_STATUS

logging.basicConfig(level=logging.INFO, format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)
logging.getLogger("botocore.credentials").setLevel(logging.CRITICAL)


@pytest.fixture(scope="module")
def cloudformation_outputs():
    response = boto3.client("cloudformation").describe_stacks(StackName="aws-data-wrangler")
    stack = [x for x in response.get("Stacks") if x["StackStatus"] in CFN_VALID_STATUS][0]
    outputs = {}
    for output in stack.get("Outputs"):
        outputs[output.get("OutputKey")] = output.get("OutputValue")
    yield outputs


@pytest.fixture(scope="module")
def loggroup(cloudformation_outputs):
    loggroup_name = cloudformation_outputs["LogGroupName"]
    logstream_name = cloudformation_outputs["LogStream"]
    client = boto3.client("logs")
    response = client.describe_log_streams(logGroupName=loggroup_name, logStreamNamePrefix=logstream_name)
    token = response["logStreams"][0].get("uploadSequenceToken")
    events = []
    for i in range(5):
        events.append({"timestamp": int(1000 * datetime.now().timestamp()), "message": str(i)})
    args = {"logGroupName": loggroup_name, "logStreamName": logstream_name, "logEvents": events}
    if token:
        args["sequenceToken"] = token
    try:
        client.put_log_events(**args)
    except client.exceptions.DataAlreadyAcceptedException:
        pass  # Concurrency
    while True:
        results = wr.cloudwatch.run_query(log_group_names=[loggroup_name], query="fields @timestamp | limit 5")
        if len(results) == 5:
            break
    yield loggroup_name


def test_query_cancelled(loggroup):
    client_logs = boto3.client("logs")
    query_id = wr.cloudwatch.start_query(
        log_group_names=[loggroup], query="fields @timestamp, @message | sort @timestamp desc"
    )
    client_logs.stop_query(queryId=query_id)
    with pytest.raises(exceptions.QueryCancelled):
        assert wr.cloudwatch.wait_query(query_id=query_id)


def test_start_and_wait_query(loggroup):
    query_id = wr.cloudwatch.start_query(
        log_group_names=[loggroup], query="fields @timestamp, @message | sort @timestamp desc | limit 5"
    )
    response = wr.cloudwatch.wait_query(query_id=query_id)
    results = response["results"]
    assert len(results) == 5
    assert len(results[0]) == 3


def test_query(loggroup):
    results = wr.cloudwatch.run_query(
        log_group_names=[loggroup], query="fields @timestamp, @message | sort @timestamp desc | limit 5"
    )
    assert len(results) == 5
    assert len(results[0]) == 3


def test_read_logs(loggroup):
    df = wr.cloudwatch.read_logs(
        log_group_names=[loggroup], query="fields @timestamp, @message | sort @timestamp desc | limit 5", limit=5
    )
    assert len(df.index) == 5
    assert len(df.columns) == 3
