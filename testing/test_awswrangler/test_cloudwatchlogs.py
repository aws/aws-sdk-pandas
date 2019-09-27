import logging
from datetime import datetime
from time import sleep

import pytest
import boto3

from awswrangler import Session
from awswrangler.exceptions import QueryCancelled

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)


@pytest.fixture(scope="module")
def cloudformation_outputs():
    response = boto3.client("cloudformation").describe_stacks(
        StackName="aws-data-wrangler-test-arena")
    outputs = {}
    for output in response.get("Stacks")[0].get("Outputs"):
        outputs[output.get("OutputKey")] = output.get("OutputValue")
    yield outputs


@pytest.fixture(scope="module")
def session():
    yield Session()


@pytest.fixture(scope="module")
def loggroup(cloudformation_outputs):
    if "LogGroupName" in cloudformation_outputs:
        database = cloudformation_outputs["LogGroupName"]
    else:
        raise Exception(
            "You must deploy the test infrastructure using Cloudformation!")
    yield database


@pytest.fixture(scope="module")
def logstream(cloudformation_outputs, loggroup):
    if "LogStream" in cloudformation_outputs:
        logstream = cloudformation_outputs["LogStream"]
    else:
        raise Exception(
            "You must deploy the test infrastructure using Cloudformation!")
    client = boto3.client("logs")
    response = client.describe_log_streams(logGroupName=loggroup,
                                           logStreamNamePrefix=logstream)
    token = response["logStreams"][0].get("uploadSequenceToken")
    events = []
    for i in range(5):
        events.append({
            "timestamp": int(1000 * datetime.utcnow().timestamp()),
            "message": str(i)
        })
    args = {
        "logGroupName": loggroup,
        "logStreamName": logstream,
        "logEvents": events
    }
    if token:
        args["sequenceToken"] = token
    client.put_log_events(**args)
    sleep(300)
    yield logstream


def test_query_cancelled(session, loggroup, logstream):
    client_logs = boto3.client("logs")
    query_id = session.cloudwatchlogs.start_query(
        log_group_names=[loggroup],
        query="fields @timestamp, @message | sort @timestamp desc | limit 5",
    )
    client_logs.stop_query(queryId=query_id)
    with pytest.raises(QueryCancelled):
        assert session.cloudwatchlogs.wait_query(query_id=query_id)


def test_start_and_wait_query(session, loggroup, logstream):
    query_id = session.cloudwatchlogs.start_query(
        log_group_names=[loggroup],
        query="fields @timestamp, @message | sort @timestamp desc | limit 5",
    )
    response = session.cloudwatchlogs.wait_query(query_id=query_id)
    results = response["results"]
    assert len(results) == 5
    assert len(results[0]) == 3


def test_query(session, loggroup, logstream):
    results = session.cloudwatchlogs.query(
        log_group_names=[loggroup],
        query="fields @timestamp, @message | sort @timestamp desc | limit 5",
    )
    assert len(results) == 5
    assert len(results[0]) == 3
