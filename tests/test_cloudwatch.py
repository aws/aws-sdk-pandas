import logging

import boto3
import pytest

import awswrangler as wr
from awswrangler import exceptions

logging.basicConfig(level=logging.INFO, format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)
logging.getLogger("botocore.credentials").setLevel(logging.CRITICAL)


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
