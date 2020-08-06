import logging

import boto3

import awswrangler as wr

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


def test_default_session():
    boto3.setup_default_session(region_name="us-east-1")
    assert wr._utils.ensure_session().region_name == "us-east-1"
    boto3.setup_default_session(region_name="us-east-2")
    assert wr._utils.ensure_session().region_name == "us-east-2"
    boto3.setup_default_session(region_name="us-west-1")
    assert wr._utils.ensure_session().region_name == "us-west-1"
    boto3.setup_default_session(region_name="us-west-2")
    assert wr._utils.ensure_session().region_name == "us-west-2"
