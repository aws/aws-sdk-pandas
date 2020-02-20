import logging
import pickle

import boto3
from botocore.config import Config

import awswrangler as wr

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)


def is_account_id_valid(session):
    account_id = session.boto3_session.client("sts").get_caller_identity().get(
        "Account")
    return type(account_id) == str


def test_session():
    assert is_account_id_valid(wr.Session()) is True
    assert is_account_id_valid(
        wr.Session(boto3_session=boto3.Session(
            region_name="us-east-1"))) is True
    assert is_account_id_valid(
        wr.Session(boto3_session=boto3.Session(
            region_name="us-east-2"))) is True
    assert is_account_id_valid(
        wr.Session(
            boto3_session=boto3.Session(region_name="us-east-1"),
            botocore_config=Config(retries={"max_attempts": 15}))) is True


def test_primitives():
    session = wr.Session(boto3_session=boto3.Session(region_name="us-east-2"),
                         botocore_config=Config(retries={"max_attempts": 15}))
    primitives = session.primitives
    buf = pickle.dumps(primitives)
    primitives2 = pickle.loads(buf)
    session2 = primitives2.build_session()
    assert is_account_id_valid(session2) is True
    assert session.boto3_session.region_name == "us-east-2"
