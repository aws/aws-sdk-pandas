import os

import pytest

import awswrangler

from .utils import write_fake_objects


@pytest.fixture(scope="module")
def bucket():
    if "AWSWRANGLER_TEST_BUCKET" in os.environ:
        bucket = os.environ.get("AWSWRANGLER_TEST_BUCKET")
    else:
        raise Exception("You must provide AWSWRANGLER_TEST_BUCKET environment variable")
    yield bucket


def test_delete_objects(bucket):
    write_fake_objects(bucket, "objs/", 3)
    awswrangler.s3.utils.delete_objects("s3://" + bucket + "/objs/", batch_size=2)


def test_delete_listed_objects(bucket):
    write_fake_objects(bucket, "objs/", 3)
    keys = awswrangler.s3.utils.list_objects("s3://" + bucket + "/objs/", batch_size=2)
    assert len(keys) == 3
    awswrangler.s3.utils.delete_listed_objects(bucket, keys, batch_size=2)
    keys = awswrangler.s3.utils.list_objects("s3://" + bucket + "/objs/", batch_size=2)
    assert len(keys) == 0


def test_get_session():
    session = awswrangler.common.get_session()
    session.client("sts").get_caller_identity().get("Account")
    profile = session.profile_name
    key = session.get_credentials().access_key
    secret = session.get_credentials().secret_key
    region = session.region_name
    session = awswrangler.common.get_session(profile=profile)
    session.client("sts").get_caller_identity().get("Account")
    session = awswrangler.common.get_session(region=region)
    session.client("sts").get_caller_identity().get("Account")
    session = awswrangler.common.get_session(key=key, secret=secret)
    session.client("sts").get_caller_identity().get("Account")
    session = awswrangler.common.get_session(
        session_primitives=awswrangler.common.SessionPrimitives()
    )
    session.client("sts").get_caller_identity().get("Account")
    session = awswrangler.common.get_session(
        session_primitives=awswrangler.common.SessionPrimitives(profile=profile)
    )
    session.client("sts").get_caller_identity().get("Account")
    session = awswrangler.common.get_session(
        session_primitives=awswrangler.common.SessionPrimitives(region=region)
    )
    session.client("sts").get_caller_identity().get("Account")
    session = awswrangler.common.get_session(
        session_primitives=awswrangler.common.SessionPrimitives(key=key, secret=secret)
    )
    session.client("sts").get_caller_identity().get("Account")
