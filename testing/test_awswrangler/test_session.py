import multiprocessing as mp
import logging

import pytest

from awswrangler import Session

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s"
)
logging.getLogger("awswrangler").setLevel(logging.DEBUG)


def assert_account_id(session):
    account_id = (session.boto3_session.client("sts").get_caller_identity().get("Account"))
    assert type(account_id) == str


@pytest.fixture(scope="module")
def default_session():
    yield Session()


def test_session(default_session):
    assert_account_id(default_session)


def test_session_region():
    assert_account_id(Session(region_name="us-east-1"))


def test_from_boto3_session(default_session):
    assert_account_id(Session(boto3_session=default_session.boto3_session))


def test_from_boto3_keys(default_session):
    assert_account_id(
        Session(
            aws_access_key_id=default_session.aws_access_key_id,
            aws_secret_access_key=default_session.aws_secret_access_key,
        )
    )


def test_from_boto3_region_name(default_session):
    assert_account_id(Session(region_name=default_session.region_name))


def test_cpu_count():
    assert_account_id(Session(procs_cpu_bound=1, procs_io_bound=1, botocore_max_retries=1))


def get_account_id_remote(primitives, account_id):
    account_id.value = (
        primitives.session.boto3_session.client("sts").get_caller_identity().get("Account")
    )


def test_multiprocessing(default_session):
    primitives = default_session.primitives
    account_id = mp.Manager().Value(typecode=str, value=None)
    proc = mp.Process(target=get_account_id_remote, args=(primitives, account_id))
    proc.start()
    proc.join()
    assert type(account_id.value) == str
