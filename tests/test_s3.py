import logging
import time
from unittest.mock import patch

import botocore
import pytest

import awswrangler as wr

from ._utils import extract_cloudformation_outputs

API_CALL = botocore.client.BaseClient._make_api_call

logging.basicConfig(level=logging.INFO, format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)
logging.getLogger("botocore.credentials").setLevel(logging.CRITICAL)


@pytest.fixture(scope="module")
def cloudformation_outputs():
    yield extract_cloudformation_outputs()


@pytest.fixture(scope="module")
def bucket(cloudformation_outputs):
    if "BucketName" in cloudformation_outputs:
        bucket = cloudformation_outputs["BucketName"]
    else:
        raise Exception("You must deploy/update the test infrastructure (CloudFormation)")
    yield bucket


def test_delete_internal_error(bucket):
    response = {
        "Errors": [
            {
                "Key": "foo/dt=2020-01-01 00%3A00%3A00/boo.txt",
                "Code": "InternalError",
                "Message": "We encountered an internal error. Please try again.",
            }
        ]
    }

    def mock_make_api_call(self, operation_name, kwarg):
        if operation_name == "DeleteObjects":
            return response
        return API_CALL(self, operation_name, kwarg)

    start = time.time()
    with patch("botocore.client.BaseClient._make_api_call", new=mock_make_api_call):
        path = f"s3://{bucket}/foo/dt=2020-01-01 00:00:00/boo.txt"
        with pytest.raises(wr.exceptions.ServiceApiError):
            wr.s3.delete_objects(path=[path])
    assert 15 <= (time.time() - start) <= 20
