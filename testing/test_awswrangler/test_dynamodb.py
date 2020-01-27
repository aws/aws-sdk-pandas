import logging

import boto3
import pytest

# import awswrangler as wr

logging.basicConfig(level=logging.INFO, format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)


@pytest.fixture(scope="module")
def cloudformation_outputs():
    response = boto3.client("cloudformation").describe_stacks(StackName="aws-data-wrangler-test")
    outputs = {}
    for output in response.get("Stacks")[0].get("Outputs"):
        outputs[output.get("OutputKey")] = output.get("OutputValue")
    yield outputs


@pytest.fixture(scope="module")
def ddb_table_arn(session, cloudformation_outputs):
    if "DynamoDbTableARN" in cloudformation_outputs:
        arn = cloudformation_outputs["DynamoDbTableARN"]
    else:
        raise Exception("You must deploy the test infrastructure using Cloudformation!")
    yield arn
