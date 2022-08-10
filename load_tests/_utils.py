import random
from datetime import datetime
from timeit import default_timer as timer
from typing import Iterator

import boto3
import botocore.exceptions

import awswrangler as wr
from awswrangler._utils import try_it

CFN_VALID_STATUS = ["CREATE_COMPLETE", "ROLLBACK_COMPLETE", "UPDATE_COMPLETE", "UPDATE_ROLLBACK_COMPLETE"]


class ExecutionTimer:
    def __init__(self, msg="elapsed time"):
        self.msg = msg

    def __enter__(self):
        self.before = timer()
        return self

    def __exit__(self, type, value, traceback):
        self.elapsed_time = round((timer() - self.before), 3)
        print(f"{self.msg}: {self.elapsed_time:.3f} sec")
        return None


def extract_cloudformation_outputs():
    outputs = {}
    client = boto3.client("cloudformation")
    response = try_it(client.describe_stacks, botocore.exceptions.ClientError, max_num_tries=5)
    for stack in response.get("Stacks"):
        if (
            stack["StackName"]
            in ["aws-data-wrangler-base", "aws-data-wrangler-databases", "aws-data-wrangler-opensearch"]
        ) and (stack["StackStatus"] in CFN_VALID_STATUS):
            for output in stack.get("Outputs"):
                outputs[output.get("OutputKey")] = output.get("OutputValue")
    return outputs


def get_time_str_with_random_suffix() -> str:
    time_str = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
    return f"{time_str}_{random.randrange(16**6):06x}"


def path_generator(bucket: str) -> Iterator[str]:
    s3_path = f"s3://{bucket}/{get_time_str_with_random_suffix()}/"
    print(f"S3 Path: {s3_path}")
    objs = wr.s3.list_objects(s3_path)
    wr.s3.delete_objects(path=objs)
    yield s3_path
    objs = wr.s3.list_objects(s3_path)
    wr.s3.delete_objects(path=objs)
