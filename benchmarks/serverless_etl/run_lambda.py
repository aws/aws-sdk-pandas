import os
import json
from time import sleep, time
from pprint import pprint

import boto3
from botocore.client import Config

from awswrangler.s3.utils import delete_objects


def clean_output(bucket):
    path = f"s3://{bucket}/lambda_output/"
    print(f"Cleaning {path}*")
    delete_objects(path)


def run_job(name, workload, bucket):
    key = f"workload_{workload:05}mb.csv"

    config_dict = {"read_timeout": 960, "retries": {"max_attempts": 0}}
    config = Config(**config_dict)
    client = boto3.client("lambda", config=config)
    print(f"Starting lambda for {key}")
    payload = {"--workload": key, "--bucket": bucket}
    start = time()
    try:
        client.invoke(
            FunctionName=name,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload),
        )
        elapsed = time() - start
    except:  # noqa
        elapsed = 0

    return elapsed, elapsed


def main():
    current_path = __file__.rpartition("/")[0]
    configs = json.load(open(f"{current_path}/config.json"))
    bucket = configs.get("bucket")
    name = configs.get("lambda-name")
    workloads = [wl for wl in configs.get("workloads") if wl < 1024]
    metrics = {}
    for wl in workloads:
        clean_output(bucket)
        print("Waiting warm up ends...")
        sleep(600)
        elapsed, execution = run_job(name, wl, bucket)
        metrics[wl] = {
            "ExecutionTime": execution,
            "TotalElapsedTime": elapsed,
            "WarmUp": elapsed - execution,
        }
        print(f"metrics: {metrics[wl]}")
    print("Total metrics:")
    pprint(metrics)
    if not os.path.exists("metrics"):
        os.makedirs("metrics")
    json.dump(metrics, open("metrics/lambda.json", "w"))


if __name__ == "__main__":
    main()
