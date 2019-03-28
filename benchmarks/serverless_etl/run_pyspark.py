import os
import json
from time import sleep
from pprint import pprint

import boto3

from awswrangler.s3.utils import delete_objects


def clean_output(bucket):
    path = f"s3://{bucket}/pyspark_output/"
    print(f"Cleaning {path}*")
    delete_objects(path)


def run_job(name, workload, bucket):
    key = f"workload_{workload:05}mb.csv"
    client = boto3.client("glue")
    print(f"Starting Pyspark job for {key}")
    response = client.start_job_run(
        JobName=name, Arguments={"--workload": key, "--bucket": bucket}
    )
    run_id = response.get("JobRunId")
    while True:
        sleep(2)
        response = client.get_job_run(JobName=name, RunId=run_id)
        state = response.get("JobRun").get("JobRunState")
        if state == "SUCCEEDED":
            break
        elif state in ["STOPPED", "FAILED", "TIMEOUT"]:
            raise Exception(f"Invalid state: {state}")
    print(f"Finished Pyspark job for {key}")
    elapsed = response.get("JobRun").get("CompletedOn") - response.get("JobRun").get(
        "StartedOn"
    )
    return elapsed.total_seconds(), response.get("JobRun").get("ExecutionTime")


def main():
    current_path = __file__.rpartition("/")[0]
    configs = json.load(open(f"{current_path}/config.json"))
    bucket = configs.get("bucket")
    name = configs.get("pyspark-name")
    workloads = configs.get("workloads")
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
    json.dump(metrics, open("metrics/pyspark.json", "w"))


if __name__ == "__main__":
    main()
