import os
import json
import string
import random
import multiprocessing as mp
import shutil

import boto3
from boto3.s3.transfer import TransferConfig


def generator(size, chars):
    return "".join(random.choice(chars) for _ in range(size))


def generate_row_64bytes():
    picked_integer = generator(10, string.digits)
    picked_float = generator(5, string.digits) + "." + generator(5, string.digits)
    picked_string = generator(38, string.ascii_letters)
    picked_partition = generator(1, ["1", "2", "3", "4"])
    return (
        ",".join([picked_integer, picked_float, picked_string, picked_partition]) + "\n"
    )


def create_workload_wrapper(args):
    return create_workload(**args)


def create_workload(workload, bucket):
    key = f"workload_{workload:05}mb.csv"
    filepath = f"workloads/{key}"
    num_of_rows = int((workload * 1_000_000) / 64)
    print(f"writing {filepath}...")
    header = "col0,col1,col2,col3\n"
    f = open(filepath, "w")
    f.write(header)
    for _ in range(num_of_rows):
        f.write(generate_row_64bytes())
    f.close()
    print(f"uploading s3://{bucket}/{key}...")
    client = boto3.client("s3")
    config = TransferConfig()
    transfer = boto3.s3.transfer.S3Transfer(client=client, config=config)
    transfer.upload_file(filename=filepath, bucket=bucket, key=key)
    print(f"deleting {filepath}...")
    os.remove(filepath)
    print(f"{filepath} done!")


def main():
    current_path = __file__.rpartition("/")[0]
    configs = json.load(open(f"{current_path}/config.json"))
    bucket = configs.get("bucket")
    workloads = configs.get("workloads")
    shutil.rmtree("workloads")
    if not os.path.exists("workloads"):
        os.makedirs("workloads")
    args = []
    for wl in workloads:
        args.append({"workload": wl, "bucket": bucket})
    pool = mp.Pool(mp.cpu_count())
    pool.map(create_workload_wrapper, args)


if __name__ == "__main__":
    main()
