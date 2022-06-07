"""
This is a very basic sample script for running things remotely.
It requires `aws_credentials` file to be present in current working directory.
On credentials file format see https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html#cli-configure-files-where
"""
import logging

import modin.pandas as pd
from modin.experimental.cloud import cluster

logging.basicConfig(format="%(asctime)s %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
cluster_settings = {"region": "us-west-2", "zone": "us-west-2a", "image": "ami-058516e5015b8db5f"}
example_cluster = cluster.create("aws", **cluster_settings)
with example_cluster:
    remote_df = pd.DataFrame([1, 2, 3, 4])
    print(len(remote_df))  # len() is executed remotely
