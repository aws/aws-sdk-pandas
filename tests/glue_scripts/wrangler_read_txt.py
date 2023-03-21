import os

import awswrangler as wr

wr.s3.read_csv(
    path=f"s3://{os.environ['data-gen-bucket']}/csv/small/partitioned/",
    ray_args={"parallelism": 1000},
)
