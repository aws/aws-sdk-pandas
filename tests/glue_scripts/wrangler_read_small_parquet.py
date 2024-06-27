import os

import awswrangler as wr

wr.s3.read_parquet(
    path=f"s3://{os.environ['data-gen-bucket']}/parquet/small/partitioned/",
    ray_args={"override_num_blocks": 1000, "bulk_read": True},
)
