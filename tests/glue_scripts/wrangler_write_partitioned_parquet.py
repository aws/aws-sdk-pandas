import os

import awswrangler as wr

df = wr.s3.read_parquet(
    path=f"s3://{os.environ['data-gen-bucket']}/parquet/medium/partitioned/",
    ray_args={"override_num_blocks": 1000},
)

wr.s3.to_parquet(
    df=df,
    path=os.environ["output-path"],
    dataset=True,
    partition_cols=["payment_type", "passenger_count"],
)
