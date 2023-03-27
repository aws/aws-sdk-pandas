import os

import awswrangler as wr

input_path = os.environ["input-path"]
output_path = os.environ["output-path"]

df = wr.s3.read_parquet(path=input_path)

df.to_parquet(
    path=output_path,
    partition_cols=["payment_type", "passenger_count"],
)
