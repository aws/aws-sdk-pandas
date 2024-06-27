import os

import ray

import awswrangler as wr

paths = wr.s3.list_objects(f"s3://{os.environ['data-gen-bucket']}/parquet/small/partitioned/")
ray.data.read_parquet_bulk(paths=paths, override_num_blocks=1000).to_modin()
