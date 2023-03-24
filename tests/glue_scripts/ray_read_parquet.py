import os

import ray

ray.init()

ray.data.read_parquet(paths=f"s3://{os.environ['data-gen-bucket']}/parquet/small/partitioned/", parallelism=1000).to_modin()
