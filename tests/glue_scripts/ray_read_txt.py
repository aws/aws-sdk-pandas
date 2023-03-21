import os

import ray

ray.init()

ray.data.read_csv(paths=f"s3://{os.environ['data-gen-bucket']}/csv/small/partitioned/", parallelism=1000).to_modin()
