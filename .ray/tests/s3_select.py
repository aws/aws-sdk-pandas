import logging
import sys
import time

import awswrangler as wr
import ray

logging.basicConfig(level=logging.INFO, format="[%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)
logging.getLogger("botocore.credentials").setLevel(logging.CRITICAL)

BENCHMARK_TIME = 60
start = time.time()
paths = [
    "s3://nyc-tlc/trip data/yellow_tripdata_2021-01.parquet",
    "s3://nyc-tlc/trip data/yellow_tripdata_2021-02.parquet",
]
print(f"S3 Select path: {paths}")
df = wr.s3.select_query(
    sql="SELECT * FROM s3object",
    path=paths,
    input_serialization="Parquet",
    input_serialization_params={},
    use_threads=True,
    scan_range_chunk_size=1024 * 1024 * 32,
)
end = time.time()

print(df)
elapsed_time = end - start
print(f"Elapsed time: {elapsed_time}")
if elapsed_time > BENCHMARK_TIME:
    sys.exit(f"Test has not met benchmark time of {BENCHMARK_TIME} seconds")
