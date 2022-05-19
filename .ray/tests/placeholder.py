import ray
import awswrangler as wr

ray.init("ray://localhost:10001", runtime_env={"py_modules": [wr]})

import time
import logging
logging.basicConfig(level=logging.INFO, format="[%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)
logging.getLogger("botocore.credentials").setLevel(logging.CRITICAL)

start = time.time()
path = "s3://nyc-tlc/trip data/fhvhv_tripdata_2020-*"
print(f"S3 Select path: {path}")
df = wr.s3.select_query(
    sql="SELECT * FROM s3object",
    path=path,
    input_serialization="CSV",
    input_serialization_params={
        "FileHeaderInfo": "Use",
        "RecordDelimiter": "\r\n",
    },
    use_threads=True,
    scan_range_chunk_size=1024*1024*32,
)
end = time.time()

print(df)
print(f"Elapsed time: {end - start}")