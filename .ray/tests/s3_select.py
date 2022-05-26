import logging
import time

import pytest

import awswrangler as wr
import ray

logging.basicConfig(level=logging.INFO, format="[%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)
logging.getLogger("botocore.credentials").setLevel(logging.CRITICAL)


@pytest.mark.repeat(10)  # set for explicit repeats, alternatively pass '--count n' at runtime
@pytest.mark.parametrize("benchmark_time", [60])
def test_s3_select(benchmark_time):
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

    elapsed_time = end - start
    print(f"Benchmark time: {benchmark_time}\nElapsed time: {elapsed_time}")
    assert elapsed_time < benchmark_time
