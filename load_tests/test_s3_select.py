import time

import pytest

import awswrangler as wr


@pytest.mark.repeat(1)
@pytest.mark.parametrize("benchmark_time", [100])
def test_s3_select(benchmark_time):
    start = time.time()

    path = "s3://ursa-labs-taxi-data/2018/1*.parquet"
    df = wr.s3.select_query(
        sql="SELECT * FROM s3object",
        path=path,
        input_serialization="Parquet",
        input_serialization_params={},
        scan_range_chunk_size=16 * 1024 * 1024,
    )
    end = time.time()

    elapsed_time = end - start
    assert elapsed_time < benchmark_time
