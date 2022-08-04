import time

import pandas as pd
import pytest

import awswrangler as wr


@pytest.mark.repeat(1)
@pytest.mark.parametrize("benchmark_time", [150])
def test_s3_select(benchmark_time):
    start = time.time()

    path = "s3://ursa-labs-taxi-data/2018/1*.parquet"
    wr.s3.select_query(
        sql="SELECT * FROM s3object",
        path=path,
        input_serialization="Parquet",
        input_serialization_params={},
        scan_range_chunk_size=16 * 1024 * 1024,
    )
    end = time.time()

    elapsed_time = end - start
    assert elapsed_time < benchmark_time


def test_s3_delete_objects(path, path2):
    df = pd.DataFrame({"id": [1, 2, 3]})
    objects_per_bucket = 5000
    paths = [f"s3://{path}delete-test{i}.json" for i in range(objects_per_bucket)] + [
        f"s3://{path2}delete-test{i}.json" for i in range(objects_per_bucket)
    ]
    for path in paths:
        wr.s3.to_json(df, path)
    wr.s3.delete_objects(path=paths)

    assert len(wr.s3.list_objects(f"{path}delete-test*")) == 0
    assert len(wr.s3.list_objects(f"{path2}delete-test*")) == 0
