import pandas as pd
import pytest

import awswrangler as wr

from ._utils import ExecutionTimer


@pytest.mark.repeat(1)
@pytest.mark.parametrize("benchmark_time", [150])
def test_s3_select(benchmark_time):

    path = "s3://ursa-labs-taxi-data/2018/1*.parquet"
    with ExecutionTimer("elapsed time of wr.s3.select_query()") as timer:
        wr.s3.select_query(
            sql="SELECT * FROM s3object",
            path=path,
            input_serialization="Parquet",
            input_serialization_params={},
            scan_range_chunk_size=16 * 1024 * 1024,
        )

    assert timer.elapsed_time < benchmark_time


@pytest.mark.parametrize("benchmark_time", [5])
def test_s3_delete_objects(path, path2, benchmark_time):
    df = pd.DataFrame({"id": [1, 2, 3]})
    objects_per_bucket = 505
    paths1 = [f"{path}delete-test{i}.json" for i in range(objects_per_bucket)]
    paths2 = [f"{path2}delete-test{i}.json" for i in range(objects_per_bucket)]
    paths = paths1 + paths2
    for path in paths:
        wr.s3.to_json(df, path)
    with ExecutionTimer("elapsed time of wr.s3.delete_objects()") as timer:
        wr.s3.delete_objects(path=paths)
    assert timer.elapsed_time < benchmark_time
    assert len(wr.s3.list_objects(f"{path}delete-test*")) == 0
    assert len(wr.s3.list_objects(f"{path2}delete-test*")) == 0
