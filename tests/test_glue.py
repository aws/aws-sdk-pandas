import logging

import pandas as pd

import awswrangler as wr

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


def test_parquet_crawler_columns(path):
    df = pd.DataFrame({"c0": [0, 1], "c1": [2, 3]})
    wr.s3.to_parquet(df, path, dataset=True, mode="overwrite")
    df = pd.DataFrame({"c1": [2, 3], "c0": [0, 1]})
    wr.s3.to_parquet(df, path, dataset=True, mode="append")
    first_schema = wr.s3.read_parquet_metadata(path=path)[0]
    for _ in range(10):
        schema = wr.s3.read_parquet_metadata(path=path)[0]
        assert list(schema.keys()) == list(first_schema.keys())
