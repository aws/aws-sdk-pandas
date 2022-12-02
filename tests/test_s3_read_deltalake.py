import logging

import pandas as pd
from deltalake import write_deltalake

import awswrangler as wr

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


def test_read_deltalake(path):
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [0, 1, 2], "c2": [0, 0, 1]})
    write_deltalake(table_or_uri=path, data=df)
    results = wr.s3.read_deltalake(path=path)
    assert results == df
