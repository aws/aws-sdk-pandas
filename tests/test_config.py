import copy
import logging
import os
from unittest.mock import patch

import boto3
import pandas as pd
import pytest
import s3fs

import awswrangler as wr

logging.basicConfig(level=logging.INFO, format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)
logging.getLogger("botocore.credentials").setLevel(logging.CRITICAL)


def test_basics(path, glue_database, glue_table):
    args = {"table": glue_table, "path": "", "columns_types": {"col0": "bigint"}}

    # Missing database argument
    with pytest.raises(TypeError):
        wr.catalog.create_parquet_table(**args)

    # Configuring default database value
    wr.config.database = glue_database

    # Testing configured database
    wr.catalog.create_parquet_table(**args)

    # Testing configured s3 block size
    size = 5 * 2 ** 20  # 5 MB
    wr.config.s3fs_block_size = size
    df = pd.DataFrame({"id": [1, 2, 3]})
    file_path = path + "0.csv"
    args = dict(
        anon=False,
        config_kwargs={"retries": {"max_attempts": 15}},
        default_block_size=size,
        default_cache_type="readahead",
        default_fill_cache=False,
        s3_additional_kwargs=None,
        skip_instance_cache=True,
        use_listings_cache=False,
        use_ssl=True,
    )
    with patch(
        "s3fs.S3FileSystem",
        return_value=s3fs.S3FileSystem(session=boto3.DEFAULT_SESSION._session, **copy.deepcopy(args)),
    ) as mock:
        wr.s3.to_csv(df, file_path, index=False)
        mock.assert_called_with(session=boto3.DEFAULT_SESSION._session, **args)
        wr.s3.read_csv([file_path])

    # Resetting all configs
    wr.config.reset()

    # Missing database argument
    with pytest.raises(TypeError):
        wr.catalog.does_table_exist(table=glue_table)

    # Configuring default database value again
    wr.config.database = glue_database

    # Testing configured database again
    assert wr.catalog.does_table_exist(table=glue_table) is True

    # Resetting this specific config
    wr.config.reset("database")

    # Missing database argument
    with pytest.raises(TypeError):
        wr.catalog.does_table_exist(table=glue_table)

    # exporting environment variable
    os.environ["WR_DATABASE"] = glue_database
    wr.config.reset("database")
    assert wr.catalog.does_table_exist(table=glue_table) is True
    del os.environ["WR_DATABASE"]
    wr.config.reset("database")

    # Missing database argument
    with pytest.raises(TypeError):
        wr.catalog.does_table_exist(table=glue_table)

    assert wr.config.to_pandas().shape == (len(wr._config._CONFIG_ARGS), 7)
