import logging
import os

import pytest

import awswrangler as wr
from awswrangler.s3._fs import open_s3_object

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


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
    size = 1 * 2 ** 20  # 1 MB
    wr.config.s3_read_ahead_size = size
    with open_s3_object(path, mode="wb") as s3obj:
        s3obj.write(b"foo")
    with open_s3_object(path, mode="rb") as s3obj:
        assert s3obj._s3_read_ahead_size == size

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
