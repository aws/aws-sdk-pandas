import logging
import os
from unittest.mock import patch

import boto3
import botocore
import pytest

import awswrangler as wr
from awswrangler.s3._fs import open_s3_object

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


def _urls_test(glue_database):
    original = botocore.client.ClientCreator.create_client

    def wrapper(self, **kwarg):
        name = kwarg["service_name"]
        url = kwarg["endpoint_url"]
        if name == "sts":
            assert url == wr.config.sts_endpoint_url
        elif name == "athena":
            assert url == wr.config.athena_endpoint_url
        elif name == "s3":
            assert url == wr.config.s3_endpoint_url
        elif name == "glue":
            assert url == wr.config.glue_endpoint_url
        return original(self, **kwarg)

    with patch("botocore.client.ClientCreator.create_client", new=wrapper):
        wr.athena.read_sql_query(sql="SELECT 1 as col0", database=glue_database)


def test_basics(path, glue_database, glue_table, workgroup0, workgroup1):
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
    wr.config.s3_block_size = size
    with open_s3_object(path, mode="wb") as s3obj:
        s3obj.write(b"foo")
    with open_s3_object(path, mode="rb") as s3obj:
        assert s3obj._s3_block_size == size

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

    # Workgroup
    wr.config.workgroup = workgroup0
    df = wr.athena.read_sql_query(sql="SELECT 1 as col0", database=glue_database)
    assert df.query_metadata["WorkGroup"] == workgroup0
    os.environ["WR_WORKGROUP"] = workgroup1
    wr.config.reset()
    df = wr.athena.read_sql_query(sql="SELECT 1 as col0", database=glue_database)
    assert df.query_metadata["WorkGroup"] == workgroup1

    # Endpoints URLs
    region = boto3.Session().region_name
    wr.config.sts_endpoint_url = f"https://sts.{region}.amazonaws.com"
    wr.config.s3_endpoint_url = f"https://s3.{region}.amazonaws.com"
    wr.config.athena_endpoint_url = f"https://athena.{region}.amazonaws.com"
    wr.config.glue_endpoint_url = f"https://glue.{region}.amazonaws.com"
    _urls_test(glue_database)
    os.environ["WR_STS_ENDPOINT_URL"] = f"https://sts.{region}.amazonaws.com"
    os.environ["WR_S3_ENDPOINT_URL"] = f"https://s3.{region}.amazonaws.com"
    os.environ["WR_ATHENA_ENDPOINT_URL"] = f"https://athena.{region}.amazonaws.com"
    os.environ["WR_GLUE_ENDPOINT_URL"] = f"https://glue.{region}.amazonaws.com"
    wr.config.reset()
    _urls_test(glue_database)
