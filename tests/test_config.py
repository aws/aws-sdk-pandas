import logging
import os
from unittest.mock import create_autospec, patch

import boto3
import botocore
import botocore.client
import botocore.config
import pytest

import awswrangler as wr
from awswrangler._config import apply_configs
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

    # Configuring default database with wrong value
    wr.config.database = "missing_database"
    with pytest.raises(boto3.client("glue").exceptions.EntityNotFoundException):
        wr.catalog.create_parquet_table(**args)

    # Overwriting configured database
    wr.catalog.create_parquet_table(database=glue_database, **args)

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


def test_athena_cache_configuration():
    wr.config.max_local_cache_entries = 20
    assert wr.config.max_remote_cache_entries == 20


def test_botocore_config(path):
    original = botocore.client.ClientCreator.create_client

    # Default values for botocore.config.Config
    expected_max_retries_attempt = 5
    expected_connect_timeout = 10
    expected_max_pool_connections = 10
    expected_retry_mode = None

    def wrapper(self, **kwarg):
        assert kwarg["client_config"].retries["max_attempts"] == expected_max_retries_attempt
        assert kwarg["client_config"].connect_timeout == expected_connect_timeout
        assert kwarg["client_config"].max_pool_connections == expected_max_pool_connections
        assert kwarg["client_config"].retries.get("mode") == expected_retry_mode
        return original(self, **kwarg)

    # Check for default values
    with patch("botocore.client.ClientCreator.create_client", new=wrapper):
        with open_s3_object(path, mode="wb") as s3obj:
            s3obj.write(b"foo")

    # Update default config with environment variables
    expected_max_retries_attempt = 20
    expected_connect_timeout = 10
    expected_max_pool_connections = 10
    expected_retry_mode = "adaptive"

    os.environ["AWS_MAX_ATTEMPTS"] = str(expected_max_retries_attempt)
    os.environ["AWS_RETRY_MODE"] = expected_retry_mode

    with patch("botocore.client.ClientCreator.create_client", new=wrapper):
        with open_s3_object(path, mode="wb") as s3obj:
            s3obj.write(b"foo")

    del os.environ["AWS_MAX_ATTEMPTS"]
    del os.environ["AWS_RETRY_MODE"]

    # Update botocore.config.Config
    expected_max_retries_attempt = 30
    expected_connect_timeout = 40
    expected_max_pool_connections = 50
    expected_retry_mode = "legacy"

    botocore_config = botocore.config.Config(
        retries={"max_attempts": expected_max_retries_attempt, "mode": expected_retry_mode},
        connect_timeout=expected_connect_timeout,
        max_pool_connections=expected_max_pool_connections,
    )
    wr.config.botocore_config = botocore_config

    with patch("botocore.client.ClientCreator.create_client", new=wrapper):
        with open_s3_object(path, mode="wb") as s3obj:
            s3obj.write(b"foo")

    wr.config.reset()


@pytest.mark.xfail(raises=AssertionError)
def test_chunk_size():
    expected_chunksize = 123

    wr.config.chunksize = expected_chunksize

    for function_to_mock in [wr.postgresql.to_sql, wr.mysql.to_sql, wr.sqlserver.to_sql, wr.redshift.to_sql]:
        mock = create_autospec(function_to_mock)
        apply_configs(mock)(df=None, con=None, table=None, schema=None)
        mock.assert_called_with(df=None, con=None, table=None, schema=None, chunksize=expected_chunksize)

    expected_chunksize = 456
    os.environ["WR_CHUNKSIZE"] = str(expected_chunksize)
    wr.config.reset()

    for function_to_mock in [wr.postgresql.to_sql, wr.mysql.to_sql, wr.sqlserver.to_sql, wr.redshift.to_sql]:
        mock = create_autospec(function_to_mock)
        apply_configs(mock)(df=None, con=None, table=None, schema=None)
        mock.assert_called_with(df=None, con=None, table=None, schema=None, chunksize=expected_chunksize)
