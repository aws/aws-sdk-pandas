import logging
import os
from types import ModuleType
from typing import Optional
from unittest.mock import create_autospec, patch

import boto3
import botocore
import botocore.client
import botocore.config
import pytest

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


def _urls_test(wr: ModuleType, glue_database: str) -> None:
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
        elif name == "secretsmanager":
            assert url == wr.config.secretsmanager_endpoint_url
        elif name == "timestream-write":
            assert url == wr.config.timestream_write_endpoint_url
        elif name == "timestream-query":
            assert url == wr.config.timestream_query_endpoint_url
        return original(self, **kwarg)

    with patch("botocore.client.ClientCreator.create_client", new=wrapper):
        wr.athena.read_sql_query(sql="SELECT 1 as col0", database=glue_database)


def test_basics(
    wr: ModuleType, path: str, glue_database: str, glue_table: str, workgroup0: str, workgroup1: str
) -> None:
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
    size = 1 * 2**20  # 1 MB
    wr.config.s3_block_size = size
    with wr.s3._fs.open_s3_object(path, mode="wb") as s3obj:
        s3obj.write(b"foo")
    with wr.s3._fs.open_s3_object(path, mode="rb") as s3obj:
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
    with patch.dict(os.environ, {"WR_DATABASE": glue_database}):
        wr.config.reset("database")

    assert wr.catalog.does_table_exist(table=glue_table) is True
    wr.config.reset("database")

    # Missing database argument
    with pytest.raises(TypeError):
        wr.catalog.does_table_exist(table=glue_table)

    assert wr.config.to_pandas().shape == (len(wr._config._CONFIG_ARGS), 7)

    # Workgroup
    wr.config.workgroup = workgroup0
    df = wr.athena.read_sql_query(sql="SELECT 1 as col0", database=glue_database)
    assert df.query_metadata["WorkGroup"] == workgroup0

    with patch.dict(os.environ, {"WR_WORKGROUP": workgroup1}):
        wr.config.reset()
    df = wr.athena.read_sql_query(sql="SELECT 1 as col0", database=glue_database)
    assert df.query_metadata["WorkGroup"] == workgroup1

    # Endpoints URLs
    region = boto3.Session().region_name
    wr.config.sts_endpoint_url = f"https://sts.{region}.amazonaws.com"
    wr.config.s3_endpoint_url = f"https://s3.{region}.amazonaws.com"
    wr.config.athena_endpoint_url = f"https://athena.{region}.amazonaws.com"
    wr.config.glue_endpoint_url = f"https://glue.{region}.amazonaws.com"
    wr.config.secretsmanager_endpoint_url = f"https://secretsmanager.{region}.amazonaws.com"
    _urls_test(wr, glue_database)

    mock_environ_dict = {}
    mock_environ_dict["WR_STS_ENDPOINT_URL"] = f"https://sts.{region}.amazonaws.com"
    mock_environ_dict["WR_S3_ENDPOINT_URL"] = f"https://s3.{region}.amazonaws.com"
    mock_environ_dict["WR_ATHENA_ENDPOINT_URL"] = f"https://athena.{region}.amazonaws.com"
    mock_environ_dict["WR_GLUE_ENDPOINT_URL"] = f"https://glue.{region}.amazonaws.com"
    mock_environ_dict["WR_SECRETSMANAGER_ENDPOINT_URL"] = f"https://secretsmanager.{region}.amazonaws.com"

    with patch.dict(os.environ, mock_environ_dict):
        wr.config.reset()
    _urls_test(wr, glue_database)


def test_athena_cache_configuration(wr: ModuleType) -> None:
    wr.config.max_remote_cache_entries = 50
    wr.config.max_local_cache_entries = 20
    assert wr.config.max_remote_cache_entries == 20


@patch.dict(
    os.environ,
    {
        "WR_ATHENA_QUERY_WAIT_POLLING_DELAY": "0.1",
        "WR_LAKEFORMATION_QUERY_WAIT_POLLING_DELAY": "0.15",
        "WR_CLOUDWATCH_QUERY_WAIT_POLLING_DELAY": "0.05",
    },
)
def test_wait_time_configuration(wr: ModuleType) -> None:
    wr.config.reset()

    assert wr.config.athena_query_wait_polling_delay == 0.1
    assert wr.config.lakeformation_query_wait_polling_delay == 0.15
    assert wr.config.cloudwatch_query_wait_polling_delay == 0.05


def test_botocore_config(wr: ModuleType, path: str) -> None:
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
        with wr.s3._fs.open_s3_object(path, mode="wb") as s3obj:
            s3obj.write(b"foo")

    # Update default config with environment variables
    expected_max_retries_attempt = 20
    expected_connect_timeout = 10
    expected_max_pool_connections = 10
    expected_retry_mode = "adaptive"

    with patch.dict(
        os.environ, {"AWS_MAX_ATTEMPTS": str(expected_max_retries_attempt), "AWS_RETRY_MODE": expected_retry_mode}
    ):
        with patch("botocore.client.ClientCreator.create_client", new=wrapper):
            with wr.s3._fs.open_s3_object(path, mode="wb") as s3obj:
                s3obj.write(b"foo")

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
        with wr.s3._fs.open_s3_object(path, mode="wb") as s3obj:
            s3obj.write(b"foo")

    wr.config.reset()


def test_chunk_size(wr: ModuleType) -> None:
    expected_chunksize = 123

    wr.config.chunksize = expected_chunksize

    for function_to_mock in [wr.postgresql.to_sql, wr.mysql.to_sql, wr.sqlserver.to_sql, wr.redshift.to_sql]:
        mock = create_autospec(function_to_mock)
        wr._config.apply_configs(mock)(df=None, con=None, table=None, schema=None)
        mock.assert_called_with(df=None, con=None, table=None, schema=None, chunksize=expected_chunksize)

    expected_chunksize = 456
    with patch.dict(os.environ, {"WR_CHUNKSIZE": str(expected_chunksize)}):
        wr.config.reset()

    for function_to_mock in [wr.postgresql.to_sql, wr.mysql.to_sql, wr.sqlserver.to_sql, wr.redshift.to_sql]:
        mock = create_autospec(function_to_mock)
        wr._config.apply_configs(mock)(df=None, con=None, table=None, schema=None)
        mock.assert_called_with(df=None, con=None, table=None, schema=None, chunksize=expected_chunksize)


@pytest.mark.parametrize("polling_delay", [None, 0.05, 0.1])
def test_athena_wait_delay_config(wr: ModuleType, glue_database: str, polling_delay: Optional[float]) -> None:
    if polling_delay:
        wr.config.athena_query_wait_polling_delay = polling_delay
    else:
        polling_delay = wr.athena._utils._QUERY_WAIT_POLLING_DELAY
        wr.config.reset("athena_query_wait_polling_delay")

    with patch("awswrangler.athena._utils.wait_query", wraps=wr.athena.wait_query) as mock_wait_query:
        wr.athena.read_sql_query("SELECT 1 as col0", database=glue_database)

        mock_wait_query.assert_called_once()

        assert mock_wait_query.call_args[1]["athena_query_wait_polling_delay"] == polling_delay


def test_athena_wait_delay_config_override(wr: ModuleType, glue_database: str) -> None:
    wr.config.athena_query_wait_polling_delay = 0.1
    polling_delay_argument = 0.15

    with patch("awswrangler.athena._utils.wait_query", wraps=wr.athena.wait_query) as mock_wait_query:
        wr.athena.read_sql_query(
            "SELECT 1 as col0", database=glue_database, athena_query_wait_polling_delay=polling_delay_argument
        )

        mock_wait_query.assert_called_once()

        assert mock_wait_query.call_args[1]["athena_query_wait_polling_delay"] == polling_delay_argument


@pytest.mark.parametrize("suppress_warnings", [False, True])
def test_load_from_env_variable(wr: ModuleType, suppress_warnings: bool) -> None:
    env_variable_value = "1" if suppress_warnings else ""

    with patch.dict(os.environ, {"WR_SUPPRESS_WARNINGS": env_variable_value}):
        wr.config.reset()

        assert wr.config.suppress_warnings == suppress_warnings
