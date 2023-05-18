from typing import Any, Dict, Iterator

import boto3
import pytest

import awswrangler as wr
import awswrangler.pandas as pd
from awswrangler.data_api.rds import RdsDataApi
from awswrangler.data_api.redshift import RedshiftDataApi

from .._utils import assert_pandas_equals, get_time_str_with_random_suffix

pytestmark = pytest.mark.distributed


@pytest.fixture
def redshift_connector(databases_parameters: Dict[str, Any]) -> "RedshiftDataApi":
    cluster_id = databases_parameters["redshift"]["identifier"]
    database = databases_parameters["redshift"]["database"]
    secret_arn = databases_parameters["redshift"]["secret_arn"]
    return wr.data_api.redshift.connect(
        cluster_id=cluster_id, database=database, secret_arn=secret_arn, boto3_session=None
    )


def create_rds_connector(rds_type: str, parameters: Dict[str, Any]) -> "RdsDataApi":
    cluster_id = parameters[rds_type]["arn"]
    database = parameters[rds_type]["database"]
    secret_arn = parameters[rds_type]["secret_arn"]
    return wr.data_api.rds.connect(cluster_id, database, secret_arn=secret_arn, boto3_session=boto3.DEFAULT_SESSION)


@pytest.fixture
def mysql_serverless_connector(databases_parameters: Dict[str, Any]) -> "RdsDataApi":
    return create_rds_connector("mysql_serverless", databases_parameters)


def test_connect_redshift_serverless_iam_role(databases_parameters: Dict[str, Any]) -> None:
    workgroup_name = databases_parameters["redshift_serverless"]["workgroup"]
    database = databases_parameters["redshift_serverless"]["database"]
    con = wr.data_api.redshift.connect(workgroup_name=workgroup_name, database=database, boto3_session=None)
    df = wr.data_api.redshift.read_sql_query("SELECT 1", con=con)
    assert df.shape == (1, 1)


def test_connect_redshift_serverless_secrets_manager(databases_parameters: Dict[str, Any]) -> None:
    workgroup_name = databases_parameters["redshift_serverless"]["workgroup"]
    database = databases_parameters["redshift_serverless"]["database"]
    secret_arn = databases_parameters["redshift_serverless"]["secret_arn"]
    con = wr.data_api.redshift.connect(
        workgroup_name=workgroup_name, database=database, secret_arn=secret_arn, boto3_session=None
    )
    df = wr.data_api.redshift.read_sql_query("SELECT 1", con=con)
    assert df.shape == (1, 1)


@pytest.fixture(scope="function")
def mysql_serverless_table(mysql_serverless_connector: "RdsDataApi") -> Iterator[str]:
    name = f"tbl_{get_time_str_with_random_suffix()}"
    print(f"Table name: {name}")
    try:
        yield name
    finally:
        wr.data_api.rds.read_sql_query(f"DROP TABLE IF EXISTS test.{name}", con=mysql_serverless_connector)


def test_data_api_redshift_columnless_query(redshift_connector: "RedshiftDataApi") -> None:
    dataframe = wr.data_api.redshift.read_sql_query("SELECT 1", con=redshift_connector)
    unknown_column_indicator = "?column?"
    expected_dataframe = pd.DataFrame([[1]], columns=[unknown_column_indicator])
    assert_pandas_equals(dataframe, expected_dataframe)


def test_data_api_redshift_basic_select(redshift_connector: "RedshiftDataApi", redshift_table: str) -> None:
    wr.data_api.redshift.read_sql_query(
        f"CREATE TABLE public.{redshift_table} (id INT, name VARCHAR)", con=redshift_connector
    )
    wr.data_api.redshift.read_sql_query(
        f"INSERT INTO public.{redshift_table} VALUES (42, 'test')", con=redshift_connector
    )
    dataframe = wr.data_api.redshift.read_sql_query(f"SELECT * FROM  public.{redshift_table}", con=redshift_connector)
    expected_dataframe = pd.DataFrame([[42, "test"]], columns=["id", "name"])
    assert_pandas_equals(dataframe, expected_dataframe)


def test_data_api_redshift_empty_results_select(redshift_connector: "RedshiftDataApi", redshift_table: str) -> None:
    wr.data_api.redshift.read_sql_query(
        f"CREATE TABLE public.{redshift_table} (id INT, name VARCHAR)", con=redshift_connector
    )
    wr.data_api.redshift.read_sql_query(
        f"INSERT INTO public.{redshift_table} VALUES (42, 'test')", con=redshift_connector
    )
    dataframe = wr.data_api.redshift.read_sql_query(
        f"SELECT * FROM  public.{redshift_table} where id = 50", con=redshift_connector
    )
    expected_dataframe = pd.DataFrame([], columns=["id", "name"])
    assert_pandas_equals(dataframe, expected_dataframe)


def test_data_api_redshift_column_subset_select(redshift_connector: "RedshiftDataApi", redshift_table: str) -> None:
    wr.data_api.redshift.read_sql_query(
        f"CREATE TABLE public.{redshift_table} (id INT, name VARCHAR)", con=redshift_connector
    )
    wr.data_api.redshift.read_sql_query(
        f"INSERT INTO public.{redshift_table} VALUES (42, 'test')", con=redshift_connector
    )
    dataframe = wr.data_api.redshift.read_sql_query(f"SELECT name FROM public.{redshift_table}", con=redshift_connector)
    expected_dataframe = pd.DataFrame([["test"]], columns=["name"])
    assert_pandas_equals(dataframe, expected_dataframe)


def test_data_api_mysql_columnless_query(mysql_serverless_connector: "RdsDataApi") -> None:
    dataframe = wr.data_api.rds.read_sql_query("SELECT 1", con=mysql_serverless_connector)
    expected_dataframe = pd.DataFrame([[1]], columns=["1"])
    assert_pandas_equals(dataframe, expected_dataframe)


def test_data_api_mysql_basic_select(mysql_serverless_connector: "RdsDataApi", mysql_serverless_table: str) -> None:
    wr.data_api.rds.read_sql_query(
        f"CREATE TABLE test.{mysql_serverless_table} (id INT, name VARCHAR(128), missing VARCHAR(256))",
        con=mysql_serverless_connector,
    )
    wr.data_api.rds.read_sql_query(
        f"INSERT INTO test.{mysql_serverless_table} (id, name) VALUES (42, 'test')", con=mysql_serverless_connector
    )
    dataframe = wr.data_api.rds.read_sql_query(
        f"SELECT * FROM test.{mysql_serverless_table}", con=mysql_serverless_connector
    )
    expected_dataframe = pd.DataFrame([[42, "test", None]], columns=["id", "name", "missing"])
    assert_pandas_equals(dataframe, expected_dataframe)


def test_data_api_mysql_empty_results_select(
    mysql_serverless_connector: "RdsDataApi", mysql_serverless_table: str
) -> None:
    wr.data_api.rds.read_sql_query(
        f"CREATE TABLE test.{mysql_serverless_table} (id INT, name VARCHAR(128))", con=mysql_serverless_connector
    )
    wr.data_api.rds.read_sql_query(
        f"INSERT INTO test.{mysql_serverless_table} VALUES (42, 'test')", con=mysql_serverless_connector
    )
    dataframe = wr.data_api.rds.read_sql_query(
        f"SELECT * FROM  test.{mysql_serverless_table} where id = 50", con=mysql_serverless_connector
    )
    expected_dataframe = pd.DataFrame([], columns=["id", "name"])
    assert_pandas_equals(dataframe, expected_dataframe)


def test_data_api_mysql_column_subset_select(
    mysql_serverless_connector: "RdsDataApi", mysql_serverless_table: str
) -> None:
    wr.data_api.rds.read_sql_query(
        f"CREATE TABLE test.{mysql_serverless_table} (id INT, name VARCHAR(128))", con=mysql_serverless_connector
    )
    wr.data_api.rds.read_sql_query(
        f"INSERT INTO test.{mysql_serverless_table} VALUES (42, 'test')", con=mysql_serverless_connector
    )
    dataframe = wr.data_api.rds.read_sql_query(
        f"SELECT name FROM test.{mysql_serverless_table}", con=mysql_serverless_connector
    )
    expected_dataframe = pd.DataFrame([["test"]], columns=["name"])
    assert_pandas_equals(dataframe, expected_dataframe)


def test_data_api_exception(mysql_serverless_connector: "RdsDataApi", mysql_serverless_table: str) -> None:
    with pytest.raises(boto3.client("rds-data").exceptions.BadRequestException):
        wr.data_api.rds.read_sql_query("CUPCAKE", con=mysql_serverless_connector)
