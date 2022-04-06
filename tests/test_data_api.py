import boto3
import pandas as pd
import pytest

import awswrangler as wr

from ._utils import get_time_str_with_random_suffix


@pytest.fixture
def redshift_connector(databases_parameters):
    cluster_id = databases_parameters["redshift"]["identifier"]
    database = databases_parameters["redshift"]["database"]
    secret_arn = databases_parameters["redshift"]["secret_arn"]
    conn = wr.data_api.redshift.connect(cluster_id, database, secret_arn=secret_arn, boto3_session=None)
    return conn


def create_rds_connector(rds_type, parameters):
    cluster_id = parameters[rds_type]["arn"]
    database = parameters[rds_type]["database"]
    secret_arn = parameters[rds_type]["secret_arn"]
    conn = wr.data_api.rds.connect(cluster_id, database, secret_arn=secret_arn, boto3_session=boto3.DEFAULT_SESSION)
    return conn


@pytest.fixture
def mysql_serverless_connector(databases_parameters):
    return create_rds_connector("mysql_serverless", databases_parameters)


@pytest.fixture(scope="function")
def mysql_serverless_table(mysql_serverless_connector):
    name = f"tbl_{get_time_str_with_random_suffix()}"
    print(f"Table name: {name}")
    yield name
    wr.data_api.rds.read_sql_query(f"DROP TABLE IF EXISTS test.{name}", con=mysql_serverless_connector)


def test_data_api_redshift_columnless_query(redshift_connector):
    dataframe = wr.data_api.redshift.read_sql_query("SELECT 1", con=redshift_connector)
    unknown_column_indicator = "?column?"
    expected_dataframe = pd.DataFrame([[1]], columns=[unknown_column_indicator])
    pd.testing.assert_frame_equal(dataframe, expected_dataframe)


def test_data_api_redshift_basic_select(redshift_connector, redshift_table):
    wr.data_api.redshift.read_sql_query(
        f"CREATE TABLE public.{redshift_table} (id INT, name VARCHAR)", con=redshift_connector
    )
    wr.data_api.redshift.read_sql_query(
        f"INSERT INTO public.{redshift_table} VALUES (42, 'test')", con=redshift_connector
    )
    dataframe = wr.data_api.redshift.read_sql_query(f"SELECT * FROM  public.{redshift_table}", con=redshift_connector)
    expected_dataframe = pd.DataFrame([[42, "test"]], columns=["id", "name"])
    pd.testing.assert_frame_equal(dataframe, expected_dataframe)


def test_data_api_redshift_empty_results_select(redshift_connector, redshift_table):
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
    pd.testing.assert_frame_equal(dataframe, expected_dataframe)


def test_data_api_redshift_column_subset_select(redshift_connector, redshift_table):
    wr.data_api.redshift.read_sql_query(
        f"CREATE TABLE public.{redshift_table} (id INT, name VARCHAR)", con=redshift_connector
    )
    wr.data_api.redshift.read_sql_query(
        f"INSERT INTO public.{redshift_table} VALUES (42, 'test')", con=redshift_connector
    )
    dataframe = wr.data_api.redshift.read_sql_query(f"SELECT name FROM public.{redshift_table}", con=redshift_connector)
    expected_dataframe = pd.DataFrame([["test"]], columns=["name"])
    pd.testing.assert_frame_equal(dataframe, expected_dataframe)


def test_data_api_mysql_columnless_query(mysql_serverless_connector):
    dataframe = wr.data_api.rds.read_sql_query("SELECT 1", con=mysql_serverless_connector)
    expected_dataframe = pd.DataFrame([[1]], columns=["1"])
    pd.testing.assert_frame_equal(dataframe, expected_dataframe)


def test_data_api_mysql_basic_select(mysql_serverless_connector, mysql_serverless_table):
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
    pd.testing.assert_frame_equal(dataframe, expected_dataframe)


def test_data_api_mysql_empty_results_select(mysql_serverless_connector, mysql_serverless_table):
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
    pd.testing.assert_frame_equal(dataframe, expected_dataframe)


def test_data_api_mysql_column_subset_select(mysql_serverless_connector, mysql_serverless_table):
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
    pd.testing.assert_frame_equal(dataframe, expected_dataframe)
