import pandas as pd
import pytest

import awswrangler as wr


@pytest.fixture
def redshift_connector(databases_parameters):
    cluster_id = databases_parameters["redshift"]["identifier"]
    database = databases_parameters["redshift"]["database"]
    secret_arn = databases_parameters["redshift"]["secret_arn"]
    conn = wr.data_api.redshift.connect(cluster_id, database, secret_arn=secret_arn)
    return conn


def create_rds_connector(rds_type, parameters):
    cluster_id = parameters[rds_type]["arn"]
    database = parameters[rds_type]["database"]
    secret_arn = parameters[rds_type]["secret_arn"]
    conn = wr.data_api.rds.connect(cluster_id, database, secret_arn=secret_arn)
    return conn


@pytest.fixture
def mysql_serverless_connector(databases_parameters):
    return create_rds_connector("mysql_serverless", databases_parameters)


def test_data_api_redshift_basic_query(redshift_connector):
    dataframe = wr.data_api.redshift.read_sql_query("SELECT 1", con=redshift_connector)
    unknown_column_indicator = "?column?"
    expected_dataframe = pd.DataFrame([[1]], columns=[unknown_column_indicator])
    pd.testing.assert_frame_equal(dataframe, expected_dataframe)


def test_data_api_msql_basic_query(mysql_serverless_connector):
    dataframe = wr.data_api.rds.read_sql_query("SELECT 1", con=mysql_serverless_connector)
    expected_dataframe = pd.DataFrame([[1]])
    pd.testing.assert_frame_equal(dataframe, expected_dataframe)
