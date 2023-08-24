from typing import Any, Dict, Iterator

import boto3
import pytest

import awswrangler as wr
import awswrangler.pandas as pd
from awswrangler.data_api.rds import RdsDataApi
from awswrangler.data_api.redshift import RedshiftDataApi

from .._utils import assert_pandas_equals, get_df, get_time_str_with_random_suffix, is_ray_modin

pytestmark = pytest.mark.distributed


@pytest.fixture
def redshift_connector(databases_parameters: Dict[str, Any]) -> Iterator["RedshiftDataApi"]:
    cluster_id = databases_parameters["redshift"]["identifier"]
    database = databases_parameters["redshift"]["database"]
    secret_arn = databases_parameters["redshift"]["secret_arn"]
    con = wr.data_api.redshift.connect(
        cluster_id=cluster_id, database=database, secret_arn=secret_arn, boto3_session=None
    )
    with con:
        yield con


def create_rds_connector(rds_type: str, parameters: Dict[str, Any]) -> "RdsDataApi":
    cluster_id = parameters[rds_type]["arn"]
    database = parameters[rds_type]["database"]
    secret_arn = parameters[rds_type]["secret_arn"]
    return wr.data_api.rds.connect(cluster_id, database, secret_arn=secret_arn, boto3_session=boto3.DEFAULT_SESSION)


@pytest.fixture
def mysql_serverless_connector(databases_parameters: Dict[str, Any]) -> "RdsDataApi":
    con = create_rds_connector("mysql_serverless", databases_parameters)
    with con:
        yield con


@pytest.fixture
def postgresql_serverless_connector(databases_parameters: Dict[str, Any]) -> "RdsDataApi":
    con = create_rds_connector("postgresql_serverless", databases_parameters)
    with con:
        yield con


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
        mysql_serverless_connector.execute(f"DROP TABLE IF EXISTS test.{name}")


@pytest.fixture(scope="function")
def postgresql_serverless_table(postgresql_serverless_connector: "RdsDataApi") -> Iterator[str]:
    name = f"tbl_{get_time_str_with_random_suffix()}"
    print(f"Table name: {name}")
    try:
        yield name
    finally:
        postgresql_serverless_connector.execute(f"DROP TABLE IF EXISTS test.{name}")


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


@pytest.mark.parametrize("use_column_names", [False, True])
def test_data_api_mysql_basic_select(
    mysql_serverless_connector: "RdsDataApi", mysql_serverless_table: str, use_column_names: bool
) -> None:
    database = "test"
    frame = pd.DataFrame([[42, "test", None], [23, "foo", "bar"]], columns=["id", "name", "missing"])

    wr.data_api.rds.to_sql(
        df=frame,
        con=mysql_serverless_connector,
        table=mysql_serverless_table,
        database=database,
        use_column_names=use_column_names,
    )

    out_frame = wr.data_api.rds.read_sql_query(
        f"SELECT * FROM test.{mysql_serverless_table}", con=mysql_serverless_connector
    )
    assert_pandas_equals(out_frame, frame)


def test_data_api_mysql_empty_results_select(
    mysql_serverless_connector: "RdsDataApi", mysql_serverless_table: str
) -> None:
    database = "test"
    frame = pd.DataFrame([[42, "test"]], columns=["id", "name"])

    wr.data_api.rds.to_sql(
        df=frame,
        con=mysql_serverless_connector,
        table=mysql_serverless_table,
        database=database,
    )

    out_frame = wr.data_api.rds.read_sql_query(
        f"SELECT * FROM  test.{mysql_serverless_table} where id = 50", con=mysql_serverless_connector
    )
    expected_dataframe = pd.DataFrame([], columns=["id", "name"])
    assert_pandas_equals(out_frame, expected_dataframe)


def test_data_api_mysql_column_subset_select(
    mysql_serverless_connector: "RdsDataApi", mysql_serverless_table: str
) -> None:
    database = "test"
    frame = pd.DataFrame([[42, "test"]], columns=["id", "name"])

    wr.data_api.rds.to_sql(
        df=frame,
        con=mysql_serverless_connector,
        table=mysql_serverless_table,
        database=database,
    )

    out_frame = wr.data_api.rds.read_sql_query(
        f"SELECT name FROM test.{mysql_serverless_table}", con=mysql_serverless_connector
    )
    expected_dataframe = pd.DataFrame([["test"]], columns=["name"])
    assert_pandas_equals(out_frame, expected_dataframe)


@pytest.mark.parametrize("mode", ["overwrite", "append"])
def test_data_api_mysql_to_sql_mode(
    mysql_serverless_connector: "RdsDataApi", mysql_serverless_table: str, mode: str
) -> None:
    database = "test"
    frame = get_df()
    wr.data_api.rds.to_sql(
        df=frame,
        con=mysql_serverless_connector,
        table=mysql_serverless_table,
        database=database,
    )

    frame2 = get_df()
    wr.data_api.rds.to_sql(
        df=frame2,
        con=mysql_serverless_connector,
        table=mysql_serverless_table,
        database=database,
        mode=mode,
    )

    out_frame = wr.data_api.rds.read_sql_query(
        f"SELECT * FROM test.{mysql_serverless_table}", con=mysql_serverless_connector
    )

    if mode == "overwrite":
        expected_frame = frame2
    else:
        expected_frame = pd.concat([frame, frame2], axis=0).reset_index(drop=True)

    # Cast types
    out_frame = out_frame.astype(expected_frame.dtypes)
    # Modin upcasts to float64 now
    if is_ray_modin:
        out_frame["float"] = out_frame["float"].astype("float32")

    assert_pandas_equals(out_frame, expected_frame)


def test_data_api_exception(mysql_serverless_connector: "RdsDataApi", mysql_serverless_table: str) -> None:
    with pytest.raises(boto3.client("rds-data").exceptions.BadRequestException):
        wr.data_api.rds.read_sql_query("CUPCAKE", con=mysql_serverless_connector)


def test_data_api_mysql_ansi(mysql_serverless_connector: "RdsDataApi", mysql_serverless_table: str) -> None:
    database = "test"
    frame = pd.DataFrame([[42, "test"]], columns=["id", "name"])

    mysql_serverless_connector.execute("SET SESSION sql_mode='ANSI_QUOTES';")

    wr.data_api.rds.to_sql(
        df=frame,
        con=mysql_serverless_connector,
        table=mysql_serverless_table,
        database=database,
        sql_mode="ansi",
    )

    out_frame = wr.data_api.rds.read_sql_query(
        f"SELECT name FROM {mysql_serverless_table} WHERE id = 42", con=mysql_serverless_connector
    )
    expected_dataframe = pd.DataFrame([["test"]], columns=["name"])
    assert_pandas_equals(out_frame, expected_dataframe)


def test_data_api_postgresql(postgresql_serverless_connector: "RdsDataApi", postgresql_serverless_table: str) -> None:
    database = "test"
    frame = pd.DataFrame([[42, "test"]], columns=["id", "name"])

    wr.data_api.rds.to_sql(
        df=frame,
        con=postgresql_serverless_connector,
        table=postgresql_serverless_table,
        database=database,
        sql_mode="ansi",
    )

    out_frame = wr.data_api.rds.read_sql_query(
        f"SELECT name FROM {postgresql_serverless_table} WHERE id = 42", con=postgresql_serverless_connector
    )
    expected_dataframe = pd.DataFrame([["test"]], columns=["name"])
    assert_pandas_equals(out_frame, expected_dataframe)
