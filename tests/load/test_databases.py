import time
from datetime import datetime
from typing import Dict

import modin.pandas as pd
import pytest
from redshift_connector import Connection

import awswrangler as wr

from .._utils import ExecutionTimer


@pytest.mark.parametrize("benchmark_time", [60])
def test_timestream_write(
    benchmark_time: int, timestream_database_and_table: str, df_timestream: pd.DataFrame, request
) -> None:
    name = timestream_database_and_table
    df_timestream["time"] = datetime.now()
    df_timestream["index"] = range(0, len(df_timestream))
    df_cpu = df_timestream[df_timestream.measure_kind == "cpu_utilization"]
    df_memory = df_timestream[df_timestream.measure_kind == "memory_utilization"]
    kwargs = {
        "database": name,
        "table": name,
        "time_col": "time",
        "measure_col": "measure",
        "dimensions_cols": ["index", "region", "az", "hostname"],
    }

    with ExecutionTimer(request) as timer:
        for df in [df_cpu, df_memory]:
            rejected_records = wr.timestream.write(df=df, **kwargs)
            assert len(rejected_records) == 0
    assert timer.elapsed_time < benchmark_time

    df = wr.timestream.query(f'SELECT COUNT(*) AS counter FROM "{name}"."{name}"')
    assert df["counter"].iloc[0] == 126_000


@pytest.mark.parametrize("benchmark_time", [90])
def test_timestream_batch_load(
    benchmark_time: int, timestream_database_and_table: str, df_timestream: pd.DataFrame, path: str, path2: str, request
) -> None:
    name = timestream_database_and_table
    df_timestream["time"] = round(time.time()) * 1_000
    df_timestream["index"] = range(0, len(df_timestream))
    df_cpu = df_timestream[df_timestream.measure_kind == "cpu_utilization"]
    df_memory = df_timestream[df_timestream.measure_kind == "memory_utilization"]
    error_bucket, error_prefix = wr._utils.parse_path(path2)
    kwargs = {
        "path": path,
        "database": name,
        "table": name,
        "time_col": "time",
        "dimensions_cols": ["index", "region", "az", "hostname"],
        "measure_cols": ["measure"],
        "measure_name_col": "measure_kind",
        "report_s3_configuration": {"BucketName": error_bucket, "ObjectKeyPrefix": error_prefix},
    }

    with ExecutionTimer(request) as timer:
        for df in [df_cpu, df_memory]:
            response = wr.timestream.batch_load(df=df, **kwargs)
            assert response["BatchLoadTaskDescription"]["ProgressReport"]["RecordIngestionFailures"] == 0
    assert timer.elapsed_time < benchmark_time

    df = wr.timestream.query(f'SELECT COUNT(*) AS counter FROM "{name}"."{name}"')
    assert df["counter"].iloc[0] == 126_000


@pytest.mark.parametrize("benchmark_time_copy", [150])
@pytest.mark.parametrize("benchmark_time_unload", [150])
def test_redshift_copy_unload(
    benchmark_time_copy: int,
    benchmark_time_unload: int,
    path: str,
    redshift_table: str,
    redshift_con: Connection,
    databases_parameters: Dict[str, str],
    request,
) -> None:
    df = wr.s3.read_parquet(path=[f"s3://ursa-labs-taxi-data/2018/{i}/data.parquet" for i in range(10, 13)])

    with ExecutionTimer(request, "redshift_copy") as timer:
        wr.redshift.copy(
            df=df,
            path=path,
            con=redshift_con,
            schema="public",
            table=redshift_table,
            mode="overwrite",
            iam_role=databases_parameters["redshift"]["role"],
        )
    assert timer.elapsed_time < benchmark_time_copy

    with ExecutionTimer(request, "redshift_unload") as timer:
        df2 = wr.redshift.unload(
            sql=f"SELECT * FROM public.{redshift_table}",
            con=redshift_con,
            iam_role=databases_parameters["redshift"]["role"],
            path=path,
            keep_files=False,
        )
    assert timer.elapsed_time < benchmark_time_unload

    assert df.shape == df2.shape


@pytest.mark.parametrize("benchmark_time", [40])
def test_athena_unload(benchmark_time: int, path: str, glue_table: str, glue_database: str, request) -> None:
    df = wr.s3.read_parquet(path="s3://ursa-labs-taxi-data/2017/", dataset=True)

    wr.s3.to_parquet(
        df,
        path,
        dataset=True,
        table=glue_table,
        database=glue_database,
        partition_cols=["passenger_count", "payment_type"],
    )

    with ExecutionTimer(request) as timer:
        df_out = wr.athena.read_sql_query(
            sql=f"SELECT * FROM {glue_table}",
            database=glue_database,
            ctas_approach=False,
            unload_approach=True,
            s3_output=f"{path}unload/",
            unload_parameters={"file_format": "PARQUET"},
        )
    assert timer.elapsed_time < benchmark_time

    assert df.shape == df_out.shape


@pytest.mark.parametrize("benchmark_time", [80])
def test_lakeformation_read(benchmark_time: int, path: str, glue_table: str, glue_database: str, request) -> None:
    df = wr.s3.read_parquet(path="s3://ursa-labs-taxi-data/2017/", dataset=True)

    wr.s3.to_parquet(
        df,
        path,
        index=False,
        dataset=True,
        table=glue_table,
        database=glue_database,
        partition_cols=["passenger_count", "payment_type"],
        glue_table_settings={
            "table_type": "GOVERNED",
        },
    )

    with ExecutionTimer(request) as timer:
        df_out = wr.lakeformation.read_sql_table(
            table=glue_table,
            database=glue_database,
        )
    assert timer.elapsed_time < benchmark_time

    assert df.shape == df_out.shape
