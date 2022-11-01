from datetime import datetime
from typing import Dict

import pytest
import ray
from pyarrow import csv
from redshift_connector import Connection

import awswrangler as wr

from .._utils import ExecutionTimer


@pytest.mark.parametrize("benchmark_time", [180])
def test_timestream_write(benchmark_time: int, timestream_database_and_table: str) -> None:
    name = timestream_database_and_table
    df = (
        ray.data.read_csv(
            "https://raw.githubusercontent.com/awslabs/amazon-timestream-tools/mainline/sample_apps/data/sample.csv",
            **{
                "read_options": csv.ReadOptions(
                    column_names=[
                        "ignore0",
                        "region",
                        "ignore1",
                        "az",
                        "ignore2",
                        "hostname",
                        "measure_kind",
                        "measure",
                        "ignore3",
                        "ignore4",
                        "ignore5",
                    ]
                )
            },
        )
        .to_modin()
        .loc[:, ["region", "az", "hostname", "measure_kind", "measure"]]
    )

    df["time"] = datetime.now()
    df.reset_index(inplace=True, drop=False)
    df_cpu = df[df.measure_kind == "cpu_utilization"]
    df_memory = df[df.measure_kind == "memory_utilization"]

    with ExecutionTimer("elapsed time of wr.timestream.write()") as timer:
        rejected_records = wr.timestream.write(
            df=df_cpu,
            database=name,
            table=name,
            time_col="time",
            measure_col="measure",
            dimensions_cols=["index", "region", "az", "hostname"],
        )
        assert len(rejected_records) == 0
        rejected_records = wr.timestream.write(
            df=df_memory,
            database=name,
            table=name,
            time_col="time",
            measure_col="measure",
            dimensions_cols=["index", "region", "az", "hostname"],
        )
        assert len(rejected_records) == 0
    assert timer.elapsed_time < benchmark_time

    df = wr.timestream.query(f'SELECT COUNT(*) AS counter FROM "{name}"."{name}"')
    assert df["counter"].iloc[0] == 126_000


@pytest.mark.parametrize("benchmark_time_copy", [240])
@pytest.mark.parametrize("benchmark_time_unload", [240])
def test_redshift_copy_unload(
    benchmark_time_copy: int,
    benchmark_time_unload: int,
    path: str,
    redshift_table: str,
    redshift_con: Connection,
    databases_parameters: Dict[str, str],
) -> None:
    df = wr.s3.read_parquet(path="s3://ursa-labs-taxi-data/2018/")

    with ExecutionTimer("elapsed time of wr.redshift.copy()") as timer:
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

    with ExecutionTimer("elapsed time of wr.redshift.unload()") as timer:
        df2 = wr.redshift.unload(
            sql=f"SELECT * FROM public.{redshift_table}",
            con=redshift_con,
            iam_role=databases_parameters["redshift"]["role"],
            path=path,
            keep_files=False,
        )
    assert timer.elapsed_time < benchmark_time_unload

    assert df.shape == df2.shape


@pytest.mark.parametrize("benchmark_time", [120])
def test_athena_unload(benchmark_time: int, path: str, glue_table: str, glue_database: str) -> None:
    df = wr.s3.read_parquet(path="s3://amazon-reviews-pds/parquet/product_category=Toys/", dataset=True)

    wr.s3.to_parquet(
        df,
        path,
        dataset=True,
        table=glue_table,
        database=glue_database,
        partition_cols=["year", "marketplace"],
    )

    with ExecutionTimer("elapsed time of wr.athena.read_sql_query()") as timer:
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


@pytest.mark.parametrize("benchmark_time", [180])
def test_lakeformation_read(benchmark_time: int, path: str, glue_table: str, glue_database: str) -> None:
    df = wr.s3.read_parquet(path="s3://amazon-reviews-pds/parquet/product_category=Home/", dataset=True)

    wr.s3.to_parquet(
        df,
        path,
        index=False,
        dataset=True,
        table_type="GOVERNED",
        table=glue_table,
        database=glue_database,
        partition_cols=["year", "marketplace"],
    )

    with ExecutionTimer("elapsed time of wr.lakeformation.read_sql_table()") as timer:
        df_out = wr.lakeformation.read_sql_table(
            table=glue_table,
            database=glue_database,
        )
    assert timer.elapsed_time < benchmark_time

    assert df.shape == df_out.shape
