from __future__ import annotations

import json
import logging
import random
import string
from decimal import Decimal
from typing import Any, Iterator

import boto3
import numpy as np
import pyarrow as pa
import pytest
import redshift_connector
from redshift_connector.error import ProgrammingError

import awswrangler as wr
import awswrangler.pandas as pd
from awswrangler import _utils

from .._utils import (
    assert_pandas_equals,
    dt,
    ensure_data_types,
    ensure_data_types_category,
    get_df,
    get_df_category,
    is_ray_modin,
    pandas_equals,
    to_pandas,
    ts,
)

logging.getLogger("awswrangler").setLevel(logging.DEBUG)

pytestmark = pytest.mark.distributed


@pytest.fixture(scope="function")
def redshift_con() -> Iterator[redshift_connector.Connection]:
    with wr.redshift.connect("aws-sdk-pandas-redshift") as con:
        yield con


def test_connection() -> None:
    with wr.redshift.connect("aws-sdk-pandas-redshift", timeout=10, force_lowercase=False):
        pass


def test_read_sql_query_simple(databases_parameters: dict[str, Any]) -> None:
    con = redshift_connector.connect(
        host=databases_parameters["redshift"]["host"],
        port=int(databases_parameters["redshift"]["port"]),
        database=databases_parameters["redshift"]["database"],
        user=databases_parameters["user"],
        password=databases_parameters["password"],
    )
    with con:
        df = wr.redshift.read_sql_query("SELECT 1", con=con)
    assert df.shape == (1, 1)


@pytest.mark.parametrize("overwrite_method", [None, "drop", "cascade", "truncate", "delete"])
def test_to_sql_simple(redshift_table: str, redshift_con: redshift_connector.Connection, overwrite_method: str) -> None:
    df = pd.DataFrame({"c0": [1, 2, 3], "c1": ["foo", "boo", "bar"]})
    wr.redshift.to_sql(df, redshift_con, redshift_table, "public", "overwrite", overwrite_method, True)


def test_to_sql_with_hyphenated_primary_key(
    redshift_table: str,
    redshift_con: redshift_connector.Connection,
) -> None:
    schema = "public"
    df = pd.DataFrame({"id-col": [1, 2, 3], "other-col": ["foo", "boo", "bar"]})
    df["id-col"] = df["id-col"].astype("Int64")
    df["other-col"] = df["other-col"].astype("string")
    wr.redshift.to_sql(
        df=df, con=redshift_con, table=redshift_table, schema=schema, mode="overwrite", primary_keys=["id-col"]
    )
    df_out = wr.redshift.read_sql_table(table=redshift_table, con=redshift_con, schema=schema)
    assert_pandas_equals(df, df_out)


def test_empty_table(redshift_table: str, redshift_con: redshift_connector.Connection) -> None:
    with redshift_con.cursor() as cursor:
        cursor.execute(f"CREATE TABLE public.{redshift_table}(c0 integer not null, c1 integer, primary key(c0));")
    df = wr.redshift.read_sql_table(table=redshift_table, con=redshift_con, schema="public")
    assert df.columns.values.tolist() == ["c0", "c1"]


def test_sql_types(redshift_table: str, redshift_con: redshift_connector.Connection) -> None:
    table = redshift_table
    df = get_df()
    df.drop(["binary"], axis=1, inplace=True)
    wr.redshift.to_sql(
        df=df,
        con=redshift_con,
        table=table,
        schema="public",
        mode="overwrite",
        index=True,
        dtype={"iint32": "INTEGER"},
    )
    df = wr.redshift.read_sql_query(f"SELECT * FROM public.{table}", redshift_con)
    ensure_data_types(df, has_list=False)
    dfs = wr.redshift.read_sql_query(
        sql=f"SELECT * FROM public.{table}",
        con=redshift_con,
        chunksize=1,
        dtype={
            "iint8": pa.int8(),
            "iint16": pa.int16(),
            "iint32": pa.int32(),
            "iint64": pa.int64(),
            "float": pa.float32(),
            "ddouble": pa.float64(),
            "decimal": pa.decimal128(3, 2),
            "string_object": pa.string(),
            "string": pa.string(),
            "date": pa.date32(),
            "timestamp": pa.timestamp(unit="ns"),
            "binary": pa.binary(),
            "category": pa.float64(),
        },
    )
    for df in dfs:
        ensure_data_types(df, has_list=False)


def test_connection_temp(databases_parameters: dict[str, Any]) -> None:
    con: redshift_connector.Connection = wr.redshift.connect_temp(
        cluster_identifier=databases_parameters["redshift"]["identifier"], user="test"
    )
    with con:
        with con.cursor() as cursor:
            cursor.execute("SELECT 1")
            assert cursor.fetchall()[0][0] == 1


def test_connection_temp2(databases_parameters: dict[str, Any]) -> None:
    con: redshift_connector.Connection = wr.redshift.connect_temp(
        cluster_identifier=databases_parameters["redshift"]["identifier"], user="john_doe", duration=900, db_groups=[]
    )
    with con:
        with con.cursor() as cursor:
            cursor.execute("SELECT 1")
            assert cursor.fetchall()[0][0] == 1


def test_copy_unload(
    path: str, redshift_table: str, redshift_con: redshift_connector.Connection, databases_parameters: dict[str, Any]
) -> None:
    df = get_df().drop(["binary"], axis=1, inplace=False)
    wr.redshift.copy(
        df=df,
        path=path,
        con=redshift_con,
        schema="public",
        table=redshift_table,
        mode="overwrite",
        iam_role=databases_parameters["redshift"]["role"],
    )
    df2 = wr.redshift.unload(
        sql=f"SELECT * FROM public.{redshift_table}",
        con=redshift_con,
        iam_role=databases_parameters["redshift"]["role"],
        path=path,
        keep_files=False,
    )
    assert len(df2.index) == 3
    ensure_data_types(df=df2, has_list=False)
    wr.redshift.copy(
        df=df,
        path=path,
        con=redshift_con,
        schema="public",
        table=redshift_table,
        mode="append",
        iam_role=databases_parameters["redshift"]["role"],
    )
    df2 = wr.redshift.unload(
        sql=f"SELECT * FROM public.{redshift_table}",
        con=redshift_con,
        iam_role=databases_parameters["redshift"]["role"],
        path=path,
        keep_files=False,
    )
    assert len(df2.index) == 6
    ensure_data_types(df=df2, has_list=False)
    dfs = wr.redshift.unload(
        sql=f"SELECT * FROM public.{redshift_table}",
        con=redshift_con,
        iam_role=databases_parameters["redshift"]["role"],
        path=path,
        keep_files=False,
        chunked=True,
    )
    for chunk in dfs:
        ensure_data_types(df=chunk, has_list=False)


def generic_test_copy_upsert(
    path: str,
    redshift_table: str,
    redshift_con: redshift_connector.Connection,
    databases_parameters: dict[str, Any],
) -> None:
    df = pd.DataFrame({"id": list(range(1_000)), "val": list(["foo" if i % 2 == 0 else "boo" for i in range(1_000)])})
    df3 = pd.DataFrame(
        {"id": list(range(1_000, 1_500)), "val": list(["foo" if i % 2 == 0 else "boo" for i in range(500)])}
    )

    # CREATE
    path = f"{path}upsert/test_redshift_copy_upsert/"
    wr.redshift.copy(
        df=df,
        path=path,
        con=redshift_con,
        schema="public",
        table=redshift_table,
        mode="overwrite",
        index=False,
        primary_keys=["id"],
        iam_role=databases_parameters["redshift"]["role"],
    )
    path = f"{path}upsert/test_redshift_copy_upsert2/"
    df2 = wr.redshift.unload(
        sql=f'SELECT * FROM public."{redshift_table}"',
        con=redshift_con,
        iam_role=databases_parameters["redshift"]["role"],
        path=path,
        keep_files=False,
    )
    assert len(df.index) == len(df2.index)
    assert len(df.columns) == len(df2.columns)

    # UPSERT
    path = f"{path}upsert/test_redshift_copy_upsert3/"
    wr.redshift.copy(
        df=df3,
        path=path,
        con=redshift_con,
        schema="public",
        table=redshift_table,
        mode="upsert",
        index=False,
        primary_keys=["id"],
        iam_role=databases_parameters["redshift"]["role"],
    )
    path = f"{path}upsert/test_redshift_copy_upsert4/"
    df4 = wr.redshift.unload(
        sql=f'SELECT * FROM public."{redshift_table}"',
        con=redshift_con,
        iam_role=databases_parameters["redshift"]["role"],
        path=path,
        keep_files=False,
    )
    assert len(df.index) + len(df3.index) == len(df4.index)
    assert len(df.columns) == len(df4.columns)

    # UPSERT 2 + lock
    wr.redshift.copy(
        df=df3,
        path=path,
        con=redshift_con,
        schema="public",
        table=redshift_table,
        mode="upsert",
        index=False,
        iam_role=databases_parameters["redshift"]["role"],
        lock=True,
    )
    path = f"{path}upsert/test_redshift_copy_upsert4/"
    df4 = wr.redshift.unload(
        sql=f'SELECT * FROM public."{redshift_table}"',
        con=redshift_con,
        iam_role=databases_parameters["redshift"]["role"],
        path=path,
        keep_files=False,
    )
    assert len(df.index) + len(df3.index) == len(df4.index)
    assert len(df.columns) == len(df4.columns)


def test_copy_upsert(
    path: str, redshift_table: str, redshift_con: redshift_connector.Connection, databases_parameters: dict[str, Any]
) -> None:
    generic_test_copy_upsert(path, redshift_table, redshift_con, databases_parameters)


def test_copy_upsert_hyphenated_name(
    path: str,
    redshift_table_with_hyphenated_name: str,
    redshift_con: redshift_connector.Connection,
    databases_parameters: dict[str, Any],
) -> None:
    generic_test_copy_upsert(path, redshift_table_with_hyphenated_name, redshift_con, databases_parameters)


@pytest.mark.parametrize(
    "diststyle,distkey,exc,sortstyle,sortkey",
    [
        ("FOO", "name", wr.exceptions.InvalidRedshiftDiststyle, None, None),
        ("KEY", "FOO", wr.exceptions.InvalidRedshiftDistkey, None, None),
        ("KEY", None, wr.exceptions.InvalidRedshiftDistkey, None, None),
        (None, None, wr.exceptions.InvalidRedshiftSortkey, None, ["foo"]),
        (None, None, wr.exceptions.InvalidRedshiftSortkey, None, 1),
        (None, None, wr.exceptions.InvalidRedshiftSortstyle, "foo", ["id"]),
    ],
)
def test_exceptions(
    path: str,
    redshift_table: str,
    redshift_con: redshift_connector.Connection,
    databases_parameters: dict[str, Any],
    diststyle: str | None,
    distkey: str | None,
    sortstyle: str | None,
    sortkey: list[str] | None,
    exc: type[BaseException],
) -> None:
    df = pd.DataFrame({"id": [1], "name": "joe"})
    with pytest.raises(exc):
        wr.redshift.copy(
            df=df,
            path=path,
            con=redshift_con,
            schema="public",
            table=redshift_table,
            mode="overwrite",
            diststyle=diststyle,
            distkey=distkey,
            sortstyle=sortstyle,
            sortkey=sortkey,
            iam_role=databases_parameters["redshift"]["role"],
            index=False,
        )


def test_spectrum(
    path: str,
    redshift_table: str,
    redshift_con: redshift_connector.Connection,
    glue_database: str,
    redshift_external_schema: str,
) -> None:
    df = pd.DataFrame({"id": [1, 2, 3, 4, 5], "col_str": ["foo", None, "bar", None, "xoo"], "par_int": [0, 1, 0, 1, 1]})
    wr.s3.to_parquet(
        df=df,
        path=path,
        database=glue_database,
        table=redshift_table,
        mode="overwrite",
        index=False,
        dataset=True,
        partition_cols=["par_int"],
    )
    with redshift_con.cursor() as cursor:
        cursor.execute(f"SELECT * FROM {redshift_external_schema}.{redshift_table}")
        rows = cursor.fetchall()
        assert len(rows) == len(df.index)
        for row in rows:
            assert len(row) == len(df.columns)


@pytest.mark.xfail(raises=NotImplementedError, reason="Unable to create pandas categorical from pyarrow table")
def test_category(
    path: str, redshift_table: str, redshift_con: redshift_connector.Connection, databases_parameters: dict[str, Any]
) -> None:
    df = get_df_category().drop(["binary"], axis=1, inplace=False)
    wr.redshift.copy(
        df=df,
        path=path,
        con=redshift_con,
        schema="public",
        table=redshift_table,
        mode="overwrite",
        iam_role=databases_parameters["redshift"]["role"],
    )
    df2 = wr.redshift.unload(
        sql=f"SELECT * FROM public.{redshift_table}",
        con=redshift_con,
        iam_role=databases_parameters["redshift"]["role"],
        path=path,
        keep_files=False,
        pyarrow_additional_kwargs={"categories": df.columns.to_list(), "strings_to_categorical": True},
    )
    ensure_data_types_category(df2)
    dfs = wr.redshift.unload(
        sql=f"SELECT * FROM public.{redshift_table}",
        con=redshift_con,
        iam_role=databases_parameters["redshift"]["role"],
        path=path,
        keep_files=False,
        chunked=True,
        pyarrow_additional_kwargs={"categories": df.columns.to_list(), "strings_to_categorical": True},
    )
    for df2 in dfs:
        ensure_data_types_category(df2)


def test_unload_extras(
    bucket: str,
    path: str,
    redshift_table: str,
    redshift_con: redshift_connector.Connection,
    databases_parameters: dict[str, Any],
    kms_key_id: str,
) -> None:
    table = redshift_table
    schema = databases_parameters["redshift"]["schema"]
    df = pd.DataFrame({"id": [1, 2], "name": ["foo", "boo"]})
    wr.redshift.to_sql(df=df, con=redshift_con, table=table, schema=schema, mode="overwrite", index=False)
    wr.redshift.unload_to_files(
        sql=f"SELECT * FROM {schema}.{table}",
        path=path,
        con=redshift_con,
        iam_role=databases_parameters["redshift"]["role"],
        region=wr.s3.get_bucket_region(bucket),
        max_file_size=5.0,
        kms_key_id=kms_key_id,
        partition_cols=["name"],
    )
    df = wr.s3.read_parquet(path=path, dataset=True)
    assert len(df.index) == 2
    assert len(df.columns) == 2
    wr.s3.delete_objects(path=path)
    df = wr.redshift.unload(
        sql=f"SELECT * FROM {schema}.{table}",
        con=redshift_con,
        iam_role=databases_parameters["redshift"]["role"],
        path=path,
        keep_files=False,
        region=wr.s3.get_bucket_region(bucket),
        max_file_size=5.0,
        kms_key_id=kms_key_id,
    )
    assert len(df.index) == 2
    assert len(df.columns) == 2


@pytest.mark.parametrize("unload_format", [None, "CSV", "PARQUET"])
def test_unload_with_prefix(
    bucket: str,
    path: str,
    redshift_table: str,
    redshift_con: redshift_connector.Connection,
    databases_parameters: dict[str, Any],
    kms_key_id: str,
    unload_format: str | None,
) -> None:
    test_prefix = "my_prefix"
    table = redshift_table
    schema = databases_parameters["redshift"]["schema"]
    df = pd.DataFrame({"id": [1, 2], "name": ["foo", "boo"]})
    wr.redshift.to_sql(df=df, con=redshift_con, table=table, schema=schema, mode="overwrite", index=False)

    args = {
        "sql": f"SELECT * FROM {schema}.{table}",
        "path": f"{path}{test_prefix}",
        "con": redshift_con,
        "iam_role": databases_parameters["redshift"]["role"],
        "region": wr.s3.get_bucket_region(bucket),
        "max_file_size": 5.0,
        "kms_key_id": kms_key_id,
        "unload_format": unload_format,
    }
    # Adding a prefix to S3 output files
    wr.redshift.unload_to_files(**args)
    filename = wr.s3.list_objects(path=path)[0].split("/")[-1]
    assert filename.startswith(test_prefix)

    # Prefix becomes part of path with partitioning
    wr.redshift.unload_to_files(
        **args,
        partition_cols=["name"],
    )
    object_prefix = wr.s3.list_objects(path=path)[0].split("/")[-3]
    assert object_prefix == test_prefix


def test_to_sql_cast(redshift_table: str, redshift_con: redshift_connector.Connection) -> None:
    table = redshift_table
    df = pd.DataFrame(
        {
            "col": [
                "".join([str(i)[-1] for i in range(1_024)]),
                "".join([str(i)[-1] for i in range(1_024)]),
                "".join([str(i)[-1] for i in range(1_024)]),
            ]
        },
        dtype="string",
    )
    wr.redshift.to_sql(
        df=df,
        con=redshift_con,
        table=table,
        schema="public",
        mode="overwrite",
        index=False,
        dtype={"col": "VARCHAR(1024)"},
    )
    df2 = wr.redshift.read_sql_query(sql=f"SELECT * FROM public.{table}", con=redshift_con)
    assert df.equals(df2)


def test_null(redshift_table: str, redshift_con: redshift_connector.Connection) -> None:
    table = redshift_table
    df = pd.DataFrame({"id": [1, 2, 3], "nothing": [None, None, None]})
    wr.redshift.to_sql(
        df=df,
        con=redshift_con,
        table=table,
        schema="public",
        mode="overwrite",
        index=False,
        dtype={"nothing": "INTEGER"},
    )
    wr.redshift.to_sql(
        df=df,
        con=redshift_con,
        table=table,
        schema="public",
        mode="append",
        index=False,
    )
    df2 = wr.redshift.read_sql_table(table=table, schema="public", con=redshift_con)
    df["id"] = df["id"].astype("Int64")
    assert pandas_equals(pd.concat(objs=[df, df], ignore_index=True), df2)


def test_spectrum_long_string(
    path: str,
    redshift_con: redshift_connector.Connection,
    glue_table: str,
    glue_database: str,
    redshift_external_schema: str,
) -> None:
    df = pd.DataFrame(
        {
            "id": [1, 2],
            "col_str": [
                "".join(random.choice(string.ascii_letters) for _ in range(300)),
                "".join(random.choice(string.ascii_letters) for _ in range(300)),
            ],
        }
    )
    wr.s3.to_parquet(
        df=df, path=path, database=glue_database, table=glue_table, mode="overwrite", index=False, dataset=True
    )
    with redshift_con.cursor() as cursor:
        cursor.execute(f"SELECT * FROM {redshift_external_schema}.{glue_table}")
        rows = cursor.fetchall()
        assert len(rows) == len(df.index)
        for row in rows:
            assert len(row) == len(df.columns)


def test_copy_unload_long_string(
    path: str, redshift_table: str, redshift_con: redshift_connector.Connection, databases_parameters: dict[str, Any]
) -> None:
    df = pd.DataFrame(
        {
            "id": [1, 2],
            "col_str": [
                "".join(random.choice(string.ascii_letters) for _ in range(300)),
                "".join(random.choice(string.ascii_letters) for _ in range(300)),
            ],
        }
    )
    wr.redshift.copy(
        df=df,
        path=path,
        con=redshift_con,
        schema="public",
        table=redshift_table,
        mode="overwrite",
        varchar_lengths={"col_str": 300},
        iam_role=databases_parameters["redshift"]["role"],
    )
    df2 = wr.redshift.unload(
        sql=f"SELECT * FROM public.{redshift_table}",
        con=redshift_con,
        iam_role=databases_parameters["redshift"]["role"],
        path=path,
        keep_files=False,
    )
    assert df2.shape == (2, 2)


def test_spectrum_decimal_cast(
    path: str,
    path2: str,
    glue_table: str,
    glue_database: str,
    redshift_external_schema: str,
    databases_parameters: dict[str, Any],
) -> None:
    df = pd.DataFrame(
        {"c0": [1, 2], "c1": [1, None], "c2": [2.22222, None], "c3": ["3.33333", None], "c4": [None, None]}
    )
    wr.s3.to_parquet(
        df=df,
        path=path,
        database=glue_database,
        table=glue_table,
        dataset=True,
        dtype={"c1": "decimal(11,5)", "c2": "decimal(11,5)", "c3": "decimal(11,5)", "c4": "decimal(11,5)"},
    )

    # Athena
    df2 = wr.athena.read_sql_table(table=glue_table, database=glue_database)
    assert df2.shape == (2, 5)
    df2 = df2[df2.c0 != 2].reset_index(drop=True)
    assert df2.c1[0] == Decimal("1.00000")
    assert df2.c2[0] == Decimal("2.22222")
    assert df2.c3[0] == Decimal("3.33333")
    assert df2.c4[0] is None

    # Redshift Spectrum
    with wr.redshift.connect(connection="aws-sdk-pandas-redshift") as con:
        df2 = wr.redshift.read_sql_table(table=glue_table, schema=redshift_external_schema, con=con)
    assert df2.shape == (2, 5)
    df2 = df2[df2.c0 != 2].reset_index(drop=True)
    assert df2.c1[0] == Decimal("1.00000")
    assert df2.c2[0] == Decimal("2.22222")
    assert df2.c3[0] == Decimal("3.33333")
    assert df2.c4[0] is None

    # Redshift Spectrum Unload
    with wr.redshift.connect(connection="aws-sdk-pandas-redshift") as con:
        df2 = wr.redshift.unload(
            sql=f"SELECT * FROM {redshift_external_schema}.{glue_table}",
            con=con,
            iam_role=databases_parameters["redshift"]["role"],
            path=path2,
        )
    assert df2.shape == (2, 5)
    df2 = df2[df2.c0 != 2].reset_index(drop=True)
    assert df2.c1[0] == Decimal("1.00000")
    assert df2.c2[0] == Decimal("2.22222")
    assert df2.c3[0] == Decimal("3.33333")
    assert df2.c4[0] is None


@pytest.mark.xfail(
    is_ray_modin, raises=wr.exceptions.InvalidArgument, reason="kwargs not supported in distributed mode"
)
@pytest.mark.parametrize(
    "s3_additional_kwargs",
    [None, {"ServerSideEncryption": "AES256"}, {"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": None}],
)
@pytest.mark.parametrize("use_threads", [True, False])
@pytest.mark.parametrize("parallel", [True, False])
def test_copy_unload_kms(
    path: str,
    redshift_table: str,
    redshift_con: redshift_connector.Connection,
    databases_parameters: dict[str, Any],
    kms_key_id: str,
    use_threads: bool,
    parallel: bool,
    s3_additional_kwargs: dict[str, Any] | None,
) -> None:
    df = pd.DataFrame({"id": [1, 2, 3]})
    if s3_additional_kwargs is not None and "SSEKMSKeyId" in s3_additional_kwargs:
        s3_additional_kwargs["SSEKMSKeyId"] = kms_key_id
    wr.redshift.copy(
        df=df,
        path=path,
        con=redshift_con,
        schema="public",
        table=redshift_table,
        mode="overwrite",
        iam_role=databases_parameters["redshift"]["role"],
        use_threads=use_threads,
        s3_additional_kwargs=s3_additional_kwargs,
    )
    df2 = wr.redshift.unload(
        sql=f"SELECT * FROM public.{redshift_table}",
        con=redshift_con,
        iam_role=databases_parameters["redshift"]["role"],
        path=path,
        keep_files=False,
        parallel=parallel,
        use_threads=use_threads,
        s3_additional_kwargs=s3_additional_kwargs,
    )
    assert df.shape == df2.shape


@pytest.mark.parametrize("parquet_infer_sampling", [1.0, 0.00000000000001])
@pytest.mark.parametrize("use_threads", [True, False])
def test_copy_extras(
    path: str,
    redshift_table: str,
    redshift_con: redshift_connector.Connection,
    databases_parameters: dict[str, Any],
    use_threads: bool,
    parquet_infer_sampling: float,
) -> None:
    df = pd.DataFrame(
        {
            "int8": [-1, None, 2],
            "int16": [-1, None, 2],
            "int32": [-1, None, 2],
            "int64": [-1, None, 2],
            "float": [0.0, None, -1.1],
            "double": [0.0, None, 1.1],
            "decimal": [Decimal((0, (1, 9, 9), -2)), None, Decimal((0, (1, 9, 0), -2))],
            "string": ["foo", None, "boo"],
            "date": [dt("2020-01-01"), None, dt("2020-01-02")],
            "timestamp": [ts("2020-01-01 00:00:00.0"), None, ts("2020-01-02 00:00:01.0")],
            "bool": [True, None, False],
        }
    )
    df["int8"] = df["int8"].astype("Int8")
    df["int16"] = df["int16"].astype("Int16")
    df["int32"] = df["int32"].astype("Int32")
    df["int64"] = df["int64"].astype("Int64")
    df["float"] = df["float"].astype("float32")
    df["string"] = df["string"].astype("string")
    num = 3
    for i in range(num):
        p = f"{path}{i}.parquet"
        wr.s3.to_parquet(df, p, use_threads=use_threads)
    wr.redshift.copy_from_files(
        path=path,
        con=redshift_con,
        schema="public",
        table=redshift_table,
        mode="overwrite",
        iam_role=databases_parameters["redshift"]["role"],
        use_threads=use_threads,
        parquet_infer_sampling=parquet_infer_sampling,
    )
    df2 = wr.redshift.read_sql_table(schema="public", table=redshift_table, con=redshift_con)
    assert len(df.columns) == len(df2.columns)
    assert len(df.index) * num == len(df2.index)
    assert df.int8.sum() * num == df2.int8.sum()
    assert df.int16.sum() * num == df2.int16.sum()
    assert df.int32.sum() * num == df2.int32.sum()
    assert df.int64.sum() * num == df2.int64.sum()


def test_decimal_cast(redshift_table: str, redshift_con: redshift_connector.Connection) -> None:
    df = pd.DataFrame(
        {
            "col0": [Decimal((0, (1, 9, 9), -2)), None, Decimal((0, (1, 9, 0), -2))],
            "col1": [Decimal((0, (1, 9, 9), -2)), None, Decimal((0, (1, 9, 0), -2))],
            "col2": [Decimal((0, (1, 9, 9), -2)), None, Decimal((0, (1, 9, 0), -2))],
        }
    )
    wr.redshift.to_sql(df, redshift_con, redshift_table, "public")
    df2 = wr.redshift.read_sql_table(
        schema="public",
        table=redshift_table,
        con=redshift_con,
        dtype={"col0": "float32", "col1": "float64", "col2": "Int64"},
    )
    assert df2.dtypes.to_list() == ["float32", "float64", "Int64"]
    assert 3.88 <= df2.col0.sum() <= 3.89
    assert 3.88 <= df2.col1.sum() <= 3.89
    assert df2.col2.sum() == 2


def test_upsert(redshift_table: str, redshift_con: redshift_connector.Connection) -> None:
    df = pd.DataFrame({"id": list(range(10)), "val": list(["foo" if i % 2 == 0 else "boo" for i in range(10)])})
    df3 = pd.DataFrame({"id": list(range(10, 15)), "val": list(["foo" if i % 2 == 0 else "boo" for i in range(5)])})

    # CREATE
    wr.redshift.to_sql(
        df=df,
        con=redshift_con,
        schema="public",
        table=redshift_table,
        mode="overwrite",
        index=False,
        primary_keys=["id"],
    )
    df2 = wr.redshift.read_sql_query(sql=f"SELECT * FROM public.{redshift_table}", con=redshift_con)
    assert df.shape == df2.shape

    # UPSERT
    wr.redshift.to_sql(
        df=df3,
        con=redshift_con,
        schema="public",
        table=redshift_table,
        mode="upsert",
        index=False,
        primary_keys=["id"],
    )
    df4 = wr.redshift.read_sql_query(
        sql=f"SELECT * FROM public.{redshift_table}",
        con=redshift_con,
    )
    assert len(df.index) + len(df3.index) == len(df4.index)
    assert len(df.columns) == len(df4.columns)

    # UPSERT 2
    wr.redshift.to_sql(
        df=df3,
        con=redshift_con,
        schema="public",
        table=redshift_table,
        mode="upsert",
        index=False,
    )
    df4 = wr.redshift.read_sql_query(sql=f"SELECT * FROM public.{redshift_table}", con=redshift_con)
    assert len(df.index) + len(df3.index) == len(df4.index)
    assert len(df.columns) == len(df4.columns)


def test_upsert_precombine(redshift_table: str, redshift_con: redshift_connector.Connection) -> None:
    df = pd.DataFrame({"id": list(range(10)), "val": list([1.0 if i % 2 == 0 else 10.0 for i in range(10)])})
    df3 = pd.DataFrame({"id": list(range(6, 14)), "val": list([10.0 if i % 2 == 0 else 1.0 for i in range(8)])})

    # Do upsert in pandas
    df_m = to_pandas(pd.merge(df, df3, on="id", how="outer"))
    df_m["val"] = np.where(df_m["val_y"] >= df_m["val_x"], df_m["val_y"], df_m["val_x"])
    df_m["val"] = df_m["val"].fillna(df_m["val_y"])
    df_m = df_m.drop(columns=["val_x", "val_y"])

    # CREATE
    wr.redshift.to_sql(
        df=df,
        con=redshift_con,
        schema="public",
        table=redshift_table,
        mode="overwrite",
        index=False,
        primary_keys=["id"],
    )
    df2 = wr.redshift.read_sql_query(sql=f"SELECT * FROM public.{redshift_table} order by id", con=redshift_con)
    assert df.shape == df2.shape

    # UPSERT
    wr.redshift.to_sql(
        df=df3,
        con=redshift_con,
        schema="public",
        table=redshift_table,
        mode="upsert",
        index=False,
        primary_keys=["id"],
        precombine_key="val",
    )
    df4 = wr.redshift.read_sql_query(
        sql=f"SELECT * FROM public.{redshift_table} order by id",
        con=redshift_con,
    )
    assert np.array_equal(df_m.to_numpy(), df4.to_numpy())

    # UPSERT 2
    wr.redshift.to_sql(
        df=df3,
        con=redshift_con,
        schema="public",
        table=redshift_table,
        mode="upsert",
        index=False,
        precombine_key="val",
    )
    df4 = wr.redshift.read_sql_query(sql=f"SELECT * FROM public.{redshift_table} order by id", con=redshift_con)
    assert np.array_equal(df_m.to_numpy(), df4.to_numpy())


def test_read_retry(redshift_con: redshift_connector.Connection) -> None:
    try:
        wr.redshift.read_sql_query("ERROR", redshift_con)
    except:  # noqa
        pass
    df = wr.redshift.read_sql_query("SELECT 1", redshift_con)
    assert df.shape == (1, 1)


def test_table_name(redshift_con: redshift_connector.Connection) -> None:
    df = pd.DataFrame({"col0": [1]})
    wr.redshift.to_sql(df, redshift_con, "Test Name", "public", mode="overwrite")
    df = wr.redshift.read_sql_table(schema="public", con=redshift_con, table="Test Name")
    assert df.shape == (1, 1)
    with redshift_con.cursor() as cursor:
        cursor.execute('DROP TABLE "Test Name"')
    redshift_con.commit()


@pytest.mark.parametrize("data_format", ["parquet", "orc", "csv"])
def test_copy_from_files(
    path: str,
    redshift_table: str,
    redshift_con: redshift_connector.Connection,
    databases_parameters: dict[str, Any],
    data_format: str,
) -> None:
    from awswrangler import _utils

    bucket, key = _utils.parse_path(f"{path}test.txt")
    boto3.client("s3").put_object(Body=b"", Bucket=bucket, Key=key)

    df = get_df_category().drop(["binary"], axis=1, inplace=False)

    column_types = {}
    if data_format == "parquet":
        wr.s3.to_parquet(df, f"{path}test.parquet")
    elif data_format == "orc":
        wr.s3.to_orc(df, f"{path}test.orc")
    else:
        wr.s3.to_csv(df, f"{path}test.csv", index=False, header=False)
        column_types = {
            "id": "BIGINT",
            "string": "VARCHAR(256)",
            "string_object": "VARCHAR(256)",
            "float": "FLOAT8",
            "int": "BIGINT",
            "par0": "BIGINT",
            "par1": "VARCHAR(256)",
        }

    wr.redshift.copy_from_files(
        path=path,
        path_suffix=f".{data_format}",
        con=redshift_con,
        table=redshift_table,
        data_format=data_format,
        redshift_column_types=column_types,
        schema="public",
        iam_role=databases_parameters["redshift"]["role"],
    )
    df2 = wr.redshift.read_sql_query(sql=f"SELECT count(*) AS counter FROM public.{redshift_table}", con=redshift_con)
    assert df2["counter"].iloc[0] == 3


def test_copy_from_files_extra_params(
    path: str, redshift_table: str, redshift_con: redshift_connector.Connection, databases_parameters: dict[str, Any]
) -> None:
    df = get_df_category().drop(["binary"], axis=1, inplace=False)
    wr.s3.to_parquet(df, f"{path}test.parquet")
    bucket, key = wr._utils.parse_path(f"{path}test.csv")
    boto3.client("s3").put_object(Body=b"", Bucket=bucket, Key=key)
    wr.redshift.copy_from_files(
        path=path,
        path_suffix=".parquet",
        con=redshift_con,
        table=redshift_table,
        schema="public",
        iam_role=databases_parameters["redshift"]["role"],
        sql_copy_extra_params=["STATUPDATE ON"],
    )
    df2 = wr.redshift.read_sql_query(sql=f"SELECT count(*) AS counter FROM public.{redshift_table}", con=redshift_con)
    assert df2["counter"].iloc[0] == 3


def test_copy_from_files_geometry_column(
    path: str, redshift_table: str, redshift_con: redshift_connector.Connection, databases_parameters: dict[str, Any]
) -> None:
    df = pd.DataFrame({"id": [1, 2, 3], "geometry": ["POINT(1 1)", "POINT(2 2)", "POINT(3 3)"]})
    wr.s3.to_csv(df, f"{path}test-geometry.csv", index=False, header=False)

    wr.redshift.copy_from_files(
        path=path,
        con=redshift_con,
        table=redshift_table,
        schema="public",
        iam_role=databases_parameters["redshift"]["role"],
        data_format="csv",
        redshift_column_types={
            "id": "BIGINT",
            "geometry": "GEOMETRY",
        },
    )

    df2 = wr.redshift.read_sql_query(sql=f"SELECT count(*) AS counter FROM public.{redshift_table}", con=redshift_con)
    assert df2["counter"].iloc[0] == 3


def test_get_paths_from_manifest(path: str) -> None:
    from awswrangler.redshift._utils import _get_paths_from_manifest

    manifest_content = {
        "entries": [
            {"url": f"{path}test0.parquet", "mandatory": False},
            {"url": f"{path}test1.parquet", "mandatory": False},
            {"url": f"{path}test2.parquet", "mandatory": True},
        ]
    }
    manifest_bucket, manifest_key = wr._utils.parse_path(f"{path}manifest.json")
    boto3.client("s3").put_object(
        Body=bytes(json.dumps(manifest_content).encode("UTF-8")), Bucket=manifest_bucket, Key=manifest_key
    )
    paths = _get_paths_from_manifest(path=f"{path}manifest.json")

    assert len(paths) == 3


def test_copy_from_files_manifest(
    path: str, redshift_table: str, redshift_con: redshift_connector.Connection, databases_parameters: dict[str, Any]
) -> None:
    df = get_df_category().drop(["binary"], axis=1, inplace=False)
    wr.s3.to_parquet(df, f"{path}test.parquet")
    bucket, key = wr._utils.parse_path(f"{path}test.parquet")
    content_length = boto3.client("s3").head_object(Bucket=bucket, Key=key)["ContentLength"]
    manifest_content = {
        "entries": [{"url": f"{path}test.parquet", "mandatory": False, "meta": {"content_length": content_length}}]
    }
    manifest_bucket, manifest_key = wr._utils.parse_path(f"{path}manifest.json")
    boto3.client("s3").put_object(
        Body=bytes(json.dumps(manifest_content).encode("UTF-8")), Bucket=manifest_bucket, Key=manifest_key
    )
    wr.redshift.copy_from_files(
        path=f"{path}manifest.json",
        con=redshift_con,
        table=redshift_table,
        schema="public",
        iam_role=databases_parameters["redshift"]["role"],
        manifest=True,
    )
    df2 = wr.redshift.read_sql_query(sql=f"SELECT count(*) AS counter FROM public.{redshift_table}", con=redshift_con)
    assert df2["counter"].iloc[0] == 3


@pytest.mark.parametrize("path_ignore_suffix", [".csv", [".csv"]])
def test_copy_from_files_ignore(
    path: str,
    redshift_table: str,
    redshift_con: redshift_connector.Connection,
    databases_parameters: dict[str, Any],
    path_ignore_suffix: str | list[str],
) -> None:
    df = get_df_category().drop(["binary"], axis=1, inplace=False)
    wr.s3.to_parquet(df, f"{path}test.parquet")
    bucket, key = wr._utils.parse_path(f"{path}test.csv")
    boto3.client("s3").put_object(Body=b"", Bucket=bucket, Key=key)
    wr.redshift.copy_from_files(
        path=path,
        path_ignore_suffix=path_ignore_suffix,
        con=redshift_con,
        table=redshift_table,
        schema="public",
        iam_role=databases_parameters["redshift"]["role"],
    )
    df2 = wr.redshift.read_sql_query(sql=f"SELECT count(*) AS counter FROM public.{redshift_table}", con=redshift_con)
    assert df2["counter"].iloc[0] == 3


def test_copy_from_files_empty(
    path: str, redshift_table: str, redshift_con: redshift_connector.Connection, databases_parameters: dict[str, Any]
) -> None:
    df = get_df_category().drop(["binary"], axis=1, inplace=False)
    wr.s3.to_parquet(df, f"{path}test.parquet")
    bucket, key = wr._utils.parse_path(f"{path}test.csv")
    boto3.client("s3").put_object(Body=b"", Bucket=bucket, Key=key)
    wr.redshift.copy_from_files(
        path=path,
        con=redshift_con,
        table=redshift_table,
        schema="public",
        iam_role=databases_parameters["redshift"]["role"],
    )
    df2 = wr.redshift.read_sql_query(sql=f"SELECT count(*) AS counter FROM public.{redshift_table}", con=redshift_con)
    assert df2["counter"].iloc[0] == 3


def test_copy_dirty_path(
    path: str, redshift_table: str, redshift_con: redshift_connector.Connection, databases_parameters: dict[str, Any]
) -> None:
    df = pd.DataFrame({"col0": [0, 1, 2]})

    # previous file at same path
    wr.s3.to_parquet(df, f"{path}test.parquet")

    with pytest.raises(wr.exceptions.InvalidArgument):
        wr.redshift.copy(  # Trying to copy using a dirty path
            df=df,
            path=path,
            con=redshift_con,
            table=redshift_table,
            schema="public",
            iam_role=databases_parameters["redshift"]["role"],
        )


@pytest.mark.parametrize("dbname", [None, "test"])
def test_connect_secret_manager(dbname: str | None) -> None:
    with wr.redshift.connect(secret_id="aws-sdk-pandas/redshift", dbname=dbname) as con:
        df = wr.redshift.read_sql_query("SELECT 1", con=con)
    assert df.shape == (1, 1)


def test_copy_unload_session(path: str, redshift_table: str, redshift_con: redshift_connector.Connection) -> None:
    df = pd.DataFrame({"col0": [1, 2, 3]})
    wr.redshift.copy(df=df, path=path, con=redshift_con, schema="public", table=redshift_table, mode="overwrite")
    df2 = wr.redshift.unload(
        sql=f"SELECT * FROM public.{redshift_table}",
        con=redshift_con,
        path=path,
        keep_files=False,
    )
    assert df2.shape == (3, 1)
    wr.redshift.copy(
        df=df,
        path=path,
        con=redshift_con,
        schema="public",
        table=redshift_table,
        mode="append",
    )
    df2 = wr.redshift.unload(
        sql=f"SELECT * FROM public.{redshift_table}",
        con=redshift_con,
        path=path,
        keep_files=False,
    )
    assert df2.shape == (6, 1)
    dfs = wr.redshift.unload(
        sql=f"SELECT * FROM public.{redshift_table}",
        con=redshift_con,
        path=path,
        keep_files=False,
        chunked=True,
    )
    for chunk in dfs:
        assert len(chunk.columns) == 1


def test_copy_unload_creds(path: str, redshift_table: str, redshift_con: redshift_connector.Connection) -> None:
    credentials = _utils.get_credentials_from_session()
    df = pd.DataFrame({"col0": [1, 2, 3]})
    wr.redshift.copy(
        df=df,
        path=path,
        con=redshift_con,
        schema="public",
        table=redshift_table,
        mode="overwrite",
        aws_access_key_id=credentials.access_key,
        aws_secret_access_key=credentials.secret_key,
        aws_session_token=credentials.token,
    )
    df2 = wr.redshift.unload(
        sql=f"SELECT * FROM public.{redshift_table}",
        con=redshift_con,
        path=path,
        keep_files=False,
        aws_access_key_id=credentials.access_key,
        aws_secret_access_key=credentials.secret_key,
        aws_session_token=credentials.token,
    )
    assert df2.shape == (3, 1)
    wr.redshift.copy(
        df=df,
        path=path,
        con=redshift_con,
        schema="public",
        table=redshift_table,
        mode="append",
        aws_access_key_id=credentials.access_key,
        aws_secret_access_key=credentials.secret_key,
        aws_session_token=credentials.token,
    )
    df2 = wr.redshift.unload(
        sql=f"SELECT * FROM public.{redshift_table}",
        con=redshift_con,
        path=path,
        keep_files=False,
        aws_access_key_id=credentials.access_key,
        aws_secret_access_key=credentials.secret_key,
        aws_session_token=credentials.token,
    )
    assert df2.shape == (6, 1)
    dfs = wr.redshift.unload(
        sql=f"SELECT * FROM public.{redshift_table}",
        con=redshift_con,
        path=path,
        keep_files=False,
        chunked=True,
        aws_access_key_id=credentials.access_key,
        aws_secret_access_key=credentials.secret_key,
        aws_session_token=credentials.token,
    )
    for chunk in dfs:
        assert len(chunk.columns) == 1


def test_column_length(
    path: str, redshift_table: str, redshift_con: redshift_connector.Connection, databases_parameters: dict[str, Any]
) -> None:
    df = pd.DataFrame({"a": ["foo"], "b": ["a" * 5000]}, dtype="string")
    wr.s3.to_parquet(df, f"{path}test.parquet")
    wr.redshift.copy_from_files(
        path=path,
        con=redshift_con,
        table=redshift_table,
        schema="public",
        iam_role=databases_parameters["redshift"]["role"],
        varchar_lengths={"a": 3, "b": 5000},
        primary_keys=["a"],
    )
    df2 = wr.redshift.read_sql_query(sql=f"SELECT * FROM public.{redshift_table}", con=redshift_con)
    assert pandas_equals(df, df2)


def test_failed_keep_files(
    path: str, redshift_table: str, redshift_con: redshift_connector.Connection, databases_parameters: dict[str, Any]
) -> None:
    df = pd.DataFrame({"c0": [1], "c1": ["foo"]}, dtype="string")
    with pytest.raises(ProgrammingError):
        wr.redshift.copy(
            df=df,
            path=path,
            con=redshift_con,
            table=redshift_table,
            schema="public",
            iam_role=databases_parameters["redshift"]["role"],
            varchar_lengths={"c1": 2},
        )
    assert len(wr.s3.list_objects(path)) == 0


def test_insert_with_column_names(redshift_table: str, redshift_con: redshift_connector.Connection) -> None:
    create_table_sql = (
        f"CREATE TABLE public.{redshift_table} " "(c0 varchar(100), " "c1 integer default 42, " "c2 integer not null);"
    )
    with redshift_con.cursor() as cursor:
        cursor.execute(create_table_sql)
        redshift_con.commit()

    df = pd.DataFrame({"c0": ["foo", "bar"], "c2": [1, 2]})

    with pytest.raises(redshift_connector.error.ProgrammingError):
        wr.redshift.to_sql(
            df=df, con=redshift_con, schema="public", table=redshift_table, mode="append", use_column_names=False
        )

    wr.redshift.to_sql(
        df=df, con=redshift_con, schema="public", table=redshift_table, mode="append", use_column_names=True
    )

    df2 = wr.redshift.read_sql_table(con=redshift_con, schema="public", table=redshift_table)

    df["c1"] = 42
    df["c0"] = df["c0"].astype("string")
    df["c1"] = df["c1"].astype("Int64")
    df["c2"] = df["c2"].astype("Int64")
    df = df.reindex(sorted(df.columns), axis=1)
    assert df.equals(df2)


@pytest.mark.parametrize("chunksize", [1, 10, 500])
def test_dfs_are_equal_for_different_chunksizes(
    redshift_table: str, redshift_con: redshift_connector.Connection, chunksize: int
) -> None:
    df = pd.DataFrame({"c0": [i for i in range(64)], "c1": ["foo" for _ in range(64)]})
    wr.redshift.to_sql(df=df, con=redshift_con, schema="public", table=redshift_table, chunksize=chunksize)

    df2 = wr.redshift.read_sql_table(con=redshift_con, schema="public", table=redshift_table)

    df["c0"] = df["c0"].astype("Int64")
    df["c1"] = df["c1"].astype("string")

    assert df.equals(df2)


def test_to_sql_multi_transaction(redshift_table: str, redshift_con: redshift_connector.Connection) -> None:
    df = pd.DataFrame({"id": list(range(10)), "val": list(["foo" if i % 2 == 0 else "boo" for i in range(10)])})
    df2 = pd.DataFrame({"id": list(range(10, 15)), "val": list(["foo" if i % 2 == 0 else "boo" for i in range(5)])})

    wr.redshift.to_sql(
        df=df,
        con=redshift_con,
        schema="public",
        table=redshift_table,
        mode="overwrite",
        index=False,
        primary_keys=["id"],
        commit_transaction=False,  # Not committing
    )

    wr.redshift.to_sql(
        df=df2,
        con=redshift_con,
        schema="public",
        table=redshift_table,
        mode="upsert",
        index=False,
        primary_keys=["id"],
        commit_transaction=False,  # Not committing
    )
    redshift_con.commit()
    df3 = wr.redshift.read_sql_query(sql=f"SELECT * FROM public.{redshift_table} ORDER BY id", con=redshift_con)
    assert len(df.index) + len(df2.index) == len(df3.index)
    assert len(df.columns) == len(df3.columns)


def test_copy_upsert_with_column_names(
    path: str, redshift_table: str, redshift_con: redshift_connector.Connection, databases_parameters: dict[str, Any]
) -> None:
    df = pd.DataFrame({"id": list(range(1_000)), "val": list(["foo" if i % 2 == 0 else "boo" for i in range(1_000)])})
    df3 = pd.DataFrame(
        {"id": list(range(1_000, 1_500)), "val": list(["foo" if i % 2 == 0 else "boo" for i in range(500)])}
    )

    # CREATE
    path = f"{path}upsert/test_redshift_copy_upsert_with_column_names/"
    wr.redshift.copy(
        df=df,
        path=path,
        con=redshift_con,
        schema="public",
        table=redshift_table,
        mode="overwrite",
        index=False,
        primary_keys=["id"],
        iam_role=databases_parameters["redshift"]["role"],
        use_column_names=True,
    )
    path = f"{path}upsert/test_redshift_copy_upsert_with_column_names2/"
    df2 = wr.redshift.unload(
        sql=f"SELECT * FROM public.{redshift_table}",
        con=redshift_con,
        iam_role=databases_parameters["redshift"]["role"],
        path=path,
        keep_files=False,
    )
    assert len(df.index) == len(df2.index)
    assert len(df.columns) == len(df2.columns)

    # UPSERT
    path = f"{path}upsert/test_redshift_copy_upsert_with_column_names3/"
    wr.redshift.copy(
        df=df3,
        path=path,
        con=redshift_con,
        schema="public",
        table=redshift_table,
        mode="upsert",
        index=False,
        primary_keys=["id"],
        iam_role=databases_parameters["redshift"]["role"],
        use_column_names=True,
    )
    path = f"{path}upsert/test_redshift_copy_upsert_with_column_names4/"
    df4 = wr.redshift.unload(
        sql=f"SELECT * FROM public.{redshift_table}",
        con=redshift_con,
        iam_role=databases_parameters["redshift"]["role"],
        path=path,
        keep_files=False,
    )
    assert len(df.index) + len(df3.index) == len(df4.index)
    assert len(df.columns) == len(df4.columns)

    # UPSERT 2 + lock
    wr.redshift.copy(
        df=df3,
        path=path,
        con=redshift_con,
        schema="public",
        table=redshift_table,
        mode="upsert",
        index=False,
        iam_role=databases_parameters["redshift"]["role"],
        lock=True,
        use_column_names=True,
    )
    path = f"{path}upsert/test_redshift_copy_upsert_with_column_names4/"
    df4 = wr.redshift.unload(
        sql=f"SELECT * FROM public.{redshift_table}",
        con=redshift_con,
        iam_role=databases_parameters["redshift"]["role"],
        path=path,
        keep_files=False,
    )
    assert len(df.index) + len(df3.index) == len(df4.index)
    assert len(df.columns) == len(df4.columns)


def test_to_sql_with_identity_column(redshift_table: str, redshift_con: redshift_connector.Connection) -> None:
    schema = "public"
    with redshift_con.cursor() as cursor:
        cursor.execute(
            f"""
            CREATE TABLE {schema}.{redshift_table} (
                id BIGINT IDENTITY(1, 1),
                foo VARCHAR(100),
                PRIMARY KEY(id)
            );
            """
        )

    df = pd.DataFrame({"foo": ["a", "b", "c"]})
    wr.redshift.to_sql(
        df=df,
        con=redshift_con,
        table=redshift_table,
        schema=schema,
        use_column_names=True,
        mode="append",
    )

    df_out = wr.redshift.read_sql_table(redshift_table, redshift_con)

    assert len(df_out) == len(df)
    assert df_out["id"].to_list() == list(range(1, len(df) + 1))
    assert df_out["foo"].to_list() == df["foo"].to_list()


def test_unload_escape_quotation_marks(
    path: str, redshift_table: str, redshift_con: redshift_connector.Connection, databases_parameters: dict[str, Any]
) -> None:
    df = get_df().drop(["binary"], axis=1, inplace=False)
    schema = "public"

    wr.redshift.to_sql(
        df=df,
        con=redshift_con,
        table=redshift_table,
        schema=schema,
        mode="overwrite",
    )
    df2 = wr.redshift.unload(
        sql=f"SELECT * FROM public.{redshift_table} WHERE string = 'Seattle'",
        con=redshift_con,
        iam_role=databases_parameters["redshift"]["role"],
        path=path,
        keep_files=False,
    )
    assert len(df2) == 1


@pytest.mark.parametrize(
    "mode,overwrite_method",
    [
        ("append", ""),
        ("upsert", ""),
        ("overwrite", "drop"),
        ("overwrite", "cascade"),
        ("overwrite", "truncate"),
        ("overwrite", "delete"),
    ],
)
def test_copy_add_new_columns(
    path: str,
    redshift_table: str,
    redshift_con: redshift_connector.Connection,
    databases_parameters: dict[str, Any],
    mode: str,
    overwrite_method: str,
) -> None:
    schema = "public"
    df = pd.DataFrame({"foo": ["a", "b", "c"], "bar": ["c", "d", "e"]})
    copy_kwargs = {
        "df": df,
        "path": path,
        "con": redshift_con,
        "schema": schema,
        "table": redshift_table,
        "iam_role": databases_parameters["redshift"]["role"],
        "primary_keys": ["foo"] if mode == "upsert" else None,
        "overwrite_method": overwrite_method,
    }

    # Create table
    wr.redshift.copy(**copy_kwargs, add_new_columns=True, mode="overwrite")
    copy_kwargs["mode"] = mode

    # Add new columns
    df["xoo"] = ["f", "g", "h"]
    df["baz"] = ["j", "k", "l"]
    wr.redshift.copy(**copy_kwargs, add_new_columns=True)

    sql = f"SELECT * FROM {schema}.{redshift_table}"
    if mode == "append":
        sql += "\nWHERE xoo IS NOT NULL AND baz IS NOT NULL"
    df2 = wr.redshift.read_sql_query(sql=sql, con=redshift_con)
    df2 = df2.sort_values(by=df2.columns.to_list())
    assert df.values.tolist() == df2.values.tolist()
    assert df.columns.tolist() == df2.columns.tolist()

    # Assert error when trying to add a new column without 'add_new_columns' parameter (False as default) in "append"
    # or "upsert". No error are expected in ('drop', 'cascade') overwrite_method
    df["abc"] = ["m", "n", "o"]
    if overwrite_method in ("drop", "cascade"):
        wr.redshift.copy(**copy_kwargs)
    else:
        with pytest.raises(redshift_connector.error.ProgrammingError) as exc_info:
            wr.redshift.copy(**copy_kwargs)
        assert "ProgrammingError" == exc_info.typename
        assert "unmatched number of columns" in str(exc_info.value).lower()


@pytest.mark.parametrize(
    "mode,overwrite_method",
    [
        ("append", ""),
        ("upsert", ""),
        ("overwrite", "drop"),
        ("overwrite", "cascade"),
        ("overwrite", "truncate"),
        ("overwrite", "delete"),
    ],
)
def test_to_sql_add_new_columns(
    path: str,
    redshift_table: str,
    redshift_con: redshift_connector.Connection,
    databases_parameters: dict[str, Any],
    mode: str,
    overwrite_method: str,
) -> None:
    schema = "public"
    df = pd.DataFrame({"foo": ["a", "b", "c"], "bar": ["c", "d", "e"]})
    to_sql_kwargs = {
        "df": df,
        "con": redshift_con,
        "schema": schema,
        "table": redshift_table,
        "primary_keys": ["foo"] if mode == "upsert" else None,
        "overwrite_method": overwrite_method,
    }

    # Create table
    wr.redshift.to_sql(**to_sql_kwargs, add_new_columns=True, mode="overwrite")
    to_sql_kwargs["mode"] = mode

    # Add new columns
    df["xoo"] = ["f", "g", "h"]
    df["baz"] = ["j", "k", "l"]
    wr.redshift.to_sql(**to_sql_kwargs, add_new_columns=True)

    sql = f"SELECT * FROM {schema}.{redshift_table}"
    if mode == "append":
        sql += "\nWHERE xoo IS NOT NULL AND baz IS NOT NULL"
    df2 = wr.redshift.read_sql_query(sql=sql, con=redshift_con)
    df2 = df2.sort_values(by=df2.columns.to_list())
    assert df.values.tolist() == df2.values.tolist()
    assert df.columns.tolist() == df2.columns.tolist()

    # Assert error when trying to add a new column without 'add_new_columns' parameter (False as default) in "append"
    # or "upsert". No errors expected in ('drop', 'cascade') overwrite_method
    df["abc"] = ["m", "n", "o"]
    if overwrite_method in ("drop", "cascade"):
        wr.redshift.to_sql(**to_sql_kwargs)
    else:
        with pytest.raises(redshift_connector.error.ProgrammingError) as exc_info:
            wr.redshift.to_sql(**to_sql_kwargs)
        assert "ProgrammingError" == exc_info.typename
        assert "insert has more expressions than target columns" in str(exc_info.value).lower()

        with pytest.raises(redshift_connector.error.ProgrammingError) as exc_info:
            wr.redshift.to_sql(**to_sql_kwargs, use_column_names=True)
        assert "ProgrammingError" == exc_info.typename
        assert 'column "abc" of relation' in str(exc_info.value).lower()


def test_add_new_columns_case_sensitive(
    path: str, redshift_table: str, redshift_con: redshift_connector.Connection, databases_parameters: dict[str, Any]
) -> None:
    schema = "public"
    df = pd.DataFrame({"foo": ["a", "b", "c"]})

    # Create table
    wr.redshift.to_sql(df=df, con=redshift_con, table=redshift_table, schema=schema, add_new_columns=True)

    # Set enable_case_sensitive_identifier to False (default value)
    with redshift_con.cursor() as cursor:
        cursor.execute("SET enable_case_sensitive_identifier TO off;")
    redshift_con.commit()

    df["Boo"] = ["f", "g", "h"]
    wr.redshift.to_sql(df=df, con=redshift_con, table=redshift_table, schema=schema, add_new_columns=True)
    df2 = wr.redshift.read_sql_query(sql=f"SELECT * FROM {schema}.{redshift_table}", con=redshift_con)

    # Since 'enable_case_sensitive_identifier' is set to False, the column 'Boo' is automatically written as 'boo' by
    # Redshift
    assert df2.columns.tolist() == [x.lower() for x in df.columns]
    assert "boo" in df2.columns

    # Trying to add a new column 'BOO' causes an exception because Redshift attempts to lowercase it, resulting in a
    # columns mismatch between the DataFrame and the table schema
    df["BOO"] = ["j", "k", "l"]
    with pytest.raises(redshift_connector.error.ProgrammingError) as exc_info:
        wr.redshift.to_sql(df=df, con=redshift_con, table=redshift_table, schema=schema, add_new_columns=True)
    assert "insert has more expressions than target columns" in str(exc_info.value).lower()

    # Enable enable_case_sensitive_identifier
    with redshift_con.cursor() as cursor:
        cursor.execute("SET enable_case_sensitive_identifier TO on;")
        redshift_con.commit()
        wr.redshift.to_sql(df=df, con=redshift_con, table=redshift_table, schema=schema, add_new_columns=True)
        cursor.execute("RESET enable_case_sensitive_identifier;")
        redshift_con.commit()

    # Ensure that the new uppercase columns have been added correctly
    df2 = wr.redshift.read_sql_query(sql=f"SELECT * FROM {schema}.{redshift_table}", con=redshift_con)
    expected_columns = list(sorted(df.columns.tolist() + ["boo"]))
    assert expected_columns == list(sorted(df2.columns.tolist()))
