import logging
import random
import string
from decimal import Decimal

import boto3
import pandas as pd
import pyarrow as pa
import pytest
import redshift_connector
from redshift_connector.error import ProgrammingError

import awswrangler as wr
from awswrangler import _utils

from ._utils import dt, ensure_data_types, ensure_data_types_category, get_df, get_df_category, ts

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


@pytest.fixture(scope="function")
def redshift_con():
    con = wr.redshift.connect("aws-data-wrangler-redshift")
    yield con
    con.close()


def test_connection():
    wr.redshift.connect("aws-data-wrangler-redshift", timeout=10).close()


def test_read_sql_query_simple(databases_parameters):
    con = redshift_connector.connect(
        host=databases_parameters["redshift"]["host"],
        port=int(databases_parameters["redshift"]["port"]),
        database=databases_parameters["redshift"]["database"],
        user=databases_parameters["user"],
        password=databases_parameters["password"],
    )
    df = wr.redshift.read_sql_query("SELECT 1", con=con)
    con.close()
    assert df.shape == (1, 1)


def test_to_sql_simple(redshift_table, redshift_con):
    df = pd.DataFrame({"c0": [1, 2, 3], "c1": ["foo", "boo", "bar"]})
    wr.redshift.to_sql(df, redshift_con, redshift_table, "public", "overwrite", True)


def test_sql_types(redshift_table, redshift_con):
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


def test_connection_temp(databases_parameters):
    con = wr.redshift.connect_temp(cluster_identifier=databases_parameters["redshift"]["identifier"], user="test")
    with con.cursor() as cursor:
        cursor.execute("SELECT 1")
        assert cursor.fetchall()[0][0] == 1
    con.close()


def test_connection_temp2(databases_parameters):
    con = wr.redshift.connect_temp(
        cluster_identifier=databases_parameters["redshift"]["identifier"], user="john_doe", duration=900, db_groups=[]
    )
    with con.cursor() as cursor:
        cursor.execute("SELECT 1")
        assert cursor.fetchall()[0][0] == 1
    con.close()


def test_copy_unload(path, redshift_table, redshift_con, databases_parameters):
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


def test_copy_upsert(path, redshift_table, redshift_con, databases_parameters):
    df = pd.DataFrame({"id": list((range(1_000))), "val": list(["foo" if i % 2 == 0 else "boo" for i in range(1_000)])})
    df3 = pd.DataFrame(
        {"id": list((range(1_000, 1_500))), "val": list(["foo" if i % 2 == 0 else "boo" for i in range(500)])}
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
        sql=f"SELECT * FROM public.{redshift_table}",
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
        sql=f"SELECT * FROM public.{redshift_table}",
        con=redshift_con,
        iam_role=databases_parameters["redshift"]["role"],
        path=path,
        keep_files=False,
    )
    assert len(df.index) + len(df3.index) == len(df4.index)
    assert len(df.columns) == len(df4.columns)

    # UPSERT 2
    wr.redshift.copy(
        df=df3,
        path=path,
        con=redshift_con,
        schema="public",
        table=redshift_table,
        mode="upsert",
        index=False,
        iam_role=databases_parameters["redshift"]["role"],
    )
    path = f"{path}upsert/test_redshift_copy_upsert4/"
    df4 = wr.redshift.unload(
        sql=f"SELECT * FROM public.{redshift_table}",
        con=redshift_con,
        iam_role=databases_parameters["redshift"]["role"],
        path=path,
        keep_files=False,
    )
    assert len(df.index) + len(df3.index) == len(df4.index)
    assert len(df.columns) == len(df4.columns)


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
    path, redshift_table, redshift_con, databases_parameters, diststyle, distkey, sortstyle, sortkey, exc
):
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


def test_spectrum(path, redshift_table, redshift_con, glue_database, redshift_external_schema):
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
    wr.catalog.delete_table_if_exists(database=glue_database, table=redshift_table)


def test_category(path, redshift_table, redshift_con, databases_parameters):
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
        categories=df.columns,
    )
    ensure_data_types_category(df2)
    dfs = wr.redshift.unload(
        sql=f"SELECT * FROM public.{redshift_table}",
        con=redshift_con,
        iam_role=databases_parameters["redshift"]["role"],
        path=path,
        keep_files=False,
        categories=df.columns,
        chunked=True,
    )
    for df2 in dfs:
        ensure_data_types_category(df2)


def test_unload_extras(bucket, path, redshift_table, redshift_con, databases_parameters, kms_key_id):
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


def test_to_sql_cast(redshift_table, redshift_con):
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


def test_null(redshift_table, redshift_con):
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
    assert pd.concat(objs=[df, df], ignore_index=True).equals(df2)


def test_spectrum_long_string(path, redshift_con, glue_table, glue_database, redshift_external_schema):
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


def test_copy_unload_long_string(path, redshift_table, redshift_con, databases_parameters):
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


@pytest.mark.xfail(raises=AttributeError)
def test_spectrum_decimal_cast(path, path2, glue_table, glue_database, redshift_external_schema, databases_parameters):
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
    df2 = df2.drop(df2[df2.c0 == 2].index)
    assert df2.c1[0] == Decimal((0, (1, 0, 0, 0, 0, 0), -5))
    assert df2.c2[0] == Decimal((0, (2, 2, 2, 2, 2, 2), -5))
    assert df2.c3[0] == Decimal((0, (3, 3, 3, 3, 3, 3), -5))
    assert df2.c4[0] is None

    # Redshift Spectrum
    con = wr.redshift.connect(connection="aws-data-wrangler-redshift")
    df2 = wr.redshift.read_sql_table(table=glue_table, schema=redshift_external_schema, con=con)
    assert df2.shape == (2, 5)
    df2 = df2.drop(df2[df2.c0 == 2].index)
    assert df2.c1[0] == Decimal((0, (1, 0, 0, 0, 0, 0), -5))
    assert df2.c2[0] == Decimal((0, (2, 2, 2, 2, 2, 2), -5))
    assert df2.c3[0] == Decimal((0, (3, 3, 3, 3, 3, 3), -5))
    assert df2.c4[0] is None
    con.close()

    # Redshift Spectrum Unload
    con = wr.redshift.connect(connection="aws-data-wrangler-redshift")
    df2 = wr.redshift.unload(
        sql=f"SELECT * FROM {redshift_external_schema}.{glue_table}",
        con=con,
        iam_role=databases_parameters["redshift"]["role"],
        path=path2,
    )
    assert df2.shape == (2, 5)
    df2 = df2.drop(df2[df2.c0 == 2].index)
    assert df2.c1[0] == Decimal((0, (1, 0, 0, 0, 0, 0), -5))
    assert df2.c2[0] == Decimal((0, (2, 2, 2, 2, 2, 2), -5))
    assert df2.c3[0] == Decimal((0, (3, 3, 3, 3, 3, 3), -5))
    assert df2.c4[0] is None
    con.close()


@pytest.mark.parametrize(
    "s3_additional_kwargs",
    [None, {"ServerSideEncryption": "AES256"}, {"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": None}],
)
@pytest.mark.parametrize("use_threads", [True, False])
def test_copy_unload_kms(
    path, redshift_table, redshift_con, databases_parameters, kms_key_id, use_threads, s3_additional_kwargs
):
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
        use_threads=use_threads,
        s3_additional_kwargs=s3_additional_kwargs,
    )
    assert df.shape == df2.shape


@pytest.mark.parametrize("parquet_infer_sampling", [1.0, 0.00000000000001])
@pytest.mark.parametrize("use_threads", [True, False])
def test_copy_extras(path, redshift_table, redshift_con, databases_parameters, use_threads, parquet_infer_sampling):
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


def test_decimal_cast(redshift_table, redshift_con):
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


def test_upsert(redshift_table, redshift_con):
    df = pd.DataFrame({"id": list((range(10))), "val": list(["foo" if i % 2 == 0 else "boo" for i in range(10)])})
    df3 = pd.DataFrame({"id": list((range(10, 15))), "val": list(["foo" if i % 2 == 0 else "boo" for i in range(5)])})

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


def test_read_retry(redshift_con):
    try:
        wr.redshift.read_sql_query("ERROR", redshift_con)
    except:  # noqa
        pass
    df = wr.redshift.read_sql_query("SELECT 1", redshift_con)
    assert df.shape == (1, 1)


def test_table_name(redshift_con):
    df = pd.DataFrame({"col0": [1]})
    wr.redshift.to_sql(df, redshift_con, "Test Name", "public", mode="overwrite")
    df = wr.redshift.read_sql_table(schema="public", con=redshift_con, table="Test Name")
    assert df.shape == (1, 1)
    with redshift_con.cursor() as cursor:
        cursor.execute('DROP TABLE "Test Name"')
    redshift_con.commit()


def test_copy_from_files(path, redshift_table, redshift_con, databases_parameters):
    df = get_df_category().drop(["binary"], axis=1, inplace=False)
    wr.s3.to_parquet(df, f"{path}test.parquet")
    bucket, key = wr._utils.parse_path(f"{path}test.csv")
    boto3.client("s3").put_object(Body=b"", Bucket=bucket, Key=key)
    wr.redshift.copy_from_files(
        path=path,
        path_suffix=[".parquet"],
        con=redshift_con,
        table=redshift_table,
        schema="public",
        iam_role=databases_parameters["redshift"]["role"],
    )
    df2 = wr.redshift.read_sql_query(sql=f"SELECT count(*) AS counter FROM public.{redshift_table}", con=redshift_con)
    assert df2["counter"].iloc[0] == 3


def test_copy_from_files_ignore(path, redshift_table, redshift_con, databases_parameters):
    df = get_df_category().drop(["binary"], axis=1, inplace=False)
    wr.s3.to_parquet(df, f"{path}test.parquet")
    bucket, key = wr._utils.parse_path(f"{path}test.csv")
    boto3.client("s3").put_object(Body=b"", Bucket=bucket, Key=key)
    wr.redshift.copy_from_files(
        path=path,
        path_ignore_suffix=[".csv"],
        con=redshift_con,
        table=redshift_table,
        schema="public",
        iam_role=databases_parameters["redshift"]["role"],
    )
    df2 = wr.redshift.read_sql_query(sql=f"SELECT count(*) AS counter FROM public.{redshift_table}", con=redshift_con)
    assert df2["counter"].iloc[0] == 3


def test_copy_from_files_empty(path, redshift_table, redshift_con, databases_parameters):
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


def test_copy_dirty_path(path, redshift_table, redshift_con, databases_parameters):
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
def test_connect_secret_manager(dbname):
    con = wr.redshift.connect(secret_id="aws-data-wrangler/redshift", dbname=dbname)
    df = wr.redshift.read_sql_query("SELECT 1", con=con)
    con.close()
    assert df.shape == (1, 1)


def test_copy_unload_session(path, redshift_table, redshift_con):
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


def test_copy_unload_creds(path, redshift_table, redshift_con):
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


def test_column_length(path, redshift_table, redshift_con, databases_parameters):
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
    assert df2.equals(df)


def test_failed_keep_files(path, redshift_table, redshift_con, databases_parameters):
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


def test_insert_with_column_names(redshift_table, redshift_con):
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
def test_dfs_are_equal_for_different_chunksizes(redshift_table, redshift_con, chunksize):
    df = pd.DataFrame({"c0": [i for i in range(64)], "c1": ["foo" for _ in range(64)]})
    wr.redshift.to_sql(df=df, con=redshift_con, schema="public", table=redshift_table, chunksize=chunksize)

    df2 = wr.redshift.read_sql_table(con=redshift_con, schema="public", table=redshift_table)

    df["c0"] = df["c0"].astype("Int64")
    df["c1"] = df["c1"].astype("string")

    assert df.equals(df2)
