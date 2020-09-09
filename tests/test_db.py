import logging
import random
import string
from decimal import Decimal

import boto3
import pandas as pd
import pyarrow as pa
import pytest
import sqlalchemy

import awswrangler as wr

from ._utils import dt, ensure_data_types, ensure_data_types_category, get_df, get_df_category, ts

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


@pytest.mark.parametrize("db_type", ["mysql", "redshift", "postgresql"])
def test_sql(redshift_table, postgresql_table, mysql_table, databases_parameters, db_type):
    if db_type == "postgresql":
        table = postgresql_table
    elif db_type == "mysql":
        table = mysql_table
    else:
        table = redshift_table
    df = get_df()
    if db_type == "redshift":
        df.drop(["binary"], axis=1, inplace=True)
    engine = wr.catalog.get_engine(connection=f"aws-data-wrangler-{db_type}", echo=False)
    index = True if engine.name == "redshift" else False
    wr.db.to_sql(
        df=df,
        con=engine,
        name=table,
        schema=databases_parameters[db_type]["schema"],
        if_exists="replace",
        index=index,
        index_label=None,
        chunksize=None,
        method=None,
        dtype={"iint32": sqlalchemy.types.Integer},
    )
    df = wr.db.read_sql_query(sql=f"SELECT * FROM {databases_parameters[db_type]['schema']}.{table}", con=engine)
    ensure_data_types(df, has_list=False)
    engine = wr.db.get_engine(
        db_type=db_type,
        host=databases_parameters[db_type]["host"],
        port=databases_parameters[db_type]["port"],
        database=databases_parameters[db_type]["database"],
        user=databases_parameters["user"],
        password=databases_parameters["password"],
        echo=False,
    )
    dfs = wr.db.read_sql_query(
        sql=f"SELECT * FROM {databases_parameters[db_type]['schema']}.{table}",
        con=engine,
        chunksize=1,
        dtype={
            "iint8": pa.int8(),
            "iint16": pa.int16(),
            "iint32": pa.int32(),
            "iint64": pa.int64(),
            "float": pa.float32(),
            "double": pa.float64(),
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
    if db_type != "redshift":
        account_id = boto3.client("sts").get_caller_identity().get("Account")
        engine = wr.catalog.get_engine(connection=f"aws-data-wrangler-{db_type}", catalog_id=account_id)
        wr.db.to_sql(
            df=pd.DataFrame({"col0": [1, 2, 3]}, dtype="Int32"),
            con=engine,
            name=table,
            schema=databases_parameters[db_type]["schema"],
            if_exists="replace",
            index=True,
            index_label="index",
        )
        schema = None
        if db_type == "postgresql":
            schema = databases_parameters[db_type]["schema"]
        df = wr.db.read_sql_table(con=engine, table=table, schema=schema, index_col="index")
        assert df.shape == (3, 1)


def test_redshift_temp_engine(databases_parameters):
    engine = wr.db.get_redshift_temp_engine(
        cluster_identifier=databases_parameters["redshift"]["identifier"], user="test"
    )
    with engine.connect() as con:
        cursor = con.execute("SELECT 1")
        assert cursor.fetchall()[0][0] == 1


def test_redshift_temp_engine2(databases_parameters):
    engine = wr.db.get_redshift_temp_engine(
        cluster_identifier=databases_parameters["redshift"]["identifier"], user="john_doe", duration=900, db_groups=[]
    )
    with engine.connect() as con:
        cursor = con.execute("SELECT 1")
        assert cursor.fetchall()[0][0] == 1


def test_postgresql_param():
    engine = wr.catalog.get_engine(connection="aws-data-wrangler-postgresql")
    df = wr.db.read_sql_query(sql="SELECT %(value)s as col0", con=engine, params={"value": 1})
    assert df["col0"].iloc[0] == 1
    df = wr.db.read_sql_query(sql="SELECT %s as col0", con=engine, params=[1])
    assert df["col0"].iloc[0] == 1


def test_redshift_copy_unload(path, redshift_table, databases_parameters):
    df = get_df().drop(["iint8", "binary"], axis=1, inplace=False)
    engine = wr.catalog.get_engine(connection="aws-data-wrangler-redshift")
    wr.db.copy_to_redshift(
        df=df,
        path=path,
        con=engine,
        schema="public",
        table=redshift_table,
        mode="overwrite",
        iam_role=databases_parameters["redshift"]["role"],
    )
    df2 = wr.db.unload_redshift(
        sql=f"SELECT * FROM public.{redshift_table}",
        con=engine,
        iam_role=databases_parameters["redshift"]["role"],
        path=path,
        keep_files=False,
    )
    assert len(df2.index) == 3
    ensure_data_types(df=df2, has_list=False)
    wr.db.copy_to_redshift(
        df=df,
        path=path,
        con=engine,
        schema="public",
        table=redshift_table,
        mode="append",
        iam_role=databases_parameters["redshift"]["role"],
    )
    df2 = wr.db.unload_redshift(
        sql=f"SELECT * FROM public.{redshift_table}",
        con=engine,
        iam_role=databases_parameters["redshift"]["role"],
        path=path,
        keep_files=False,
    )
    assert len(df2.index) == 6
    ensure_data_types(df=df2, has_list=False)
    dfs = wr.db.unload_redshift(
        sql=f"SELECT * FROM public.{redshift_table}",
        con=engine,
        iam_role=databases_parameters["redshift"]["role"],
        path=path,
        keep_files=False,
        chunked=True,
    )
    for chunk in dfs:
        ensure_data_types(df=chunk, has_list=False)


def test_redshift_copy_upsert(path, redshift_table, databases_parameters):
    engine = wr.catalog.get_engine(connection="aws-data-wrangler-redshift")
    df = pd.DataFrame({"id": list((range(1_000))), "val": list(["foo" if i % 2 == 0 else "boo" for i in range(1_000)])})
    df3 = pd.DataFrame(
        {"id": list((range(1_000, 1_500))), "val": list(["foo" if i % 2 == 0 else "boo" for i in range(500)])}
    )

    # CREATE
    path = f"{path}upsert/test_redshift_copy_upsert/"
    wr.db.copy_to_redshift(
        df=df,
        path=path,
        con=engine,
        schema="public",
        table=redshift_table,
        mode="overwrite",
        index=False,
        primary_keys=["id"],
        iam_role=databases_parameters["redshift"]["role"],
    )
    path = f"{path}upsert/test_redshift_copy_upsert2/"
    df2 = wr.db.unload_redshift(
        sql=f"SELECT * FROM public.{redshift_table}",
        con=engine,
        iam_role=databases_parameters["redshift"]["role"],
        path=path,
        keep_files=False,
    )
    assert len(df.index) == len(df2.index)
    assert len(df.columns) == len(df2.columns)

    # UPSERT
    path = f"{path}upsert/test_redshift_copy_upsert3/"
    wr.db.copy_to_redshift(
        df=df3,
        path=path,
        con=engine,
        schema="public",
        table=redshift_table,
        mode="upsert",
        index=False,
        primary_keys=["id"],
        iam_role=databases_parameters["redshift"]["role"],
    )
    path = f"{path}upsert/test_redshift_copy_upsert4/"
    df4 = wr.db.unload_redshift(
        sql=f"SELECT * FROM public.{redshift_table}",
        con=engine,
        iam_role=databases_parameters["redshift"]["role"],
        path=path,
        keep_files=False,
    )
    assert len(df.index) + len(df3.index) == len(df4.index)
    assert len(df.columns) == len(df4.columns)

    # UPSERT 2
    wr.db.copy_to_redshift(
        df=df3,
        path=path,
        con=engine,
        schema="public",
        table=redshift_table,
        mode="upsert",
        index=False,
        iam_role=databases_parameters["redshift"]["role"],
    )
    path = f"{path}upsert/test_redshift_copy_upsert4/"
    df4 = wr.db.unload_redshift(
        sql=f"SELECT * FROM public.{redshift_table}",
        con=engine,
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
def test_redshift_exceptions(path, redshift_table, databases_parameters, diststyle, distkey, sortstyle, sortkey, exc):
    df = pd.DataFrame({"id": [1], "name": "joe"})
    engine = wr.catalog.get_engine(connection="aws-data-wrangler-redshift")
    with pytest.raises(exc):
        wr.db.copy_to_redshift(
            df=df,
            path=path,
            con=engine,
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
    wr.s3.delete_objects(path=path)


def test_redshift_spectrum(path, redshift_table, glue_database, redshift_external_schema):
    df = pd.DataFrame({"id": [1, 2, 3, 4, 5], "col_str": ["foo", None, "bar", None, "xoo"], "par_int": [0, 1, 0, 1, 1]})
    paths = wr.s3.to_parquet(
        df=df,
        path=path,
        database=glue_database,
        table=redshift_table,
        mode="overwrite",
        index=False,
        dataset=True,
        partition_cols=["par_int"],
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    engine = wr.catalog.get_engine(connection="aws-data-wrangler-redshift")
    with engine.connect() as con:
        cursor = con.execute(f"SELECT * FROM {redshift_external_schema}.{redshift_table}")
        rows = cursor.fetchall()
        assert len(rows) == len(df.index)
        for row in rows:
            assert len(row) == len(df.columns)
    wr.s3.delete_objects(path=path)
    assert wr.catalog.delete_table_if_exists(database=glue_database, table=redshift_table) is True


def test_redshift_category(path, redshift_table, databases_parameters):
    df = get_df_category().drop(["binary"], axis=1, inplace=False)
    engine = wr.catalog.get_engine(connection="aws-data-wrangler-redshift")
    wr.db.copy_to_redshift(
        df=df,
        path=path,
        con=engine,
        schema="public",
        table=redshift_table,
        mode="overwrite",
        iam_role=databases_parameters["redshift"]["role"],
    )
    df2 = wr.db.unload_redshift(
        sql=f"SELECT * FROM public.{redshift_table}",
        con=engine,
        iam_role=databases_parameters["redshift"]["role"],
        path=path,
        keep_files=False,
        categories=df.columns,
    )
    ensure_data_types_category(df2)
    dfs = wr.db.unload_redshift(
        sql=f"SELECT * FROM public.{redshift_table}",
        con=engine,
        iam_role=databases_parameters["redshift"]["role"],
        path=path,
        keep_files=False,
        categories=df.columns,
        chunked=True,
    )
    for df2 in dfs:
        ensure_data_types_category(df2)
    wr.s3.delete_objects(path=path)


def test_redshift_unload_extras(bucket, path, redshift_table, databases_parameters, kms_key_id):
    table = redshift_table
    schema = databases_parameters["redshift"]["schema"]
    wr.s3.delete_objects(path=path)
    engine = wr.catalog.get_engine(connection="aws-data-wrangler-redshift")
    df = pd.DataFrame({"id": [1, 2], "name": ["foo", "boo"]})
    wr.db.to_sql(df=df, con=engine, name=table, schema=schema, if_exists="replace", index=False)
    paths = wr.db.unload_redshift_to_files(
        sql=f"SELECT * FROM {schema}.{table}",
        path=path,
        con=engine,
        iam_role=databases_parameters["redshift"]["role"],
        region=wr.s3.get_bucket_region(bucket),
        max_file_size=5.0,
        kms_key_id=kms_key_id,
        partition_cols=["name"],
    )
    wr.s3.wait_objects_exist(paths=paths)
    df = wr.s3.read_parquet(path=path, dataset=True)
    assert len(df.index) == 2
    assert len(df.columns) == 2
    wr.s3.delete_objects(path=path)
    df = wr.db.unload_redshift(
        sql=f"SELECT * FROM {schema}.{table}",
        con=engine,
        iam_role=databases_parameters["redshift"]["role"],
        path=path,
        keep_files=False,
        region=wr.s3.get_bucket_region(bucket),
        max_file_size=5.0,
        kms_key_id=kms_key_id,
    )
    assert len(df.index) == 2
    assert len(df.columns) == 2
    wr.s3.delete_objects(path=path)


@pytest.mark.parametrize("db_type", ["mysql", "redshift", "postgresql"])
def test_to_sql_cast(redshift_table, postgresql_table, mysql_table, databases_parameters, db_type):
    if db_type == "postgresql":
        table = postgresql_table
    elif db_type == "mysql":
        table = mysql_table
    else:
        table = redshift_table
    schema = databases_parameters[db_type]["schema"]
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
    engine = wr.catalog.get_engine(connection=f"aws-data-wrangler-{db_type}")
    wr.db.to_sql(
        df=df,
        con=engine,
        name=table,
        schema=schema,
        if_exists="replace",
        index=False,
        index_label=None,
        chunksize=None,
        method=None,
        dtype={"col": sqlalchemy.types.VARCHAR(length=1_024)},
    )
    df2 = wr.db.read_sql_query(sql=f"SELECT * FROM {schema}.{table}", con=engine)
    assert df.equals(df2)


def test_uuid(postgresql_table, databases_parameters):
    table = postgresql_table
    schema = databases_parameters["postgresql"]["schema"]
    engine = wr.catalog.get_engine(connection="aws-data-wrangler-postgresql")
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "uuid": [
                "ec0f0482-8d3b-11ea-8b27-8c859043dd95",
                "f56ff7c0-8d3b-11ea-be94-8c859043dd95",
                "fa043e90-8d3b-11ea-b7e7-8c859043dd95",
            ],
        }
    )
    wr.db.to_sql(
        df=df,
        con=engine,
        name=table,
        schema=schema,
        if_exists="replace",
        index=False,
        index_label=None,
        chunksize=None,
        method=None,
        dtype={"uuid": sqlalchemy.dialects.postgresql.UUID},
    )
    df2 = wr.db.read_sql_table(table=table, schema=schema, con=engine)
    df["id"] = df["id"].astype("Int64")
    df["uuid"] = df["uuid"].astype("string")
    assert df.equals(df2)


@pytest.mark.parametrize("db_type", ["mysql", "redshift", "postgresql"])
def test_null(redshift_table, postgresql_table, mysql_table, databases_parameters, db_type):
    if db_type == "postgresql":
        table = postgresql_table
    elif db_type == "mysql":
        table = mysql_table
    else:
        table = redshift_table
    schema = databases_parameters[db_type]["schema"]
    engine = wr.catalog.get_engine(connection=f"aws-data-wrangler-{db_type}")
    df = pd.DataFrame({"id": [1, 2, 3], "nothing": [None, None, None]})
    wr.db.to_sql(
        df=df,
        con=engine,
        name=table,
        schema=schema,
        if_exists="replace",
        index=False,
        index_label=None,
        chunksize=None,
        method=None,
        dtype={"nothing": sqlalchemy.types.Integer},
    )
    wr.db.to_sql(
        df=df,
        con=engine,
        name=table,
        schema=schema,
        if_exists="append",
        index=False,
        index_label=None,
        chunksize=None,
        method=None,
    )
    df2 = wr.db.read_sql_table(table=table, schema=schema, con=engine)
    df["id"] = df["id"].astype("Int64")
    assert pd.concat(objs=[df, df], ignore_index=True).equals(df2)


def test_redshift_spectrum_long_string(path, glue_table, glue_database, redshift_external_schema):
    df = pd.DataFrame(
        {
            "id": [1, 2],
            "col_str": [
                "".join(random.choice(string.ascii_letters) for _ in range(300)),
                "".join(random.choice(string.ascii_letters) for _ in range(300)),
            ],
        }
    )
    paths = wr.s3.to_parquet(
        df=df, path=path, database=glue_database, table=glue_table, mode="overwrite", index=False, dataset=True
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)
    engine = wr.catalog.get_engine(connection="aws-data-wrangler-redshift")
    with engine.connect() as con:
        cursor = con.execute(f"SELECT * FROM {redshift_external_schema}.{glue_table}")
        rows = cursor.fetchall()
        assert len(rows) == len(df.index)
        for row in rows:
            assert len(row) == len(df.columns)


def test_redshift_copy_unload_long_string(path, redshift_table, databases_parameters):
    df = pd.DataFrame(
        {
            "id": [1, 2],
            "col_str": [
                "".join(random.choice(string.ascii_letters) for _ in range(300)),
                "".join(random.choice(string.ascii_letters) for _ in range(300)),
            ],
        }
    )
    engine = wr.catalog.get_engine(connection="aws-data-wrangler-redshift")
    wr.db.copy_to_redshift(
        df=df,
        path=path,
        con=engine,
        schema="public",
        table=redshift_table,
        mode="overwrite",
        varchar_lengths={"col_str": 300},
        iam_role=databases_parameters["redshift"]["role"],
    )
    df2 = wr.db.unload_redshift(
        sql=f"SELECT * FROM public.{redshift_table}",
        con=engine,
        iam_role=databases_parameters["redshift"]["role"],
        path=path,
        keep_files=False,
    )
    assert df2.shape == (2, 2)


def test_spectrum_decimal_cast(path, path2, glue_table, glue_database, redshift_external_schema, databases_parameters):
    df = pd.DataFrame(
        {"c0": [1, 2], "c1": [1, None], "c2": [2.22222, None], "c3": ["3.33333", None], "c4": [None, None]}
    )
    paths = wr.s3.to_parquet(
        df=df,
        path=path,
        database=glue_database,
        table=glue_table,
        dataset=True,
        dtype={"c1": "decimal(11,5)", "c2": "decimal(11,5)", "c3": "decimal(11,5)", "c4": "decimal(11,5)"},
    )["paths"]
    wr.s3.wait_objects_exist(paths=paths, use_threads=False)

    # Athena
    df2 = wr.athena.read_sql_table(table=glue_table, database=glue_database)
    assert df2.shape == (2, 5)
    df2 = df2.drop(df2[df2.c0 == 2].index)
    assert df2.c1[0] == Decimal((0, (1, 0, 0, 0, 0, 0), -5))
    assert df2.c2[0] == Decimal((0, (2, 2, 2, 2, 2, 2), -5))
    assert df2.c3[0] == Decimal((0, (3, 3, 3, 3, 3, 3), -5))
    assert df2.c4[0] is None

    # Redshift Spectrum
    engine = wr.catalog.get_engine(connection="aws-data-wrangler-redshift")
    df2 = wr.db.read_sql_table(table=glue_table, schema=redshift_external_schema, con=engine)
    assert df2.shape == (2, 5)
    df2 = df2.drop(df2[df2.c0 == 2].index)
    assert df2.c1[0] == Decimal((0, (1, 0, 0, 0, 0, 0), -5))
    assert df2.c2[0] == Decimal((0, (2, 2, 2, 2, 2, 2), -5))
    assert df2.c3[0] == Decimal((0, (3, 3, 3, 3, 3, 3), -5))
    assert df2.c4[0] is None

    # Redshift Spectrum Unload
    engine = wr.catalog.get_engine(connection="aws-data-wrangler-redshift")
    df2 = wr.db.unload_redshift(
        sql=f"SELECT * FROM {redshift_external_schema}.{glue_table}",
        con=engine,
        iam_role=databases_parameters["redshift"]["role"],
        path=path2,
    )
    assert df2.shape == (2, 5)
    df2 = df2.drop(df2[df2.c0 == 2].index)
    assert df2.c1[0] == Decimal((0, (1, 0, 0, 0, 0, 0), -5))
    assert df2.c2[0] == Decimal((0, (2, 2, 2, 2, 2, 2), -5))
    assert df2.c3[0] == Decimal((0, (3, 3, 3, 3, 3, 3), -5))
    assert df2.c4[0] is None


def test_postgresql_kwargs():
    engine = wr.catalog.get_engine(connection="aws-data-wrangler-postgresql")
    sql = """
    create or replace function sleep (integer) returns time as '
        declare
            seconds alias for $1;
            later time;
            thetime time;
        begin
            thetime := timeofday()::timestamp;
            later := thetime + (seconds::text || '' seconds'')::interval;
            loop
                if thetime >= later then
                    exit;
                else
                    thetime := timeofday()::timestamp;
                end if;
            end loop;
            return later;
        end;
    ' language plpgsql;
    """
    with engine.connect() as con:
        con.execute(sql)
    engine2 = wr.catalog.get_engine(
        connection="aws-data-wrangler-postgresql", connect_args={"options": "-c statement_timeout=5s"}
    )
    df = wr.db.read_sql_query(sql="SELECT sleep(2)", con=engine2)
    assert df.shape == (1, 1)
    with pytest.raises(sqlalchemy.exc.OperationalError) as ex:
        wr.db.read_sql_query(sql="SELECT sleep(6)", con=engine2)
    assert "canceling statement due to statement timeout" in str(ex)


@pytest.mark.parametrize(
    "s3_additional_kwargs",
    [None, {"ServerSideEncryption": "AES256"}, {"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": None}],
)
@pytest.mark.parametrize("use_threads", [True, False])
def test_redshift_copy_unload_kms(
    path, redshift_table, databases_parameters, kms_key_id, use_threads, s3_additional_kwargs
):
    df = pd.DataFrame({"id": [1, 2, 3]})
    if s3_additional_kwargs is not None and "SSEKMSKeyId" in s3_additional_kwargs:
        s3_additional_kwargs["SSEKMSKeyId"] = kms_key_id
    engine = wr.catalog.get_engine(connection="aws-data-wrangler-redshift")
    wr.db.copy_to_redshift(
        df=df,
        path=path,
        con=engine,
        schema="public",
        table=redshift_table,
        mode="overwrite",
        iam_role=databases_parameters["redshift"]["role"],
        use_threads=use_threads,
        s3_additional_kwargs=s3_additional_kwargs,
    )
    df2 = wr.db.unload_redshift(
        sql=f"SELECT * FROM public.{redshift_table}",
        con=engine,
        iam_role=databases_parameters["redshift"]["role"],
        path=path,
        keep_files=False,
        use_threads=use_threads,
        s3_additional_kwargs=s3_additional_kwargs,
    )
    assert df.shape == df2.shape


@pytest.mark.parametrize("parquet_infer_sampling", [1.0, 0.00000000000001])
@pytest.mark.parametrize("use_threads", [True, False])
def test_redshift_copy_extras(path, redshift_table, databases_parameters, use_threads, parquet_infer_sampling):
    df = pd.DataFrame(
        {
            "int16": [1, None, 2],
            "int32": [1, None, 2],
            "int64": [1, None, 2],
            "float": [0.0, None, 1.1],
            "double": [0.0, None, 1.1],
            "decimal": [Decimal((0, (1, 9, 9), -2)), None, Decimal((0, (1, 9, 0), -2))],
            "string": ["foo", None, "boo"],
            "date": [dt("2020-01-01"), None, dt("2020-01-02")],
            "timestamp": [ts("2020-01-01 00:00:00.0"), None, ts("2020-01-02 00:00:01.0")],
            "bool": [True, None, False],
        }
    )
    df["int16"] = df["int16"].astype("Int16")
    df["int32"] = df["int32"].astype("Int32")
    df["int64"] = df["int64"].astype("Int64")
    df["float"] = df["float"].astype("float32")
    df["string"] = df["string"].astype("string")
    paths = []
    num = 3
    for i in range(num):
        p = f"{path}data/{i}.parquet"
        wr.s3.to_parquet(df, p, use_threads=use_threads)
        paths.append(p)
    engine = wr.catalog.get_engine(connection="aws-data-wrangler-redshift")
    wr.db.copy_files_to_redshift(
        path=paths,
        con=engine,
        schema="public",
        table=redshift_table,
        mode="overwrite",
        iam_role=databases_parameters["redshift"]["role"],
        manifest_directory=f"{path}manifest/",
        use_threads=use_threads,
        parquet_infer_sampling=parquet_infer_sampling,
    )
    df2 = wr.db.read_sql_table(schema="public", table=redshift_table, con=engine)
    assert len(df.columns) == len(df2.columns)
    assert len(df.index) * num == len(df2.index)
    assert df.int16.sum() * num == df2.int16.sum()
    assert df.int32.sum() * num == df2.int32.sum()
    assert df.int64.sum() * num == df2.int64.sum()
