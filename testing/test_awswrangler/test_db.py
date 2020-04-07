import logging
import random

import boto3
import pandas as pd
import pyarrow as pa
import pytest
import sqlalchemy

import awswrangler as wr

from ._utils import ensure_data_types, get_df

logging.basicConfig(level=logging.INFO, format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)
logging.getLogger("botocore.credentials").setLevel(logging.CRITICAL)


@pytest.fixture(scope="module")
def cloudformation_outputs():
    response = boto3.client("cloudformation").describe_stacks(StackName="aws-data-wrangler-test")
    outputs = {}
    for output in response.get("Stacks")[0].get("Outputs"):
        outputs[output.get("OutputKey")] = output.get("OutputValue")
    yield outputs


@pytest.fixture(scope="module")
def bucket(cloudformation_outputs):
    if "BucketName" in cloudformation_outputs:
        bucket = cloudformation_outputs["BucketName"]
    else:
        raise Exception("You must deploy/update the test infrastructure (CloudFormation)")
    yield bucket


@pytest.fixture(scope="module")
def parameters(cloudformation_outputs):
    parameters = dict(postgresql={}, mysql={}, redshift={})
    parameters["postgresql"]["host"] = cloudformation_outputs["PostgresqlAddress"]
    parameters["postgresql"]["port"] = 3306
    parameters["postgresql"]["schema"] = "public"
    parameters["postgresql"]["database"] = "postgres"
    parameters["mysql"]["host"] = cloudformation_outputs["MysqlAddress"]
    parameters["mysql"]["port"] = 3306
    parameters["mysql"]["schema"] = "test"
    parameters["mysql"]["database"] = "test"
    parameters["redshift"]["host"] = cloudformation_outputs["RedshiftAddress"]
    parameters["redshift"]["port"] = cloudformation_outputs["RedshiftPort"]
    parameters["redshift"]["identifier"] = cloudformation_outputs["RedshiftIdentifier"]
    parameters["redshift"]["schema"] = "public"
    parameters["redshift"]["database"] = "test"
    parameters["redshift"]["role"] = cloudformation_outputs["RedshiftRole"]
    parameters["password"] = cloudformation_outputs["DatabasesPassword"]
    parameters["user"] = "test"
    yield parameters


@pytest.mark.parametrize("db_type", ["mysql", "redshift", "postgresql"])
def test_sql(parameters, db_type):
    df = get_df()
    if db_type == "redshift":
        df.drop(["binary"], axis=1, inplace=True)
    engine = wr.catalog.get_engine(connection=f"aws-data-wrangler-{db_type}")
    wr.db.to_sql(
        df=df,
        con=engine,
        name="test_sql",
        schema=parameters[db_type]["schema"],
        if_exists="replace",
        index=False,
        index_label=None,
        chunksize=None,
        method=None,
        dtype={"iint32": sqlalchemy.types.Integer},
    )
    df = wr.db.read_sql_query(sql=f"SELECT * FROM {parameters[db_type]['schema']}.test_sql", con=engine)
    ensure_data_types(df, has_list=False)
    engine = wr.db.get_engine(
        db_type=db_type,
        host=parameters[db_type]["host"],
        port=parameters[db_type]["port"],
        database=parameters[db_type]["database"],
        user=parameters["user"],
        password=parameters["password"],
    )
    dfs = wr.db.read_sql_query(
        sql=f"SELECT * FROM {parameters[db_type]['schema']}.test_sql",
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
            name="test_sql",
            schema=parameters[db_type]["schema"],
            if_exists="replace",
            index=True,
            index_label="index",
        )
        schema = None
        if db_type == "postgresql":
            schema = parameters[db_type]["schema"]
        df = wr.db.read_sql_table(con=engine, table="test_sql", schema=schema, index_col="index")
        assert len(df.index) == 3
        assert len(df.columns) == 1


def test_redshift_temp_engine(parameters):
    engine = wr.db.get_redshift_temp_engine(cluster_identifier=parameters["redshift"]["identifier"], user="test")
    with engine.connect() as con:
        cursor = con.execute("SELECT 1")
        assert cursor.fetchall()[0][0] == 1


def test_postgresql_param():
    engine = wr.catalog.get_engine(connection=f"aws-data-wrangler-postgresql")
    df = wr.db.read_sql_query(sql="SELECT %(value)s as col0", con=engine, params={"value": 1})
    assert df["col0"].iloc[0] == 1
    df = wr.db.read_sql_query(sql="SELECT %s as col0", con=engine, params=[1])
    assert df["col0"].iloc[0] == 1


def test_redshift_copy_unload(bucket, parameters):
    path = f"s3://{bucket}/test_redshift_copy/"
    df = get_df().drop(["iint8", "binary"], axis=1, inplace=False)
    engine = wr.catalog.get_engine(connection=f"aws-data-wrangler-redshift")
    wr.db.copy_to_redshift(
        df=df,
        path=path,
        con=engine,
        schema="public",
        table="test_redshift_copy",
        mode="overwrite",
        iam_role=parameters["redshift"]["role"],
    )
    df2 = wr.db.unload_redshift(
        sql="SELECT * FROM public.test_redshift_copy",
        con=engine,
        iam_role=parameters["redshift"]["role"],
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
        table="test_redshift_copy",
        mode="append",
        iam_role=parameters["redshift"]["role"],
    )
    df2 = wr.db.unload_redshift(
        sql="SELECT * FROM public.test_redshift_copy",
        con=engine,
        iam_role=parameters["redshift"]["role"],
        path=path,
        keep_files=False,
    )
    assert len(df2.index) == 6
    ensure_data_types(df=df2, has_list=False)
    dfs = wr.db.unload_redshift(
        sql="SELECT * FROM public.test_redshift_copy",
        con=engine,
        iam_role=parameters["redshift"]["role"],
        path=path,
        keep_files=False,
        chunked=True,
    )
    for chunk in dfs:
        ensure_data_types(df=chunk, has_list=False)


def test_redshift_copy_upsert(bucket, parameters):
    engine = wr.catalog.get_engine(connection=f"aws-data-wrangler-redshift")
    df = pd.DataFrame({"id": list((range(1_000))), "val": list(["foo" if i % 2 == 0 else "boo" for i in range(1_000)])})
    df3 = pd.DataFrame(
        {"id": list((range(1_000, 1_500))), "val": list(["foo" if i % 2 == 0 else "boo" for i in range(500)])}
    )

    # CREATE
    path = f"s3://{bucket}/upsert/test_redshift_copy_upsert/"
    wr.db.copy_to_redshift(
        df=df,
        path=path,
        con=engine,
        schema="public",
        table="test_redshift_copy_upsert",
        mode="overwrite",
        index=False,
        primary_keys=["id"],
        iam_role=parameters["redshift"]["role"],
    )
    path = f"s3://{bucket}/upsert/test_redshift_copy_upsert2/"
    df2 = wr.db.unload_redshift(
        sql="SELECT * FROM public.test_redshift_copy_upsert",
        con=engine,
        iam_role=parameters["redshift"]["role"],
        path=path,
        keep_files=False,
    )
    assert len(df.index) == len(df2.index)
    assert len(df.columns) == len(df2.columns)

    # UPSERT
    path = f"s3://{bucket}/upsert/test_redshift_copy_upsert3/"
    wr.db.copy_to_redshift(
        df=df3,
        path=path,
        con=engine,
        schema="public",
        table="test_redshift_copy_upsert",
        mode="upsert",
        index=False,
        primary_keys=["id"],
        iam_role=parameters["redshift"]["role"],
    )
    path = f"s3://{bucket}/upsert/test_redshift_copy_upsert4/"
    df4 = wr.db.unload_redshift(
        sql="SELECT * FROM public.test_redshift_copy_upsert",
        con=engine,
        iam_role=parameters["redshift"]["role"],
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
        table="test_redshift_copy_upsert",
        mode="upsert",
        index=False,
        iam_role=parameters["redshift"]["role"],
    )
    path = f"s3://{bucket}/upsert/test_redshift_copy_upsert4/"
    df4 = wr.db.unload_redshift(
        sql="SELECT * FROM public.test_redshift_copy_upsert",
        con=engine,
        iam_role=parameters["redshift"]["role"],
        path=path,
        keep_files=False,
    )
    assert len(df.index) + len(df3.index) == len(df4.index)
    assert len(df.columns) == len(df4.columns)

    # CLEANING
    wr.s3.delete_objects(path=f"s3://{bucket}/upsert/")


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
def test_redshift_exceptions(bucket, parameters, diststyle, distkey, sortstyle, sortkey, exc):
    df = pd.DataFrame({"id": [1], "name": "joe"})
    engine = wr.catalog.get_engine(connection=f"aws-data-wrangler-redshift")
    path = f"s3://{bucket}/test_redshift_exceptions_{random.randint(0, 1_000_000)}/"
    with pytest.raises(exc):
        wr.db.copy_to_redshift(
            df=df,
            path=path,
            con=engine,
            schema="public",
            table="test_redshift_exceptions",
            mode="overwrite",
            diststyle=diststyle,
            distkey=distkey,
            sortstyle=sortstyle,
            sortkey=sortkey,
            iam_role=parameters["redshift"]["role"],
            index=False,
        )
    wr.s3.delete_objects(path=path)
