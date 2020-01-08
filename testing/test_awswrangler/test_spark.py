import logging
from datetime import datetime, date
from time import sleep

import pytest
import boto3
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, array, create_map, struct
from pyspark.sql.types import StructType, StructField, IntegerType, DateType,\
    TimestampType, StringType, FloatType, MapType, ArrayType

import awswrangler as wr
from awswrangler import Session

logging.basicConfig(level=logging.INFO, format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)


@pytest.fixture(scope="module")
def cloudformation_outputs():
    response = boto3.client("cloudformation").describe_stacks(StackName="aws-data-wrangler-test")
    outputs = {}
    for output in response.get("Stacks")[0].get("Outputs"):
        outputs[output.get("OutputKey")] = output.get("OutputValue")
    yield outputs


@pytest.fixture(scope="module")
def session():
    spark_session: SparkSession = SparkSession.builder.appName("AWS Wrangler Test").getOrCreate()
    spark_session.sparkContext.setLogLevel("ERROR")
    spark_session.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.attempts.maximum", "200")
    spark_session.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.retry.interval", "1000ms")
    spark_session.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.retry.limit", "200")
    yield Session(spark_session=spark_session)


@pytest.fixture(scope="module")
def bucket(session, cloudformation_outputs):
    if "BucketName" in cloudformation_outputs:
        bucket = cloudformation_outputs.get("BucketName")
        session.s3.delete_objects(path=f"s3://{bucket}/")
    else:
        raise Exception("You must deploy the test infrastructure using SAM!")
    yield bucket
    session.s3.delete_objects(path=f"s3://{bucket}/")


@pytest.fixture(scope="module")
def database(cloudformation_outputs):
    if "GlueDatabaseName" in cloudformation_outputs:
        database = cloudformation_outputs["GlueDatabaseName"]
    else:
        raise Exception("You must deploy the test infrastructure using Cloudformation!")
    yield database
    tables = wr.glue.tables(database=database)["Table"].tolist()
    for t in tables:
        print(f"Dropping: {database}.{t}...")
        wr.glue.delete_table_if_exists(database=database, table=t)


@pytest.mark.parametrize(
    "sample_name",
    ["nano", "micro", "small"],
)
def test_read_csv(session, bucket, sample_name):
    path = f"data_samples/{sample_name}.csv"
    if sample_name == "micro":
        schema = "id SMALLINT, name STRING, value FLOAT, date TIMESTAMP"
        timestamp_format = "yyyy-MM-dd"
    elif sample_name == "small":
        schema = "id BIGINT, name STRING, date DATE"
        timestamp_format = "dd-MM-yy"
    elif sample_name == "nano":
        schema = "id INTEGER, name STRING, value DOUBLE, date DATE, time TIMESTAMP"
        timestamp_format = "yyyy-MM-dd"
    else:
        raise Exception("Impossible situation!")
    dataframe = session.spark.read_csv(path=path,
                                       schema=schema,
                                       timestampFormat=timestamp_format,
                                       dateFormat=timestamp_format,
                                       header=True)

    boto3.client("s3").upload_file(path, bucket, path)
    path2 = f"s3a://{bucket}/{path}"
    dataframe2 = session.spark.read_csv(path=path2,
                                        schema=schema,
                                        timestampFormat=timestamp_format,
                                        dateFormat=timestamp_format,
                                        header=True)
    assert dataframe.count() == dataframe2.count()
    assert len(list(dataframe.columns)) == len(list(dataframe2.columns))


@pytest.mark.parametrize(
    "compression, partition_by",
    [("snappy", []), ("gzip", ["date", "value"]), ("none", ["time"])],
)
def test_create_glue_table_parquet(session, bucket, database, compression, partition_by):
    s3_path = f"s3://{bucket}/test"
    session.s3.delete_objects(path=s3_path)
    sleep(10)
    path = "data_samples/nano.csv"
    schema = "id INTEGER, name STRING, value DOUBLE, date DATE, time TIMESTAMP"
    timestamp_format = "yyyy-MM-dd"
    dataframe = session.spark.read_csv(path=path,
                                       schema=schema,
                                       timestampFormat=timestamp_format,
                                       dateFormat=timestamp_format,
                                       header=True)
    dataframe = dataframe \
        .withColumn("my_array", array(lit(0), lit(1))) \
        .withColumn("my_struct", struct(lit("text").alias("a"), lit(1).alias("b"))) \
        .withColumn("my_map", create_map(lit("k0"), lit(1.0), lit("k1"), lit(2.0)))
    dataframe.write \
        .mode("overwrite") \
        .format("parquet") \
        .partitionBy(partition_by) \
        .save(compression=compression, path=s3_path.replace("s3://", "s3a://"))
    sleep(10)
    session.spark.create_glue_table(dataframe=dataframe,
                                    file_format="parquet",
                                    partition_by=partition_by,
                                    path=s3_path,
                                    compression=compression,
                                    database=database,
                                    table="test",
                                    replace_if_exists=True)
    query = "select count(*) as counter from test"
    pandas_df = session.pandas.read_sql_athena(sql=query, database=database)
    assert pandas_df.iloc[0]["counter"] == 5
    query = "select my_array[1] as foo, my_struct.a as boo, my_map['k0'] as bar from test limit 1"
    pandas_df = session.pandas.read_sql_athena(sql=query, database=database)
    session.s3.delete_objects(path=s3_path)
    assert pandas_df.iloc[0]["foo"] == 0
    assert pandas_df.iloc[0]["boo"] == "text"
    assert pandas_df.iloc[0]["bar"] == 1.0


@pytest.mark.parametrize(
    "compression, partition_by, serde",
    [("gzip", [], None), ("gzip", ["date", "value"], None), ("none", ["time"], "OpenCSVSerDe"),
     ("gzip", [], "LazySimpleSerDe"), ("gzip", ["date", "value"], "LazySimpleSerDe"),
     ("none", ["time"], "LazySimpleSerDe")],
)
def test_create_glue_table_csv(session, bucket, database, compression, partition_by, serde):
    path = "data_samples/nano.csv"
    schema = "id INTEGER, name STRING, value DOUBLE, date DATE, time TIMESTAMP"
    timestamp_format = "yyyy-MM-dd"
    dataframe = session.spark.read_csv(path=path,
                                       schema=schema,
                                       timestampFormat=timestamp_format,
                                       dateFormat=timestamp_format,
                                       header=True)
    s3_path = f"s3://{bucket}/test"
    session.s3.delete_objects(path=s3_path)
    sleep(10)
    dataframe.write \
        .mode("overwrite") \
        .format("csv") \
        .partitionBy(partition_by) \
        .save(compression=compression, path=s3_path.replace("s3://", "s3a://"))
    sleep(10)
    session.spark.create_glue_table(dataframe=dataframe,
                                    file_format="csv",
                                    partition_by=partition_by,
                                    path=s3_path,
                                    compression=compression,
                                    database=database,
                                    table="test",
                                    serde=serde,
                                    replace_if_exists=True)
    query = "select count(*) as counter from test"
    pandas_df = session.pandas.read_sql_athena(sql=query, database=database)
    assert pandas_df.iloc[0]["counter"] == 5
    query = "select id, name, value from test where cast(id as varchar) = '4' limit 1"
    pandas_df = session.pandas.read_sql_athena(sql=query, database=database)
    assert int(pandas_df.iloc[0]["id"]) == 4
    assert pandas_df.iloc[0]["name"] == "four"
    assert float(pandas_df.iloc[0]["value"]) == 4.0


def test_flatten_simple_struct(session):
    pdf = pd.DataFrame({
        "a": [1, 2],
        "b": [
            {
                "a": 1,
                "b": 2
            },
            {
                "a": 1,
                "b": 2
            },
        ],
    })
    schema = StructType([
        StructField(name="a", dataType=IntegerType(), nullable=True),
        StructField(name="b",
                    dataType=StructType([
                        StructField(name="bb1", dataType=IntegerType(), nullable=True),
                        StructField(name="bb2", dataType=IntegerType(), nullable=True),
                    ]),
                    nullable=True),
    ])
    df = session.spark_session.createDataFrame(data=pdf, schema=schema)
    df.printSchema()
    dfs = session.spark.flatten(dataframe=df)
    assert len(dfs) == 1
    dfs["root"].printSchema()
    dtypes = str(dfs["root"].dtypes)
    assert dtypes == "[('a', 'int'), ('b_bb1', 'int'), ('b_bb2', 'int')]"
    assert df.count() == dfs["root"].count()


def test_flatten_complex_struct(session):
    pdf = pd.DataFrame({
        "a": [1, 2],
        "b": [
            {
                "a": 1,
                "b": {
                    "a": "foo",
                    "b": {
                        "a": datetime(2030, 1, 1),
                        "b": {
                            "a": date(2030, 1, 1),
                            "b": 0.999
                        }
                    }
                }
            },
            {
                "a": 1,
                "b": {
                    "a": "foo",
                    "b": {
                        "a": datetime(2030, 1, 1),
                        "b": {
                            "a": date(2030, 1, 1),
                            "b": 0.999
                        }
                    }
                }
            },
        ],
    })
    schema = StructType([
        StructField(name="a", dataType=IntegerType(), nullable=True),
        StructField(name="b",
                    dataType=StructType([
                        StructField(name="a", dataType=IntegerType(), nullable=True),
                        StructField(name="b",
                                    dataType=StructType([
                                        StructField(name="a", dataType=StringType(), nullable=True),
                                        StructField(name="b",
                                                    dataType=StructType([
                                                        StructField(name="a", dataType=TimestampType(), nullable=True),
                                                        StructField(name="b",
                                                                    dataType=StructType([
                                                                        StructField(name="a",
                                                                                    dataType=DateType(),
                                                                                    nullable=True),
                                                                        StructField(name="b",
                                                                                    dataType=FloatType(),
                                                                                    nullable=True),
                                                                    ]),
                                                                    nullable=True),
                                                    ]),
                                                    nullable=True),
                                    ]),
                                    nullable=True),
                    ]),
                    nullable=True),
    ])
    df = session.spark_session.createDataFrame(data=pdf, schema=schema)
    df.printSchema()
    dfs = session.spark.flatten(dataframe=df)
    assert len(dfs) == 1
    dfs["root"].printSchema()
    dtypes = str(dfs["root"].dtypes)
    assert dtypes == "[('a', 'int'), ('b_a', 'int'), ('b_b_a', 'string'), ('b_b_b_a', 'timestamp'), " \
                     "('b_b_b_b_a', 'date'), ('b_b_b_b_b', 'float')]"
    assert df.count() == dfs["root"].count()
    dfs["root"].show()


def test_flatten_simple_map(session):
    pdf = pd.DataFrame({
        "a": [1, 2],
        "b": [
            {
                "a": 1,
                "b": 2
            },
            {
                "a": 1,
                "b": 2
            },
        ],
    })
    schema = StructType([
        StructField(name="a", dataType=IntegerType(), nullable=True),
        StructField(name="b",
                    dataType=MapType(keyType=StringType(), valueType=IntegerType(), valueContainsNull=True),
                    nullable=True),
    ])
    df = session.spark_session.createDataFrame(data=pdf, schema=schema)
    df.printSchema()
    dfs = session.spark.flatten(dataframe=df)
    assert len(dfs) == 2

    # root
    dfs["root"].printSchema()
    dtypes = str(dfs["root"].dtypes)
    print(dtypes)
    assert dtypes == "[('a', 'int')]"
    assert dfs["root"].count() == df.count()
    dfs["root"].show()

    # root_b
    dfs["root_b"].printSchema()
    dtypes = str(dfs["root_b"].dtypes)
    print(dtypes)
    assert dtypes == "[('a', 'int'), ('b_pos', 'int'), ('b_key', 'string'), ('b_value', 'int')]"
    assert dfs["root_b"].count() == 4
    dfs["root_b"].show()


def test_flatten_simple_array(session):
    pdf = pd.DataFrame({
        "a": [1, 2],
        "b": [
            [1, 2, 3],
            [4, 5],
        ],
    })
    schema = StructType([
        StructField(name="a", dataType=IntegerType(), nullable=True),
        StructField(name="b", dataType=ArrayType(elementType=IntegerType(), containsNull=True), nullable=True),
    ])
    df = session.spark_session.createDataFrame(data=pdf, schema=schema)
    df.printSchema()
    dfs = session.spark.flatten(dataframe=df)
    assert len(dfs) == 2

    # root
    dfs["root"].printSchema()
    dtypes = str(dfs["root"].dtypes)
    print(dtypes)
    assert dtypes == "[('a', 'int')]"
    assert dfs["root"].count() == df.count()
    dfs["root"].show()

    # root_b
    dfs["root_b"].printSchema()
    dtypes = str(dfs["root_b"].dtypes)
    print(dtypes)
    assert dtypes == "[('a', 'int'), ('b_pos', 'int'), ('b', 'int')]"
    assert dfs["root_b"].count() == 5
    dfs["root_b"].show()
