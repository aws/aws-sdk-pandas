import logging

import pytest
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, array, create_map, struct

from awswrangler import Session

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)


@pytest.fixture(scope="module")
def cloudformation_outputs():
    response = boto3.client("cloudformation").describe_stacks(
        StackName="aws-data-wrangler-test-arena")
    outputs = {}
    for output in response.get("Stacks")[0].get("Outputs"):
        outputs[output.get("OutputKey")] = output.get("OutputValue")
    yield outputs


@pytest.fixture(scope="module")
def session():
    yield Session(spark_session=SparkSession.builder.appName(
        "AWS Wrangler Test").getOrCreate())


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
        raise Exception(
            "You must deploy the test infrastructure using Cloudformation!")
    yield database


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
    dataframe = session.spark.read_csv(path=path,
                                       schema=schema,
                                       timestampFormat=timestamp_format,
                                       dateFormat=timestamp_format,
                                       header=True)

    boto3.client("s3").upload_file(path, bucket, path)
    path2 = f"s3://{bucket}/{path}"
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
def test_create_glue_table_parquet(session, bucket, database, compression,
                                   partition_by):
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
    s3_path = f"s3://{bucket}/test"
    dataframe.write \
        .mode("overwrite") \
        .format("parquet") \
        .partitionBy(partition_by) \
        .save(compression=compression, path=s3_path)
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
    assert pandas_df.iloc[0]["foo"] == 0
    assert pandas_df.iloc[0]["boo"] == "text"
    assert pandas_df.iloc[0]["bar"] == 1.0


@pytest.mark.parametrize(
    "compression, partition_by, serde",
    [("gzip", [], None), ("gzip", ["date", "value"], None),
     ("none", ["time"], "OpenCSVSerDe"), ("gzip", [], "LazySimpleSerDe"),
     ("gzip", ["date", "value"], "LazySimpleSerDe"),
     ("none", ["time"], "LazySimpleSerDe")],
)
def test_create_glue_table_csv(session, bucket, database, compression,
                               partition_by, serde):
    path = "data_samples/nano.csv"
    schema = "id INTEGER, name STRING, value DOUBLE, date DATE, time TIMESTAMP"
    timestamp_format = "yyyy-MM-dd"
    dataframe = session.spark.read_csv(path=path,
                                       schema=schema,
                                       timestampFormat=timestamp_format,
                                       dateFormat=timestamp_format,
                                       header=True)
    s3_path = f"s3://{bucket}/test"
    dataframe.write \
        .mode("overwrite") \
        .format("csv") \
        .partitionBy(partition_by) \
        .save(compression=compression, path=s3_path)
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
