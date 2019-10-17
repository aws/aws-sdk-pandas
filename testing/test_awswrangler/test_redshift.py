import json
import logging

import pytest
import boto3
import pandas
from pyspark.sql import SparkSession
import pg8000

from awswrangler import Session, Redshift
from awswrangler.exceptions import InvalidRedshiftDiststyle, InvalidRedshiftDistkey, InvalidRedshiftSortstyle, InvalidRedshiftSortkey

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s"
)
logging.getLogger("awswrangler").setLevel(logging.DEBUG)


@pytest.fixture(scope="module")
def cloudformation_outputs():
    response = boto3.client("cloudformation").describe_stacks(
        StackName="aws-data-wrangler-test-arena"
    )
    outputs = {}
    for output in response.get("Stacks")[0].get("Outputs"):
        outputs[output.get("OutputKey")] = output.get("OutputValue")
    yield outputs


@pytest.fixture(scope="module")
def session():
    yield Session(spark_session=SparkSession.builder.appName("AWS Wrangler Test").getOrCreate())


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
def redshift_parameters(cloudformation_outputs):
    redshift_parameters = {}
    if "RedshiftAddress" in cloudformation_outputs:
        redshift_parameters["RedshiftAddress"] = cloudformation_outputs.get("RedshiftAddress")
    else:
        raise Exception("You must deploy the test infrastructure using SAM!")
    if "RedshiftPassword" in cloudformation_outputs:
        redshift_parameters["RedshiftPassword"] = cloudformation_outputs.get("RedshiftPassword")
    else:
        raise Exception("You must deploy the test infrastructure using SAM!")
    if "RedshiftPort" in cloudformation_outputs:
        redshift_parameters["RedshiftPort"] = cloudformation_outputs.get("RedshiftPort")
    else:
        raise Exception("You must deploy the test infrastructure using SAM!")
    if "RedshiftRole" in cloudformation_outputs:
        redshift_parameters["RedshiftRole"] = cloudformation_outputs.get("RedshiftRole")
    else:
        raise Exception("You must deploy the test infrastructure using SAM!")
    yield redshift_parameters


@pytest.mark.parametrize(
    "sample_name,mode,factor,diststyle,distkey,sortstyle,sortkey",
    [
        ("micro", "overwrite", 1, "AUTO", "name", None, ["id"]),
        ("micro", "append", 2, None, None, "INTERLEAVED", ["id", "value"]),
        ("small", "overwrite", 1, "KEY", "name", "INTERLEAVED", ["id", "name"]),
        ("small", "append", 2, None, None, "INTERLEAVED", ["id", "name", "date"]),
        ("nano", "overwrite", 1, "ALL", None, "compound", ["id", "name", "date"]),
        ("nano", "append", 2, "ALL", "name", "INTERLEAVED", ["id"]),
    ],
)
def test_to_redshift_pandas(
    session, bucket, redshift_parameters, sample_name, mode, factor, diststyle, distkey, sortstyle,
    sortkey
):
    if sample_name == "micro":
        dates = ["date"]
    if sample_name == "small":
        dates = ["date"]
    if sample_name == "nano":
        dates = ["date", "time"]
    dataframe = pandas.read_csv(
        f"data_samples/{sample_name}.csv", parse_dates=dates, infer_datetime_format=True
    )
    dataframe["date"] = dataframe["date"].dt.date
    con = Redshift.generate_connection(
        database="test",
        host=redshift_parameters.get("RedshiftAddress"),
        port=redshift_parameters.get("RedshiftPort"),
        user="test",
        password=redshift_parameters.get("RedshiftPassword"),
    )
    path = f"s3://{bucket}/redshift-load/"
    session.pandas.to_redshift(
        dataframe=dataframe,
        path=path,
        schema="public",
        table="test",
        connection=con,
        iam_role=redshift_parameters.get("RedshiftRole"),
        diststyle=diststyle,
        distkey=distkey,
        sortstyle=sortstyle,
        sortkey=sortkey,
        mode=mode,
        preserve_index=True,
    )
    cursor = con.cursor()
    cursor.execute("SELECT * from public.test")
    rows = cursor.fetchall()
    cursor.close()
    con.close()
    assert len(dataframe.index) * factor == len(rows)
    assert len(list(dataframe.columns)) + 1 == len(list(rows[0]))


@pytest.mark.parametrize(
    "sample_name,mode,factor,diststyle,distkey,exc,sortstyle,sortkey",
    [
        ("micro", "overwrite", 1, "FOO", "name", InvalidRedshiftDiststyle, None, None),
        ("micro", "overwrite", 2, "KEY", "FOO", InvalidRedshiftDistkey, None, None),
        ("small", "overwrite", 1, "KEY", None, InvalidRedshiftDistkey, None, None),
        ("small", "overwrite", 1, None, None, InvalidRedshiftSortkey, None, ["foo"]),
        ("small", "overwrite", 1, None, None, InvalidRedshiftSortstyle, "foo", ["id"]),
    ],
)
def test_to_redshift_pandas_exceptions(
    session, bucket, redshift_parameters, sample_name, mode, factor, diststyle, distkey, sortstyle,
    sortkey, exc
):
    dataframe = pandas.read_csv(f"data_samples/{sample_name}.csv")
    con = Redshift.generate_connection(
        database="test",
        host=redshift_parameters.get("RedshiftAddress"),
        port=redshift_parameters.get("RedshiftPort"),
        user="test",
        password=redshift_parameters.get("RedshiftPassword"),
    )
    path = f"s3://{bucket}/redshift-load/"
    with pytest.raises(exc):
        assert session.pandas.to_redshift(
            dataframe=dataframe,
            path=path,
            schema="public",
            table="test",
            connection=con,
            iam_role=redshift_parameters.get("RedshiftRole"),
            diststyle=diststyle,
            distkey=distkey,
            sortstyle=sortstyle,
            sortkey=sortkey,
            mode=mode,
            preserve_index=False,
        )
    con.close()


@pytest.mark.parametrize(
    "sample_name,mode,factor,diststyle,distkey,sortstyle,sortkey",
    [
        ("micro", "overwrite", 1, "AUTO", "name", None, ["id"]),
        ("micro", "append", 2, None, None, "INTERLEAVED", ["id", "value"]),
        ("small", "overwrite", 1, "KEY", "name", "INTERLEAVED", ["id", "name"]),
        ("small", "append", 2, None, None, "INTERLEAVED", ["id", "name", "date"]),
        ("nano", "overwrite", 1, "ALL", None, "compound", ["id", "name", "date"]),
        ("nano", "append", 2, "ALL", "name", "INTERLEAVED", ["id"]),
    ],
)
def test_to_redshift_spark(
    session, bucket, redshift_parameters, sample_name, mode, factor, diststyle, distkey, sortstyle,
    sortkey
):
    path = f"data_samples/{sample_name}.csv"
    if sample_name == "micro":
        schema = "id SMALLINT, name STRING, value FLOAT, date DATE"
        timestamp_format = "yyyy-MM-dd"
    elif sample_name == "small":
        schema = "id BIGINT, name STRING, date DATE"
        timestamp_format = "dd-MM-yy"
    elif sample_name == "nano":
        schema = "id INTEGER, name STRING, value DOUBLE, date DATE, time TIMESTAMP"
        timestamp_format = "yyyy-MM-dd"
    dataframe = session.spark.read_csv(
        path=path,
        schema=schema,
        timestampFormat=timestamp_format,
        dateFormat=timestamp_format,
        header=True
    )
    con = Redshift.generate_connection(
        database="test",
        host=redshift_parameters.get("RedshiftAddress"),
        port=redshift_parameters.get("RedshiftPort"),
        user="test",
        password=redshift_parameters.get("RedshiftPassword"),
    )
    session.spark.to_redshift(
        dataframe=dataframe,
        path=f"s3://{bucket}/redshift-load/",
        connection=con,
        schema="public",
        table="test",
        iam_role=redshift_parameters.get("RedshiftRole"),
        diststyle=diststyle,
        distkey=distkey,
        sortstyle=sortstyle,
        sortkey=sortkey,
        mode=mode,
        min_num_partitions=2,
    )
    cursor = con.cursor()
    cursor.execute("SELECT * from public.test")
    rows = cursor.fetchall()
    cursor.close()
    con.close()
    assert (dataframe.count() * factor) == len(rows)
    assert len(list(dataframe.columns)) == len(list(rows[0]))


@pytest.mark.parametrize(
    "sample_name,mode,factor,diststyle,distkey,exc,sortstyle,sortkey",
    [
        ("micro", "overwrite", 1, "FOO", "name", InvalidRedshiftDiststyle, None, None),
        ("micro", "overwrite", 2, "KEY", "FOO", InvalidRedshiftDistkey, None, None),
        ("small", "overwrite", 1, "KEY", None, InvalidRedshiftDistkey, None, None),
        ("small", "overwrite", 1, None, None, InvalidRedshiftSortkey, None, ["foo"]),
        ("small", "overwrite", 1, None, None, InvalidRedshiftSortstyle, "foo", ["id"]),
    ],
)
def test_to_redshift_spark_exceptions(
    session, bucket, redshift_parameters, sample_name, mode, factor, diststyle, distkey, sortstyle,
    sortkey, exc
):
    path = f"data_samples/{sample_name}.csv"
    dataframe = session.spark.read_csv(path=path)
    con = Redshift.generate_connection(
        database="test",
        host=redshift_parameters.get("RedshiftAddress"),
        port=redshift_parameters.get("RedshiftPort"),
        user="test",
        password=redshift_parameters.get("RedshiftPassword"),
    )
    with pytest.raises(exc):
        assert session.spark.to_redshift(
            dataframe=dataframe,
            path=f"s3://{bucket}/redshift-load/",
            connection=con,
            schema="public",
            table="test",
            iam_role=redshift_parameters.get("RedshiftRole"),
            diststyle=diststyle,
            distkey=distkey,
            sortstyle=sortstyle,
            sortkey=sortkey,
            mode=mode,
            min_num_partitions=2,
        )
    con.close()


def test_write_load_manifest(session, bucket):
    boto3.client("s3").upload_file("data_samples/small.csv", bucket, "data_samples/small.csv")
    object_path = f"s3://{bucket}/data_samples/small.csv"
    manifest_path = f"s3://{bucket}/manifest.json"
    session.redshift.write_load_manifest(manifest_path=manifest_path, objects_paths=[object_path])
    manifest_json = (
        boto3.client("s3").get_object(Bucket=bucket, Key="manifest.json").get("Body").read()
    )
    manifest = json.loads(manifest_json)
    assert manifest.get("entries")[0].get("url") == object_path
    assert manifest.get("entries")[0].get("mandatory")
    assert manifest.get("entries")[0].get("meta").get("content_length") == 2247


def test_connection_timeout(redshift_parameters):
    with pytest.raises(pg8000.core.InterfaceError):
        Redshift.generate_connection(
            database="test",
            host=redshift_parameters.get("RedshiftAddress"),
            port=12345,
            user="test",
            password=redshift_parameters.get("RedshiftPassword"),
        )


def test_connection_with_different_port_types(redshift_parameters):
    conn = Redshift.generate_connection(
        database="test",
        host=redshift_parameters.get("RedshiftAddress"),
        port=str(redshift_parameters.get("RedshiftPort")),
        user="test",
        password=redshift_parameters.get("RedshiftPassword"),
    )
    conn.close()
    conn = Redshift.generate_connection(
        database="test",
        host=redshift_parameters.get("RedshiftAddress"),
        port=float(redshift_parameters.get("RedshiftPort")),
        user="test",
        password=redshift_parameters.get("RedshiftPassword"),
    )
    conn.close()
