from datetime import datetime

import boto3
import pytest

import awswrangler as wr

from ._utils import extract_cloudformation_outputs, get_time_str_with_random_suffix, list_workgroups, path_generator


@pytest.fixture(scope="session")
def cloudformation_outputs():
    return extract_cloudformation_outputs()


@pytest.fixture(scope="session")
def region(cloudformation_outputs):
    return cloudformation_outputs["Region"]


@pytest.fixture(scope="session")
def bucket(cloudformation_outputs):
    return cloudformation_outputs["BucketName"]


@pytest.fixture(scope="session")
def glue_database(cloudformation_outputs):
    return cloudformation_outputs["GlueDatabaseName"]


@pytest.fixture(scope="session")
def kms_key(cloudformation_outputs):
    return cloudformation_outputs["KmsKeyArn"]


@pytest.fixture(scope="session")
def kms_key_id(kms_key):
    return kms_key.split("/", 1)[1]


@pytest.fixture(scope="session")
def loggroup(cloudformation_outputs):
    loggroup_name = cloudformation_outputs["LogGroupName"]
    logstream_name = cloudformation_outputs["LogStream"]
    client = boto3.client("logs")
    response = client.describe_log_streams(logGroupName=loggroup_name, logStreamNamePrefix=logstream_name)
    token = response["logStreams"][0].get("uploadSequenceToken")
    events = []
    for i in range(5):
        events.append({"timestamp": int(1000 * datetime.now().timestamp()), "message": str(i)})
    args = {"logGroupName": loggroup_name, "logStreamName": logstream_name, "logEvents": events}
    if token:
        args["sequenceToken"] = token
    try:
        client.put_log_events(**args)
    except client.exceptions.DataAlreadyAcceptedException:
        pass  # Concurrency
    while True:
        results = wr.cloudwatch.run_query(log_group_names=[loggroup_name], query="fields @timestamp | limit 5")
        if len(results) >= 5:
            break
    yield loggroup_name


@pytest.fixture(scope="session")
def workgroup0(bucket):
    wkg_name = "aws_data_wrangler_0"
    client = boto3.client("athena")
    wkgs = list_workgroups()
    wkgs = [x["Name"] for x in wkgs["WorkGroups"]]
    if wkg_name not in wkgs:
        client.create_work_group(
            Name=wkg_name,
            Configuration={
                "ResultConfiguration": {"OutputLocation": f"s3://{bucket}/athena_workgroup0/"},
                "EnforceWorkGroupConfiguration": True,
                "PublishCloudWatchMetricsEnabled": True,
                "BytesScannedCutoffPerQuery": 100_000_000,
                "RequesterPaysEnabled": False,
            },
            Description="AWS Data Wrangler Test WorkGroup Number 0",
        )
    return wkg_name


@pytest.fixture(scope="session")
def workgroup1(bucket):
    wkg_name = "aws_data_wrangler_1"
    client = boto3.client("athena")
    wkgs = list_workgroups()
    wkgs = [x["Name"] for x in wkgs["WorkGroups"]]
    if wkg_name not in wkgs:
        client.create_work_group(
            Name=wkg_name,
            Configuration={
                "ResultConfiguration": {
                    "OutputLocation": f"s3://{bucket}/athena_workgroup1/",
                    "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
                },
                "EnforceWorkGroupConfiguration": True,
                "PublishCloudWatchMetricsEnabled": True,
                "BytesScannedCutoffPerQuery": 100_000_000,
                "RequesterPaysEnabled": False,
            },
            Description="AWS Data Wrangler Test WorkGroup Number 1",
        )
    return wkg_name


@pytest.fixture(scope="session")
def workgroup2(bucket, kms_key):
    wkg_name = "aws_data_wrangler_2"
    client = boto3.client("athena")
    wkgs = list_workgroups()
    wkgs = [x["Name"] for x in wkgs["WorkGroups"]]
    if wkg_name not in wkgs:
        client.create_work_group(
            Name=wkg_name,
            Configuration={
                "ResultConfiguration": {
                    "OutputLocation": f"s3://{bucket}/athena_workgroup2/",
                    "EncryptionConfiguration": {"EncryptionOption": "SSE_KMS", "KmsKey": kms_key},
                },
                "EnforceWorkGroupConfiguration": False,
                "PublishCloudWatchMetricsEnabled": True,
                "BytesScannedCutoffPerQuery": 100_000_000,
                "RequesterPaysEnabled": False,
            },
            Description="AWS Data Wrangler Test WorkGroup Number 2",
        )
    return wkg_name


@pytest.fixture(scope="session")
def workgroup3(bucket, kms_key):
    wkg_name = "aws_data_wrangler_3"
    client = boto3.client("athena")
    wkgs = list_workgroups()
    wkgs = [x["Name"] for x in wkgs["WorkGroups"]]
    if wkg_name not in wkgs:
        client.create_work_group(
            Name=wkg_name,
            Configuration={
                "ResultConfiguration": {
                    "OutputLocation": f"s3://{bucket}/athena_workgroup3/",
                    "EncryptionConfiguration": {"EncryptionOption": "SSE_KMS", "KmsKey": kms_key},
                },
                "EnforceWorkGroupConfiguration": True,
                "PublishCloudWatchMetricsEnabled": True,
                "BytesScannedCutoffPerQuery": 100_000_000,
                "RequesterPaysEnabled": False,
            },
            Description="AWS Data Wrangler Test WorkGroup Number 3",
        )
    return wkg_name


@pytest.fixture(scope="session")
def databases_parameters(cloudformation_outputs):
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
    return parameters


@pytest.fixture(scope="session")
def redshift_external_schema(cloudformation_outputs, databases_parameters, glue_database):
    region = cloudformation_outputs.get("Region")
    sql = f"""
    CREATE EXTERNAL SCHEMA IF NOT EXISTS aws_data_wrangler_external FROM data catalog
    DATABASE '{glue_database}'
    IAM_ROLE '{databases_parameters["redshift"]["role"]}'
    REGION '{region}';
    """
    engine = wr.catalog.get_engine(connection="aws-data-wrangler-redshift")
    with engine.connect() as con:
        con.execute(sql)
    return "aws_data_wrangler_external"


@pytest.fixture(scope="function")
def glue_table(glue_database):
    name = f"tbl_{get_time_str_with_random_suffix()}"
    print(f"Table name: {name}")
    wr.catalog.delete_table_if_exists(database=glue_database, table=name)
    yield name
    wr.catalog.delete_table_if_exists(database=glue_database, table=name)
    print(f"Table {glue_database}.{name} deleted.")


@pytest.fixture(scope="function")
def glue_table2(glue_database):
    name = f"tbl_{get_time_str_with_random_suffix()}"
    print(f"Table name: {name}")
    wr.catalog.delete_table_if_exists(database=glue_database, table=name)
    yield name
    wr.catalog.delete_table_if_exists(database=glue_database, table=name)


@pytest.fixture(scope="function")
def path(bucket):
    yield from path_generator(bucket)


@pytest.fixture(scope="function")
def path2(bucket):
    yield from path_generator(bucket)


@pytest.fixture(scope="function")
def path3(bucket):
    yield from path_generator(bucket)
