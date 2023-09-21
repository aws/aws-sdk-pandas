import json
import os
import uuid
from datetime import datetime
from importlib import reload
from types import ModuleType
from typing import Iterator, Optional

import boto3
import botocore.exceptions
import pytest

import awswrangler as wr

from ._utils import create_workgroup, extract_cloudformation_outputs, get_time_str_with_random_suffix, path_generator


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
    return create_workgroup(
        wkg_name="aws_sdk_pandas_0",
        config={
            "ResultConfiguration": {"OutputLocation": f"s3://{bucket}/athena_workgroup0/"},
            "EnforceWorkGroupConfiguration": True,
            "PublishCloudWatchMetricsEnabled": True,
            "BytesScannedCutoffPerQuery": 100_000_000,
            "RequesterPaysEnabled": False,
        },
    )


@pytest.fixture(scope="session")
def workgroup1(bucket):
    return create_workgroup(
        wkg_name="aws_sdk_pandas_1",
        config={
            "ResultConfiguration": {
                "OutputLocation": f"s3://{bucket}/athena_workgroup1/",
                "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
            },
            "EnforceWorkGroupConfiguration": True,
            "PublishCloudWatchMetricsEnabled": True,
            "BytesScannedCutoffPerQuery": 100_000_000,
            "RequesterPaysEnabled": False,
        },
    )


@pytest.fixture(scope="session")
def workgroup2(bucket, kms_key):
    return create_workgroup(
        wkg_name="aws_sdk_pandas_2",
        config={
            "ResultConfiguration": {
                "OutputLocation": f"s3://{bucket}/athena_workgroup2/",
                "EncryptionConfiguration": {"EncryptionOption": "SSE_KMS", "KmsKey": kms_key},
            },
            "EnforceWorkGroupConfiguration": False,
            "PublishCloudWatchMetricsEnabled": True,
            "BytesScannedCutoffPerQuery": 100_000_000,
            "RequesterPaysEnabled": False,
        },
    )


@pytest.fixture(scope="session")
def workgroup3(bucket, kms_key):
    return create_workgroup(
        wkg_name="aws_sdk_pandas_3",
        config={
            "ResultConfiguration": {
                "OutputLocation": f"s3://{bucket}/athena_workgroup3/",
                "EncryptionConfiguration": {"EncryptionOption": "SSE_KMS", "KmsKey": kms_key},
            },
            "EnforceWorkGroupConfiguration": True,
            "PublishCloudWatchMetricsEnabled": True,
            "BytesScannedCutoffPerQuery": 100_000_000,
            "RequesterPaysEnabled": False,
        },
    )


@pytest.fixture(scope="session")
def databases_parameters(cloudformation_outputs, db_password):
    parameters = dict(
        postgresql={},
        mysql={},
        redshift={},
        sqlserver={},
        mysql_serverless={},
        oracle={},
        redshift_serverless={},
        postgresql_serverless={},
    )
    parameters["postgresql"]["host"] = cloudformation_outputs.get("PostgresqlAddress")
    parameters["postgresql"]["port"] = cloudformation_outputs.get("PostgresqlPort")
    parameters["postgresql"]["schema"] = cloudformation_outputs.get("PostgresqlSchema")
    parameters["postgresql"]["database"] = cloudformation_outputs.get("PostgresqlDatabase")
    parameters["mysql"]["host"] = cloudformation_outputs.get("MysqlAddress")
    parameters["mysql"]["port"] = cloudformation_outputs.get("MysqlPort")
    parameters["mysql"]["schema"] = cloudformation_outputs.get("MysqlDatabase")
    parameters["mysql"]["database"] = cloudformation_outputs.get("MysqlSchema")
    parameters["redshift"]["secret_arn"] = cloudformation_outputs.get("RedshiftSecretArn")
    parameters["redshift"]["host"] = cloudformation_outputs.get("RedshiftAddress")
    parameters["redshift"]["port"] = cloudformation_outputs.get("RedshiftPort")
    parameters["redshift"]["identifier"] = cloudformation_outputs.get("RedshiftIdentifier")
    parameters["redshift"]["schema"] = cloudformation_outputs.get("RedshiftSchema")
    parameters["redshift"]["database"] = cloudformation_outputs.get("RedshiftDatabase")
    parameters["redshift"]["role"] = cloudformation_outputs.get("RedshiftRole")
    parameters["password"] = db_password
    parameters["user"] = "test"
    parameters["sqlserver"]["host"] = cloudformation_outputs.get("SqlServerAddress")
    parameters["sqlserver"]["port"] = cloudformation_outputs.get("SqlServerPort")
    parameters["sqlserver"]["schema"] = cloudformation_outputs.get("SqlServerSchema")
    parameters["sqlserver"]["database"] = cloudformation_outputs.get("SqlServerDatabase")
    parameters["mysql_serverless"]["secret_arn"] = cloudformation_outputs.get("MysqlServerlessSecretArn")
    parameters["mysql_serverless"]["schema"] = cloudformation_outputs.get("MysqlServerlessSchema")
    parameters["mysql_serverless"]["database"] = cloudformation_outputs.get("MysqlServerlessDatabase")
    parameters["mysql_serverless"]["arn"] = cloudformation_outputs.get("MysqlServerlessClusterArn")
    parameters["postgresql_serverless"]["secret_arn"] = cloudformation_outputs.get("PostgresqlServerlessSecretArn")
    parameters["postgresql_serverless"]["schema"] = cloudformation_outputs.get("PostgresqlServerlessSchema")
    parameters["postgresql_serverless"]["database"] = cloudformation_outputs.get("PostgresqlServerlessDatabase")
    parameters["postgresql_serverless"]["arn"] = cloudformation_outputs.get("PostgresqlServerlessClusterArn")
    parameters["oracle"]["host"] = cloudformation_outputs.get("OracleAddress")
    parameters["oracle"]["port"] = cloudformation_outputs.get("OraclePort")
    parameters["oracle"]["schema"] = cloudformation_outputs.get("OracleSchema")
    parameters["oracle"]["database"] = cloudformation_outputs.get("OracleDatabase")
    parameters["redshift_serverless"]["secret_arn"] = cloudformation_outputs.get("RedshiftServerlessSecretArn")
    parameters["redshift_serverless"]["workgroup"] = cloudformation_outputs.get("RedshiftServerlessWorkgroup")
    parameters["redshift_serverless"]["database"] = cloudformation_outputs.get("RedshiftServerlessDatabase")
    return parameters


@pytest.fixture(scope="session")
def redshift_external_schema(cloudformation_outputs, databases_parameters, glue_database):
    region = cloudformation_outputs.get("Region")
    sql = f"""
    CREATE EXTERNAL SCHEMA IF NOT EXISTS aws_sdk_pandas_external FROM data catalog
    DATABASE '{glue_database}'
    IAM_ROLE '{databases_parameters["redshift"]["role"]}'
    REGION '{region}';
    """
    with wr.redshift.connect(connection="aws-sdk-pandas-redshift") as con:
        with con.cursor() as cursor:
            cursor.execute(sql)
            con.commit()
    return "aws_sdk_pandas_external"


@pytest.fixture(scope="session")
def account_id():
    return boto3.client("sts").get_caller_identity().get("Account")


@pytest.fixture(scope="session")
def db_password():
    return boto3.client("secretsmanager").get_secret_value(SecretId="aws-sdk-pandas/db_password")["SecretString"]


@pytest.fixture(scope="function")
def dynamodb_table(params) -> None:
    name = f"tbl_{get_time_str_with_random_suffix()}"
    print(f"Table name: {name}")
    params.update({"TableName": name, "BillingMode": "PAY_PER_REQUEST"})
    dynamodb_resource = boto3.resource("dynamodb")
    table = dynamodb_resource.create_table(**params)
    table.wait_until_exists()
    yield name
    table.delete()
    table.wait_until_not_exists()
    print(f"Table {name} deleted.")


@pytest.fixture(scope="function")
def glue_ctas_database():
    name = f"db_{get_time_str_with_random_suffix()}"
    print(f"Database name: {name}")
    wr.catalog.create_database(name=name)
    yield name
    wr.catalog.delete_database(name=name)
    print(f"Database {name} deleted.")


@pytest.fixture(scope="function")
def glue_table(glue_database: str) -> str:
    name = f"tbl_{get_time_str_with_random_suffix()}"
    print(f"Table name: {name}")
    wr.catalog.delete_table_if_exists(database=glue_database, table=name)
    yield name
    wr.catalog.delete_table_if_exists(database=glue_database, table=name)
    print(f"Table {glue_database}.{name} deleted.")


@pytest.fixture(scope="function")
def glue_table_with_hyphenated_name(glue_database: str) -> str:
    name = f"tbl-{get_time_str_with_random_suffix()}"
    print(f"Table name: {name}")
    wr.catalog.delete_table_if_exists(database=glue_database, table=name)
    yield name
    wr.catalog.delete_table_if_exists(database=glue_database, table=name)
    print(f"Table {glue_database}.{name} deleted.")


@pytest.fixture(scope="function")
def glue_table2(glue_database) -> str:
    name = f"tbl_{get_time_str_with_random_suffix()}"
    print(f"Table name: {name}")
    wr.catalog.delete_table_if_exists(database=glue_database, table=name)
    yield name
    wr.catalog.delete_table_if_exists(database=glue_database, table=name)


@pytest.fixture(scope="function")
def quicksight_datasource() -> str:
    name = f"test{str(uuid.uuid4())[:8]}"
    print(f"Quicksight Data Source: {name}")
    wr.quicksight.delete_all_data_sources(regex_filter=name)
    yield name
    wr.quicksight.delete_all_data_sources(regex_filter=name)
    print(f"Quicksight Data Source: {name} deleted")


@pytest.fixture(scope="function")
def quicksight_dataset() -> str:
    name = f"test{str(uuid.uuid4())[:8]}"
    print(f"Quicksight Dataset: {name}")
    wr.quicksight.delete_all_datasets(regex_filter=name)
    yield name
    wr.quicksight.delete_all_datasets(regex_filter=name)
    print(f"Quicksight Dataset: {name} deleted")


@pytest.fixture(scope="function")
def path(bucket):
    yield from path_generator(bucket)


@pytest.fixture(scope="function")
def path2(bucket):
    yield from path_generator(bucket)


@pytest.fixture(scope="function")
def path3(bucket):
    yield from path_generator(bucket)


@pytest.fixture(scope="function")
def redshift_table():
    name = f"tbl_{get_time_str_with_random_suffix()}"
    print(f"Table name: {name}")
    yield name
    with wr.redshift.connect("aws-sdk-pandas-redshift") as con:
        with con.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS public.{name}")
        con.commit()


@pytest.fixture(scope="function")
def redshift_table_with_hyphenated_name():
    name = f"tbl-{get_time_str_with_random_suffix()}"
    print(f"Table name: {name}")
    yield name
    with wr.redshift.connect("aws-sdk-pandas-redshift") as con:
        with con.cursor() as cursor:
            cursor.execute(f'DROP TABLE IF EXISTS public."{name}"')
        con.commit()


@pytest.fixture(scope="function")
def postgresql_table():
    name = f"tbl_{get_time_str_with_random_suffix()}"
    print(f"Table name: {name}")
    yield name
    con = wr.postgresql.connect("aws-sdk-pandas-postgresql")
    with con.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS public.{name}")
    con.commit()
    con.close()


@pytest.fixture(scope="function")
def mysql_table():
    name = f"tbl_{get_time_str_with_random_suffix()}"
    print(f"Table name: {name}")
    yield name
    con = wr.mysql.connect("aws-sdk-pandas-mysql")
    with con.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS test.{name}")
    con.commit()
    con.close()


@pytest.fixture(scope="function")
def sqlserver_table():
    name = f"tbl_{get_time_str_with_random_suffix()}"
    print(f"Table name: {name}")
    yield name
    con = wr.sqlserver.connect("aws-sdk-pandas-sqlserver")
    with con.cursor() as cursor:
        cursor.execute(f"IF OBJECT_ID(N'dbo.{name}', N'U') IS NOT NULL DROP TABLE dbo.{name}")
    con.commit()
    con.close()


@pytest.fixture(scope="function")
def oracle_table() -> str:
    name = f"tbl_{get_time_str_with_random_suffix()}"
    print(f"Table name: {name}")
    yield name
    with wr.oracle.connect("aws-sdk-pandas-oracle") as con:
        sql = f"""
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE "TEST"."{name}"';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -942 THEN
            RAISE;
        END IF;
END;
        """
        with con.cursor() as cursor:
            cursor.execute(sql)
        con.commit()


@pytest.fixture(scope="function")
def timestream_database():
    name = f"tbl_{get_time_str_with_random_suffix()}"
    print(f"Timestream name: {name}")
    wr.timestream.create_database(name)
    yield name
    try:
        wr.timestream.delete_database(name)
    except botocore.exceptions.ClientError as err:
        if err.response["Error"]["Code"] == "ResourceNotFound":
            pass


@pytest.fixture(scope="function")
def timestream_database_and_table():
    name = f"tbl_{get_time_str_with_random_suffix()}"
    print(f"Timestream name: {name}")
    wr.timestream.create_database(name)
    wr.timestream.create_table(name, name, 1, 1)
    yield name
    try:
        wr.timestream.delete_table(name, name)
    except botocore.exceptions.ClientError as err:
        if err.response["Error"]["Code"] == "ResourceNotFound":
            pass
    try:
        wr.timestream.delete_database(name)
    except botocore.exceptions.ClientError as err:
        if err.response["Error"]["Code"] == "ResourceNotFound":
            pass


@pytest.fixture(scope="function")
def compare_filename_prefix():
    def assert_filename_prefix(filename, filename_prefix, test_prefix):
        if filename_prefix:
            assert filename.startswith(test_prefix)
        else:
            assert not filename.startswith(test_prefix)

    return assert_filename_prefix


@pytest.fixture(scope="function")
def random_glue_database():
    database_name = get_time_str_with_random_suffix()
    yield database_name
    wr.catalog.delete_database(database_name)


@pytest.fixture(scope="function")
def redshift_con():
    with wr.redshift.connect("aws-sdk-pandas-redshift") as con:
        yield con


@pytest.fixture(scope="function")
def glue_ruleset() -> str:
    name = f"ruleset_{get_time_str_with_random_suffix()}"
    print(f"Ruleset name: {name}")
    yield name


@pytest.fixture(scope="function")
def emr_security_configuration():
    name = f"emr_{get_time_str_with_random_suffix()}"
    print(f"EMR Security Configuration: {name}")
    security_configuration = {
        "EncryptionConfiguration": {"EnableInTransitEncryption": False, "EnableAtRestEncryption": False},
        "InstanceMetadataServiceConfiguration": {
            "MinimumInstanceMetadataServiceVersion": 2,
            "HttpPutResponseHopLimit": 1,
        },
    }
    boto3.client("emr").create_security_configuration(
        Name=name, SecurityConfiguration=json.dumps(security_configuration)
    )
    yield name
    boto3.client("emr").delete_security_configuration(Name=name)
    print(f"Security Configuration: {name} deleted.")


@pytest.fixture(scope="session")
def emr_serverless_execution_role_arn(cloudformation_outputs):
    return cloudformation_outputs["EMRServerlessExecutionRoleArn"]


@pytest.fixture(scope="session")
def glue_data_quality_role(cloudformation_outputs):
    return cloudformation_outputs["GlueDataQualityRole"]


@pytest.fixture(scope="session")
def cleanrooms_membership_id(cloudformation_outputs):
    return cloudformation_outputs["CleanRoomsMembershipId"]


@pytest.fixture(scope="session")
def cleanrooms_glue_database_name(cloudformation_outputs):
    return cloudformation_outputs["CleanRoomsGlueDatabaseName"]


@pytest.fixture(scope="session")
def cleanrooms_s3_bucket_name(cloudformation_outputs):
    return cloudformation_outputs["CleanRoomsS3BucketName"]


@pytest.fixture(scope="function")
def local_filename() -> Iterator[str]:
    filename = os.path.join(".", f"{get_time_str_with_random_suffix()}.data")

    yield filename

    try:
        os.remove(filename)
    except OSError:
        pass


@pytest.fixture(scope="function", name="wr")
def awswrangler_import() -> Iterator[ModuleType]:
    import awswrangler

    awswrangler.config.reset()

    yield reload(awswrangler)

    # Reset for future tests
    awswrangler.config.reset()


@pytest.fixture(scope="function")
def data_gen_bucket() -> Optional[str]:
    try:
        ssm_parameter = boto3.client("ssm").get_parameter(Name="/SDKPandas/GlueRay/DataGenBucketName")
    except botocore.exceptions.ClientError:
        return None
    return ssm_parameter["Parameter"]["Value"]  # type: ignore
