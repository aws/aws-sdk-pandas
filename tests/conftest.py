from datetime import datetime

import boto3  # type: ignore
import pytest  # type: ignore

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
        wkg_name="aws_data_wrangler_0",
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
        wkg_name="aws_data_wrangler_1",
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
        wkg_name="aws_data_wrangler_2",
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
        wkg_name="aws_data_wrangler_3",
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
def databases_parameters(cloudformation_outputs):
    parameters = dict(postgresql={}, mysql={}, redshift={}, sqlserver={})
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
    parameters["sqlserver"]["host"] = cloudformation_outputs["SqlServerAddress"]
    parameters["sqlserver"]["port"] = 1433
    parameters["sqlserver"]["schema"] = "dbo"
    parameters["sqlserver"]["database"] = "test"
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
    con = wr.redshift.connect(connection="aws-data-wrangler-redshift")
    with con.cursor() as cursor:
        cursor.execute(sql)
        con.commit()
    con.close()
    return "aws_data_wrangler_external"


@pytest.fixture(scope="function")
def glue_ctas_database():
    name = f"db_{get_time_str_with_random_suffix()}"
    print(f"Database name: {name}")
    wr.catalog.create_database(name=name)
    yield name
    wr.catalog.delete_database(name=name)
    print(f"Database {name} deleted.")


@pytest.fixture(scope="function")
def glue_table(glue_database: str) -> None:
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


@pytest.fixture(scope="function")
def redshift_table():
    name = f"tbl_{get_time_str_with_random_suffix()}"
    print(f"Table name: {name}")
    yield name
    con = wr.redshift.connect("aws-data-wrangler-redshift")
    with con.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS public.{name}")
    con.commit()
    con.close()


@pytest.fixture(scope="function")
def postgresql_table():
    name = f"tbl_{get_time_str_with_random_suffix()}"
    print(f"Table name: {name}")
    yield name
    con = wr.postgresql.connect("aws-data-wrangler-postgresql")
    with con.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS public.{name}")
    con.commit()
    con.close()


@pytest.fixture(scope="function")
def mysql_table():
    name = f"tbl_{get_time_str_with_random_suffix()}"
    print(f"Table name: {name}")
    yield name
    con = wr.mysql.connect("aws-data-wrangler-mysql")
    with con.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS test.{name}")
    con.commit()
    con.close()


@pytest.fixture(scope="function")
def sqlserver_table():
    name = f"tbl_{get_time_str_with_random_suffix()}"
    print(f"Table name: {name}")
    yield name
    con = wr.sqlserver.connect("aws-data-wrangler-sqlserver")
    with con.cursor() as cursor:
        cursor.execute(f"IF OBJECT_ID(N'dbo.{name}', N'U') IS NOT NULL DROP TABLE dbo.{name}")
    con.commit()
    con.close()


@pytest.fixture(scope="function")
def timestream_database_and_table():
    name = f"tbl_{get_time_str_with_random_suffix()}"
    print(f"Timestream name: {name}")
    wr.timestream.create_database(name)
    wr.timestream.create_table(name, name, 1, 1)
    yield name
    wr.timestream.delete_table(name, name)
    wr.timestream.delete_database(name)
