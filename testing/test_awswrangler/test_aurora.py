import logging

import pytest
import boto3

from awswrangler import Aurora
from awswrangler.exceptions import InvalidEngine

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
def postgres_parameters(cloudformation_outputs):
    postgres_parameters = {}
    if "PostgresAddress" in cloudformation_outputs:
        postgres_parameters["PostgresAddress"] = cloudformation_outputs.get("PostgresAddress")
    else:
        raise Exception("You must deploy the test infrastructure using SAM!")
    if "DatabasesPassword" in cloudformation_outputs:
        postgres_parameters["DatabasesPassword"] = cloudformation_outputs.get("DatabasesPassword")
    else:
        raise Exception("You must deploy the test infrastructure using SAM!")
    yield postgres_parameters


@pytest.fixture(scope="module")
def mysql_parameters(cloudformation_outputs):
    mysql_parameters = {}
    if "MysqlAddress" in cloudformation_outputs:
        mysql_parameters["MysqlAddress"] = cloudformation_outputs.get("MysqlAddress")
    else:
        raise Exception("You must deploy the test infrastructure using SAM!")
    if "DatabasesPassword" in cloudformation_outputs:
        mysql_parameters["DatabasesPassword"] = cloudformation_outputs.get("DatabasesPassword")
    else:
        raise Exception("You must deploy the test infrastructure using SAM!")
    yield mysql_parameters


def test_postgres_connection(postgres_parameters):
    conn = Aurora.generate_connection(database="postgres",
                                      host=postgres_parameters["PostgresAddress"],
                                      port=3306,
                                      user="test",
                                      password=postgres_parameters["DatabasesPassword"],
                                      engine="postgres")
    cursor = conn.cursor()
    cursor.execute("SELECT 1 + 2, 3 + 4")
    first_row = cursor.fetchall()[0]
    assert first_row[0] == 3
    assert first_row[1] == 7
    cursor.close()
    conn.close()


def test_mysql_connection(mysql_parameters):
    conn = Aurora.generate_connection(database="mysql",
                                      host=mysql_parameters["MysqlAddress"],
                                      port=3306,
                                      user="test",
                                      password=mysql_parameters["DatabasesPassword"],
                                      engine="mysql")
    cursor = conn.cursor()
    cursor.execute("SELECT 1 + 2, 3 + 4")
    first_row = cursor.fetchall()[0]
    assert first_row[0] == 3
    assert first_row[1] == 7
    cursor.close()
    conn.close()


def test_invalid_engine(mysql_parameters):
    with pytest.raises(InvalidEngine):
        Aurora.generate_connection(database="mysql",
                                   host=mysql_parameters["MysqlAddress"],
                                   port=3306,
                                   user="test",
                                   password=mysql_parameters["DatabasesPassword"],
                                   engine="foo")
