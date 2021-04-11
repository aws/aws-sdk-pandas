import boto3
import pytest
from tests._utils import get_time_str_with_random_suffix

import awswrangler as wr


@pytest.fixture(scope="function")
def random_glue_database():
    database_name = get_time_str_with_random_suffix()
    yield database_name
    wr.catalog.delete_database(database_name)


def test_create_database(random_glue_database):
    description = "foo"
    glue_client = boto3.client("glue")

    wr.catalog.create_database(name=random_glue_database, description=description)
    r = glue_client.get_database(Name=random_glue_database)
    assert r["Database"]["Name"] == random_glue_database
    assert r["Database"]["Description"] == description

    with pytest.raises(wr.exceptions.AlreadyExists):
        wr.catalog.create_database(name=random_glue_database, description=description)

    description = "bar"
    wr.catalog.create_database(name=random_glue_database, description=description, exist_ok=True)
    r = glue_client.get_database(Name=random_glue_database)
    assert r["Database"]["Name"] == random_glue_database
    assert r["Database"]["Description"] == description
