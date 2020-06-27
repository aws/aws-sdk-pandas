import logging

import pytest

import awswrangler as wr

from ._utils import extract_cloudformation_outputs, get_time_str_with_random_suffix

logging.basicConfig(level=logging.INFO, format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)
logging.getLogger("botocore.credentials").setLevel(logging.CRITICAL)


@pytest.fixture(scope="module")
def cloudformation_outputs():
    yield extract_cloudformation_outputs()


@pytest.fixture(scope="module")
def glue_database(cloudformation_outputs):
    yield cloudformation_outputs["GlueDatabaseName"]


@pytest.fixture(scope="function")
def glue_table(glue_database):
    name = f"tbl_{get_time_str_with_random_suffix()}"
    print(f"Table name: {name}")
    wr.catalog.delete_table_if_exists(database=glue_database, table=name)
    yield name
    wr.catalog.delete_table_if_exists(database=glue_database, table=name)


def test_catalog_basics(glue_database, glue_table):
    args = {"table": glue_table, "path": "", "columns_types": {"col0": "bigint"}}

    # Missing database argument
    with pytest.raises(TypeError):
        wr.catalog.create_parquet_table(**args)

    # Configuring default database value
    wr.config.database = glue_database

    # Testing configured database
    wr.catalog.create_parquet_table(**args)

    # Resetting all configs
    wr.config.reset()

    # Missing database argument
    with pytest.raises(TypeError):
        wr.catalog.does_table_exist(table=glue_table)

    # Configuring default database value again
    wr.config.database = glue_database

    # Testing configured database again
    assert wr.catalog.does_table_exist(table=glue_table) is True

    # Resetting this specific config
    wr.config.reset("database")

    # Missing database argument
    with pytest.raises(TypeError):
        wr.catalog.does_table_exist(table=glue_table)
