import logging

import pytest

import awswrangler as wr

from ._utils import extract_cloudformation_outputs, get_df_quicksight, get_time_str_with_random_suffix, path_generator

logging.basicConfig(level=logging.INFO, format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)
logging.getLogger("botocore.credentials").setLevel(logging.CRITICAL)


@pytest.fixture(scope="module")
def cloudformation_outputs():
    yield extract_cloudformation_outputs()


@pytest.fixture(scope="module")
def bucket(cloudformation_outputs):
    if "BucketName" in cloudformation_outputs:
        bucket = cloudformation_outputs["BucketName"]
    else:
        raise Exception("You must deploy/update the test infrastructure (CloudFormation)")
    yield bucket


@pytest.fixture(scope="module")
def database(cloudformation_outputs):
    yield cloudformation_outputs["GlueDatabaseName"]


@pytest.fixture(scope="function")
def table(database):
    name = f"tbl_{get_time_str_with_random_suffix()}"
    print(f"Table name: {name}")
    wr.catalog.delete_table_if_exists(database=database, table=name)
    yield name
    wr.catalog.delete_table_if_exists(database=database, table=name)


@pytest.fixture(scope="function")
def path(bucket):
    yield from path_generator(bucket)


def test_quicksight(path, database, table):
    df = get_df_quicksight()
    paths = wr.s3.to_parquet(
        df=df, path=path, dataset=True, database=database, table=table, partition_cols=["par0", "par1"]
    )["paths"]
    wr.s3.wait_objects_exist(paths, use_threads=False)

    wr.quicksight.delete_all_dashboards()
    wr.quicksight.delete_all_datasets()
    wr.quicksight.delete_all_data_sources()
    wr.quicksight.delete_all_templates()

    wr.quicksight.create_athena_data_source(
        name="test", allowed_to_manage=[wr.sts.get_current_identity_name()], tags={"Env": "aws-data-wrangler"}
    )
    assert wr.quicksight.describe_data_source("test")["Name"] == "test"
    assert (
        wr.quicksight.describe_data_source_permissions("test")[0]["Principal"].endswith(
            wr.sts.get_current_identity_name()
        )
        is True
    )

    wr.quicksight.create_athena_dataset(
        name="test-table",
        database=database,
        table=table,
        data_source_name="test",
        allowed_to_manage=[wr.sts.get_current_identity_name()],
        rename_columns={"iint16": "new_col"},
        cast_columns_types={"new_col": "STRING"},
    )
    assert wr.quicksight.describe_dataset("test-table")["Name"] == "test-table"

    wr.quicksight.create_athena_dataset(
        name="test-sql",
        sql=f"SELECT * FROM {database}.{table}",
        data_source_name="test",
        import_mode="SPICE",
        allowed_to_use=[wr.sts.get_current_identity_name()],
        allowed_to_manage=[wr.sts.get_current_identity_name()],
        rename_columns={"iint16": "new_col"},
        cast_columns_types={"new_col": "STRING"},
        tags={"foo": "boo"},
    )

    ingestion_id = wr.quicksight.create_ingestion("test-sql")
    status = None
    while status not in ["FAILED", "COMPLETED", "CANCELLED"]:
        status = wr.quicksight.describe_ingestion(ingestion_id, "test-sql")["IngestionStatus"]
    assert status == "COMPLETED"

    ingestion_id = wr.quicksight.create_ingestion("test-sql")
    wr.quicksight.cancel_ingestion(ingestion_id, "test-sql")
    assert len(wr.quicksight.list_ingestions("test-sql")) == 3

    wr.quicksight.list_groups()
    wr.quicksight.list_iam_policy_assignments()
    wr.quicksight.list_iam_policy_assignments_for_user(wr.sts.get_current_identity_name())
    wr.quicksight.list_user_groups(wr.sts.get_current_identity_name())
    wr.quicksight.list_users()
    wr.quicksight.get_dataset_ids("test-sql")
    wr.quicksight.get_data_source_ids("test")

    wr.quicksight.delete_all_datasets()
    wr.quicksight.delete_all_data_sources()
