import logging
import uuid

import boto3
import pytest

import awswrangler as wr

from .._utils import get_df_quicksight

logging.getLogger("awswrangler").setLevel(logging.DEBUG)

client = boto3.client("quicksight")


@pytest.mark.xfail(raises=client.exceptions.ConflictException)
def test_quicksight(path, glue_database, glue_table):
    df = get_df_quicksight()
    wr.s3.to_parquet(
        df=df, path=path, dataset=True, database=glue_database, table=glue_table, partition_cols=["par0", "par1"]
    )
    resource_name = f"test{str(uuid.uuid4())[:8]}"

    wr.quicksight.delete_all_dashboards(regex_filter="test.*")
    wr.quicksight.delete_all_datasets(regex_filter="test.*")
    wr.quicksight.delete_all_data_sources(regex_filter="test.*")
    wr.quicksight.delete_all_templates(regex_filter="test.*")

    wr.quicksight.create_athena_data_source(
        name=resource_name, allowed_to_manage=[wr.sts.get_current_identity_name()], tags={"Env": "aws-sdk-pandas"}
    )
    assert wr.quicksight.describe_data_source(resource_name)["Name"] == resource_name
    assert (
        wr.quicksight.describe_data_source_permissions(resource_name)[0]["Principal"].endswith(
            wr.sts.get_current_identity_name()
        )
        is True
    )
    dataset_name = f"{resource_name}-table"
    wr.quicksight.create_athena_dataset(
        name=dataset_name,
        database=glue_database,
        table=glue_table,
        data_source_name=resource_name,
        allowed_to_manage=[wr.sts.get_current_identity_name()],
        rename_columns={"iint16": "new_col"},
        cast_columns_types={"new_col": "STRING"},
        tag_columns={"string": [{"ColumnGeographicRole": "CITY"}, {"ColumnDescription": {"Text": "some description"}}]},
    )
    assert wr.quicksight.describe_dataset(dataset_name)["Name"] == dataset_name

    wr.quicksight.create_athena_dataset(
        name=f"{resource_name}-sql",
        sql=f"SELECT * FROM {glue_database}.{glue_table}",
        data_source_name=resource_name,
        import_mode="SPICE",
        allowed_to_use=[wr.sts.get_current_identity_name()],
        allowed_to_manage=[wr.sts.get_current_identity_name()],
        rename_columns={"iint16": "new_col"},
        cast_columns_types={"new_col": "STRING"},
        tag_columns={"string": [{"ColumnGeographicRole": "CITY"}, {"ColumnDescription": {"Text": "some description"}}]},
        tags={"foo": "boo"},
    )

    ingestion_id = wr.quicksight.create_ingestion(f"{resource_name}-sql")
    status = None
    while status not in ["FAILED", "COMPLETED", "CANCELLED"]:
        status = wr.quicksight.describe_ingestion(ingestion_id, f"{resource_name}-sql")["IngestionStatus"]
    assert status == "COMPLETED"

    ingestion_id = wr.quicksight.create_ingestion(f"{resource_name}-sql")
    wr.quicksight.cancel_ingestion(ingestion_id, f"{resource_name}-sql")
    assert len(wr.quicksight.list_ingestions(f"{resource_name}-sql")) == 3

    wr.quicksight.list_groups()
    wr.quicksight.list_iam_policy_assignments()
    wr.quicksight.list_iam_policy_assignments_for_user(wr.sts.get_current_identity_name())
    wr.quicksight.list_user_groups(wr.sts.get_current_identity_name())
    wr.quicksight.list_users()
    wr.quicksight.get_dataset_ids(f"{resource_name}-sql")
    wr.quicksight.get_data_source_ids("test")

    wr.quicksight.delete_all_datasets(regex_filter="test.*")
    wr.quicksight.delete_all_data_sources(regex_filter="test.*")


def test_quicksight_delete_all_datasources_filter():
    wr.quicksight.delete_all_data_sources(regex_filter="test.*")
    resource_name = "test-delete"
    wr.quicksight.create_athena_data_source(
        name=resource_name, allowed_to_manage=[wr.sts.get_current_identity_name()], tags={"Env": "aws-sdk-pandas"}
    )
    wr.quicksight.delete_all_data_sources(regex_filter="test-no-delete")

    assert len(wr.quicksight.get_data_source_ids(resource_name)) == 1

    wr.quicksight.delete_all_data_sources(regex_filter="test-delete.*")
    assert len(wr.quicksight.get_data_source_ids(resource_name)) == 0


def test_quicksight_delete_all_datasets(path, glue_database, glue_table):
    df = get_df_quicksight()
    wr.s3.to_parquet(
        df=df, path=path, dataset=True, database=glue_database, table=glue_table, partition_cols=["par0", "par1"]
    )
    wr.quicksight.delete_all_datasets(regex_filter="test.*")
    wr.quicksight.delete_all_data_sources(regex_filter="test.*")

    resource_name = f"test{str(uuid.uuid4())[:8]}"
    wr.quicksight.create_athena_data_source(
        name=resource_name, allowed_to_manage=[wr.sts.get_current_identity_name()], tags={"Env": "aws-sdk-pandas"}
    )
    wr.quicksight.create_athena_dataset(
        name=f"{resource_name}-sql",
        sql=f"SELECT * FROM {glue_database}.{glue_table}",
        data_source_name=resource_name,
        import_mode="SPICE",
        allowed_to_use=[wr.sts.get_current_identity_name()],
        allowed_to_manage=[wr.sts.get_current_identity_name()],
        rename_columns={"iint16": "new_col"},
        cast_columns_types={"new_col": "STRING"},
        tag_columns={"string": [{"ColumnGeographicRole": "CITY"}, {"ColumnDescription": {"Text": "some description"}}]},
        tags={"foo": "boo"},
    )
    wr.quicksight.delete_all_datasets(regex_filter="test.*")
    wr.quicksight.delete_all_data_sources(regex_filter="test.*")

    assert len(wr.quicksight.get_dataset_ids(resource_name)) == 0
