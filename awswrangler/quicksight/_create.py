"""Amazon QuickSight Create Module."""

import logging
import uuid
from typing import Any, Dict, List, Optional, Union, cast

import boto3

from awswrangler import _utils, exceptions, sts
from awswrangler.quicksight._get_list import get_data_source_arn, get_dataset_id, list_users
from awswrangler.quicksight._utils import extract_athena_query_columns, extract_athena_table_columns

_logger: logging.Logger = logging.getLogger(__name__)

_ALLOWED_ACTIONS: Dict[str, Dict[str, List[str]]] = {
    "data_source": {
        "allowed_to_use": [
            "quicksight:DescribeDataSource",
            "quicksight:DescribeDataSourcePermissions",
            "quicksight:PassDataSource",
        ],
        "allowed_to_manage": [
            "quicksight:DescribeDataSource",
            "quicksight:DescribeDataSourcePermissions",
            "quicksight:PassDataSource",
            "quicksight:UpdateDataSource",
            "quicksight:DeleteDataSource",
            "quicksight:UpdateDataSourcePermissions",
        ],
    },
    "dataset": {
        "allowed_to_use": [
            "quicksight:DescribeDataSet",
            "quicksight:DescribeDataSetPermissions",
            "quicksight:PassDataSet",
            "quicksight:DescribeIngestion",
            "quicksight:ListIngestions",
        ],
        "allowed_to_manage": [
            "quicksight:DescribeDataSet",
            "quicksight:DescribeDataSetPermissions",
            "quicksight:PassDataSet",
            "quicksight:DescribeIngestion",
            "quicksight:ListIngestions",
            "quicksight:UpdateDataSet",
            "quicksight:DeleteDataSet",
            "quicksight:CreateIngestion",
            "quicksight:CancelIngestion",
            "quicksight:UpdateDataSetPermissions",
        ],
    },
}


def _usernames_to_arns(user_names: List[str], all_users: List[Dict[str, Any]]) -> List[str]:
    return [cast(str, u["Arn"]) for u in all_users if u.get("UserName") in user_names]


def _generate_permissions(
    resource: str,
    namespace: str,
    account_id: str,
    boto3_session: boto3.Session,
    allowed_to_use: Optional[List[str]] = None,
    allowed_to_manage: Optional[List[str]] = None,
) -> List[Dict[str, Union[str, List[str]]]]:
    permissions: List[Dict[str, Union[str, List[str]]]] = []
    if (allowed_to_use is None) and (allowed_to_manage is None):
        return permissions

    # Forcing same user not be in both lists at the same time.
    if (allowed_to_use is not None) and (allowed_to_manage is not None):
        allowed_to_use = list(set(allowed_to_use) - set(allowed_to_manage))

    all_users: List[Dict[str, Any]] = list_users(
        namespace=namespace, account_id=account_id, boto3_session=boto3_session
    )

    if allowed_to_use is not None:
        allowed_arns: List[str] = _usernames_to_arns(user_names=allowed_to_use, all_users=all_users)
        permissions += [
            {
                "Principal": arn,
                "Actions": _ALLOWED_ACTIONS[resource]["allowed_to_use"],
            }
            for arn in allowed_arns
        ]
    if allowed_to_manage is not None:
        allowed_arns = _usernames_to_arns(user_names=allowed_to_manage, all_users=all_users)
        permissions += [
            {
                "Principal": arn,
                "Actions": _ALLOWED_ACTIONS[resource]["allowed_to_manage"],
            }
            for arn in allowed_arns
        ]
    return permissions


def _generate_transformations(
    rename_columns: Optional[Dict[str, str]],
    cast_columns_types: Optional[Dict[str, str]],
    tag_columns: Optional[Dict[str, List[Dict[str, Any]]]],
) -> List[Dict[str, Dict[str, Any]]]:
    trans: List[Dict[str, Dict[str, Any]]] = []
    if rename_columns is not None:
        for k, v in rename_columns.items():
            trans.append({"RenameColumnOperation": {"ColumnName": k, "NewColumnName": v}})
    if cast_columns_types is not None:
        for k, v in cast_columns_types.items():
            trans.append({"CastColumnTypeOperation": {"ColumnName": k, "NewColumnType": v.upper()}})
    if tag_columns is not None:
        for k, tags in tag_columns.items():
            trans.append({"TagColumnOperation": {"ColumnName": k, "Tags": tags}})
    return trans


def create_athena_data_source(
    name: str,
    workgroup: str = "primary",
    allowed_to_use: Optional[List[str]] = None,
    allowed_to_manage: Optional[List[str]] = None,
    tags: Optional[Dict[str, str]] = None,
    account_id: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
    namespace: str = "default",
) -> None:
    """Create a QuickSight data source pointing to an Athena/Workgroup.

    Note
    ----
    You will not be able to see the the data source in the console
    if you not pass your user to one of the ``allowed_*`` arguments.

    Parameters
    ----------
    name : str
        Data source name.
    workgroup : str
        Athena workgroup.
    tags : Dict[str, str], optional
        Key/Value collection to put on the Cluster.
        e.g. {"foo": "boo", "bar": "xoo"})
    allowed_to_use : optional
        List of principals that will be allowed to see and use the data source.
        e.g. ["John"]
    allowed_to_manage : optional
        List of principals that will be allowed to see, use, update and delete the data source.
        e.g. ["Mary"]
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    namespace : str
        The namespace. Currently, you should set this to default.

    Returns
    -------
    None
        None.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.quicksight.create_athena_data_source(
    ...     name="...",
    ...     allowed_to_manage=["john"]
    ... )
    """
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    client: boto3.client = _utils.client(service_name="quicksight", session=session)
    if account_id is None:
        account_id = sts.get_account_id(boto3_session=session)
    args: Dict[str, Any] = {
        "AwsAccountId": account_id,
        "DataSourceId": name,
        "Name": name,
        "Type": "ATHENA",
        "DataSourceParameters": {"AthenaParameters": {"WorkGroup": workgroup}},
        "SslProperties": {"DisableSsl": True},
    }
    permissions: List[Dict[str, Union[str, List[str]]]] = _generate_permissions(
        resource="data_source",
        account_id=account_id,
        boto3_session=session,
        allowed_to_use=allowed_to_use,
        allowed_to_manage=allowed_to_manage,
        namespace=namespace,
    )
    if permissions:
        args["Permissions"] = permissions
    if tags is not None:
        _tags: List[Dict[str, str]] = [{"Key": k, "Value": v} for k, v in tags.items()]
        args["Tags"] = _tags
    client.create_data_source(**args)


def create_athena_dataset(
    name: str,
    database: Optional[str] = None,
    table: Optional[str] = None,
    sql: Optional[str] = None,
    sql_name: str = "CustomSQL",
    data_source_name: Optional[str] = None,
    data_source_arn: Optional[str] = None,
    import_mode: str = "DIRECT_QUERY",
    allowed_to_use: Optional[List[str]] = None,
    allowed_to_manage: Optional[List[str]] = None,
    logical_table_alias: str = "LogicalTable",
    rename_columns: Optional[Dict[str, str]] = None,
    cast_columns_types: Optional[Dict[str, str]] = None,
    tag_columns: Optional[Dict[str, List[Dict[str, Any]]]] = None,
    tags: Optional[Dict[str, str]] = None,
    account_id: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
    namespace: str = "default",
) -> str:
    """Create a QuickSight dataset.

    Note
    ----
    You will not be able to see the the dataset in the console
    if you not pass your username to one of the ``allowed_*`` arguments.

    Note
    ----
    You must pass ``database``/``table`` OR ``sql`` argument.

    Note
    ----
    You must pass ``data_source_name`` OR ``data_source_arn`` argument.

    Parameters
    ----------
    name : str
        Dataset name.
    database : str
        Athena's database name.
    table : str
        Athena's table name.
    sql : str
        Use a SQL query to define your table.
    sql_name : str
        Query name.
    data_source_name : str, optional
        QuickSight data source name.
    data_source_arn : str, optional
        QuickSight data source ARN.
    import_mode : str
        Indicates whether you want to import the data into SPICE.
        'SPICE'|'DIRECT_QUERY'
    tags : Dict[str, str], optional
        Key/Value collection to put on the Cluster.
        e.g. {"foo": "boo", "bar": "xoo"}
    allowed_to_use : optional
        List of usernames that will be allowed to see and use the data source.
        e.g. ["john", "Mary"]
    allowed_to_manage : optional
        List of usernames that will be allowed to see, use, update and delete the data source.
        e.g. ["Mary"]
    logical_table_alias : str
        A display name for the logical table.
    rename_columns : Dict[str, str], optional
        Dictionary to map column renames. e.g. {"old_name": "new_name", "old_name2": "new_name2"}
    cast_columns_types : Dict[str, str], optional
        Dictionary to map column casts. e.g. {"col_name": "STRING", "col_name2": "DECIMAL"}
        Valid types: 'STRING'|'INTEGER'|'DECIMAL'|'DATETIME'
    tag_columns : Dict[str, List[Dict[str, Any]]], optional
        Dictionary to map column tags.
        e.g. {"col_name": [{ "ColumnGeographicRole": "CITY" }],
              "col_name2": [{ "ColumnDescription": { "Text": "description" }}]}
        Valid geospatial roles: 'COUNTRY'|'STATE'|'COUNTY'|'CITY'|'POSTCODE'|'LONGITUDE'|'LATITUDE'
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    namespace : str
        The namespace. Currently, you should set this to default.

    Returns
    -------
    str
        Dataset ID.

    Examples
    --------
    >>> import awswrangler as wr
    >>> dataset_id = wr.quicksight.create_athena_dataset(
    ...     name="...",
    ...     database="..."
    ...     table="..."
    ...     data_source_name="..."
    ...     allowed_to_manage=["Mary"]
    ... )
    """
    if (data_source_name is None) and (data_source_arn is None):
        raise exceptions.InvalidArgument("You must pass a not None data_source_name or data_source_arn argument.")
    if ((database is None) and (table is None)) and (sql is None):
        raise exceptions.InvalidArgument("You must pass database/table OR sql argument.")
    if (database is not None) and (sql is not None):
        raise exceptions.InvalidArgument(
            "If you provide sql argument, please include the database name inside the sql statement."
            "Do NOT pass in with database argument."
        )
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    client: boto3.client = _utils.client(service_name="quicksight", session=session)
    if account_id is None:
        account_id = sts.get_account_id(boto3_session=session)
    if (data_source_arn is None) and (data_source_name is not None):
        data_source_arn = get_data_source_arn(name=data_source_name, account_id=account_id, boto3_session=session)
    if sql is not None:
        physical_table: Dict[str, Dict[str, Any]] = {
            "CustomSql": {
                "DataSourceArn": data_source_arn,
                "Name": sql_name,
                "SqlQuery": sql,
                "Columns": extract_athena_query_columns(
                    sql=sql,
                    data_source_arn=data_source_arn,  # type: ignore
                    account_id=account_id,
                    boto3_session=session,
                ),
            }
        }
    else:
        physical_table = {
            "RelationalTable": {
                "DataSourceArn": data_source_arn,
                "Schema": database,
                "Name": table,
                "InputColumns": extract_athena_table_columns(
                    database=database,  # type: ignore
                    table=table,  # type: ignore
                    boto3_session=session,
                ),
            }
        }
    table_uuid: str = uuid.uuid4().hex
    dataset_id: str = uuid.uuid4().hex
    args: Dict[str, Any] = {
        "AwsAccountId": account_id,
        "DataSetId": dataset_id,
        "Name": name,
        "ImportMode": import_mode,
        "PhysicalTableMap": {table_uuid: physical_table},
        "LogicalTableMap": {table_uuid: {"Alias": logical_table_alias, "Source": {"PhysicalTableId": table_uuid}}},
    }
    trans: List[Dict[str, Dict[str, Any]]] = _generate_transformations(
        rename_columns=rename_columns, cast_columns_types=cast_columns_types, tag_columns=tag_columns
    )
    if trans:
        args["LogicalTableMap"][table_uuid]["DataTransforms"] = trans
    permissions: List[Dict[str, Union[str, List[str]]]] = _generate_permissions(
        resource="dataset",
        account_id=account_id,
        boto3_session=session,
        allowed_to_use=allowed_to_use,
        allowed_to_manage=allowed_to_manage,
        namespace=namespace,
    )
    if permissions:
        args["Permissions"] = permissions
    if tags is not None:
        _tags: List[Dict[str, str]] = [{"Key": k, "Value": v} for k, v in tags.items()]
        args["Tags"] = _tags
    client.create_data_set(**args)
    return dataset_id


def create_ingestion(
    dataset_name: Optional[str] = None,
    dataset_id: Optional[str] = None,
    ingestion_id: Optional[str] = None,
    account_id: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> str:
    """Create and starts a new SPICE ingestion on a dataset.

    Note
    ----
    You must pass ``dataset_name`` OR ``dataset_id`` argument.

    Parameters
    ----------
    dataset_name : str, optional
        Dataset name.
    dataset_id : str, optional
        Dataset ID.
    ingestion_id : str, optional
        Ingestion ID.
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    str
        Ingestion ID

    Examples
    --------
    >>> import awswrangler as wr
    >>> status = wr.quicksight.create_ingestion("my_dataset")
    """
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    if account_id is None:
        account_id = sts.get_account_id(boto3_session=session)
    if (dataset_name is None) and (dataset_id is None):
        raise exceptions.InvalidArgument("You must pass a not None dataset_name or dataset_id argument.")
    if (dataset_id is None) and (dataset_name is not None):
        dataset_id = get_dataset_id(name=dataset_name, account_id=account_id, boto3_session=session)
    if ingestion_id is None:
        ingestion_id = uuid.uuid4().hex
    client: boto3.client = _utils.client(service_name="quicksight", session=session)
    response: Dict[str, Any] = client.create_ingestion(
        DataSetId=dataset_id, IngestionId=ingestion_id, AwsAccountId=account_id
    )
    return cast(str, response["IngestionId"])
