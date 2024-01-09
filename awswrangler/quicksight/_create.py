"""Amazon QuickSight Create Module."""

from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING, Any, Callable, List, Literal, Optional, TypeVar, Union, cast

import boto3

from awswrangler import _utils, exceptions, sts
from awswrangler.quicksight._get_list import get_data_source_arn, get_dataset_id, list_groups, list_users
from awswrangler.quicksight._utils import (
    _QuicksightPrincipalList,
    extract_athena_query_columns,
    extract_athena_table_columns,
)

if TYPE_CHECKING:
    from mypy_boto3_quicksight.type_defs import GroupTypeDef, UserTypeDef


_logger: logging.Logger = logging.getLogger(__name__)


_ALLOWED_ACTIONS: dict[str, dict[str, list[str]]] = {
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


def _groupnames_to_arns(group_names: set[str], all_groups: list["GroupTypeDef"]) -> list[str]:
    return [u["Arn"] for u in all_groups if u.get("GroupName") in group_names]


def _usernames_to_arns(user_names: set[str], all_users: list["UserTypeDef"]) -> list[str]:
    return [u["Arn"] for u in all_users if u.get("UserName") in user_names]


_PrincipalTypeDef = TypeVar("_PrincipalTypeDef", "UserTypeDef", "GroupTypeDef")


def _generate_permissions_base(
    resource: str,
    namespace: str,
    account_id: str,
    boto3_session: boto3.Session | None,
    allowed_to_use: list[str] | None,
    allowed_to_manage: list[str] | None,
    principal_names_to_arns_func: Callable[[set[str], list[_PrincipalTypeDef]], list[str]],
    list_principals: Callable[[str, str, boto3.Session | None], list[dict[str, Any]]],
) -> list[dict[str, str | list[str]]]:
    permissions: list[dict[str, str | list[str]]] = []
    if (allowed_to_use is None) and (allowed_to_manage is None):
        return permissions

    allowed_to_use_set = set(allowed_to_use) if allowed_to_use else None
    allowed_to_manage_set = set(allowed_to_manage) if allowed_to_manage else None

    # Forcing same principal not be in both lists at the same time.
    if (allowed_to_use_set is not None) and (allowed_to_manage_set is not None):
        allowed_to_use_set = allowed_to_use_set - allowed_to_manage_set

    all_principals = cast(List[_PrincipalTypeDef], list_principals(namespace, account_id, boto3_session))

    if allowed_to_use_set is not None:
        allowed_arns: list[str] = principal_names_to_arns_func(allowed_to_use_set, all_principals)
        permissions += [
            {
                "Principal": arn,
                "Actions": _ALLOWED_ACTIONS[resource]["allowed_to_use"],
            }
            for arn in allowed_arns
        ]
    if allowed_to_manage_set is not None:
        allowed_arns = principal_names_to_arns_func(allowed_to_manage_set, all_principals)
        permissions += [
            {
                "Principal": arn,
                "Actions": _ALLOWED_ACTIONS[resource]["allowed_to_manage"],
            }
            for arn in allowed_arns
        ]
    return permissions


def _generate_permissions(
    resource: str,
    namespace: str,
    account_id: str,
    boto3_session: boto3.Session | None,
    allowed_users_to_use: list[str] | None = None,
    allowed_groups_to_use: list[str] | None = None,
    allowed_users_to_manage: list[str] | None = None,
    allowed_groups_to_manage: list[str] | None = None,
) -> list[dict[str, str | list[str]]]:
    permissions_users = _generate_permissions_base(
        resource=resource,
        namespace=namespace,
        account_id=account_id,
        boto3_session=boto3_session,
        allowed_to_use=allowed_users_to_use,
        allowed_to_manage=allowed_users_to_manage,
        principal_names_to_arns_func=_usernames_to_arns,
        list_principals=list_users,
    )

    permissions_groups = _generate_permissions_base(
        resource=resource,
        namespace=namespace,
        account_id=account_id,
        boto3_session=boto3_session,
        allowed_to_use=allowed_groups_to_use,
        allowed_to_manage=allowed_groups_to_manage,
        principal_names_to_arns_func=_groupnames_to_arns,
        list_principals=list_groups,
    )

    return permissions_users + permissions_groups


def _generate_transformations(
    rename_columns: dict[str, str] | None,
    cast_columns_types: dict[str, str] | None,
    tag_columns: dict[str, list[dict[str, Any]]] | None,
) -> list[dict[str, dict[str, Any]]]:
    trans: list[dict[str, dict[str, Any]]] = []
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


_AllowedType = Optional[Union[List[str], _QuicksightPrincipalList]]


def _get_principal_names(principals: _AllowedType, type: Literal["users", "groups"]) -> list[str] | None:
    if principals is None:
        return None

    if isinstance(principals, list):
        if type == "users":
            return principals
        else:
            return None

    return principals.get(type)


def create_athena_data_source(
    name: str,
    workgroup: str = "primary",
    allowed_to_use: _AllowedType = None,
    allowed_to_manage: _AllowedType = None,
    tags: dict[str, str] | None = None,
    account_id: str | None = None,
    boto3_session: boto3.Session | None = None,
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
        e.g. ```{"foo": "boo", "bar": "xoo"})```
    allowed_to_use: dict["users" | "groups", list[str]], optional
        Dictionary containing usernames and groups that will be allowed to see and
        use the data.
        e.g. ```{"users": ["john", "Mary"], "groups": ["engineering", "customers"]}```
        Alternatively, if a list of string is passed,
        it will be interpreted as a list of usernames only.
    allowed_to_manage: dict["users" | "groups", list[str]], optional
        Dictionary containing usernames and groups that will be allowed to see, use,
        update and delete the data source.
        e.g. ```{"users": ["Mary"], "groups": ["engineering"]}```
        Alternatively, if a list of string is passed,
        it will be interpreted as a list of usernames only.
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
    client = _utils.client(service_name="quicksight", session=boto3_session)
    if account_id is None:
        account_id = sts.get_account_id(boto3_session=boto3_session)
    args: dict[str, Any] = {
        "AwsAccountId": account_id,
        "DataSourceId": name,
        "Name": name,
        "Type": "ATHENA",
        "DataSourceParameters": {"AthenaParameters": {"WorkGroup": workgroup}},
        "SslProperties": {"DisableSsl": True},
    }
    permissions: list[dict[str, str | list[str]]] = _generate_permissions(
        resource="data_source",
        namespace=namespace,
        account_id=account_id,
        boto3_session=boto3_session,
        allowed_users_to_use=_get_principal_names(allowed_to_use, "users"),
        allowed_users_to_manage=_get_principal_names(allowed_to_manage, "users"),
        allowed_groups_to_use=_get_principal_names(allowed_to_use, "groups"),
        allowed_groups_to_manage=_get_principal_names(allowed_to_manage, "groups"),
    )
    if permissions:
        args["Permissions"] = permissions
    if tags is not None:
        _tags: list[dict[str, str]] = [{"Key": k, "Value": v} for k, v in tags.items()]
        args["Tags"] = _tags
    client.create_data_source(**args)


def create_athena_dataset(
    name: str,
    database: str | None = None,
    table: str | None = None,
    sql: str | None = None,
    sql_name: str | None = None,
    data_source_name: str | None = None,
    data_source_arn: str | None = None,
    import_mode: Literal["SPICE", "DIRECT_QUERY"] = "DIRECT_QUERY",
    allowed_to_use: _AllowedType = None,
    allowed_to_manage: _AllowedType = None,
    logical_table_alias: str = "LogicalTable",
    rename_columns: dict[str, str] | None = None,
    cast_columns_types: dict[str, str] | None = None,
    tag_columns: dict[str, list[dict[str, Any]]] | None = None,
    tags: dict[str, str] | None = None,
    account_id: str | None = None,
    boto3_session: boto3.Session | None = None,
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
    sql_name : str, optional
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
    allowed_to_use: dict["users" | "groups", list[str]], optional
        Dictionary containing usernames and groups that will be allowed to see and
        use the data.
        e.g. ```{"users": ["john", "Mary"], "groups": ["engineering", "customers"]}```
        Alternatively, if a list of string is passed,
        it will be interpreted as a list of usernames only.
    allowed_to_manage: dict["users" | "groups", list[str]], optional
        Dictionary containing usernames and groups that will be allowed to see, use,
        update and delete the data source.
        e.g. ```{"users": ["Mary"], "groups": ["engineering"]}```
        Alternatively, if a list of string is passed,
        it will be interpreted as a list of usernames only.
    logical_table_alias : str
        A display name for the logical table.
    rename_columns : Dict[str, str], optional
        Dictionary to map column renames. e.g. {"old_name": "new_name", "old_name2": "new_name2"}
    cast_columns_types : Dict[str, str], optional
        Dictionary to map column casts. e.g. {"col_name": "STRING", "col_name2": "DECIMAL"}
        Valid types: 'STRING'|'INTEGER'|'DECIMAL'|'DATETIME'
    tag_columns : Dict[str, List[Dict[str, Any]]], optional
        Dictionary to map column tags.
        e.g. {"col_name": [{ "ColumnGeographicRole": "CITY" }],"col_name2": [{ "ColumnDescription": { "Text": "description" }}]}
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
    client = _utils.client(service_name="quicksight", session=boto3_session)
    if account_id is None:
        account_id = sts.get_account_id(boto3_session=boto3_session)
    if (data_source_arn is None) and (data_source_name is not None):
        data_source_arn = get_data_source_arn(name=data_source_name, account_id=account_id, boto3_session=boto3_session)
    if sql is not None:
        physical_table: dict[str, dict[str, Any]] = {
            "CustomSql": {
                "DataSourceArn": data_source_arn,
                "Name": sql_name if sql_name else f"CustomSQL-{uuid.uuid4().hex[:8]}",
                "SqlQuery": sql,
                "Columns": extract_athena_query_columns(
                    sql=sql,
                    data_source_arn=data_source_arn,  # type: ignore[arg-type]
                    account_id=account_id,
                    boto3_session=boto3_session,
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
                    database=database,  # type: ignore[arg-type]
                    table=table,  # type: ignore[arg-type]
                    boto3_session=boto3_session,
                ),
            }
        }
    table_uuid: str = uuid.uuid4().hex
    dataset_id: str = uuid.uuid4().hex
    args: dict[str, Any] = {
        "AwsAccountId": account_id,
        "DataSetId": dataset_id,
        "Name": name,
        "ImportMode": import_mode,
        "PhysicalTableMap": {table_uuid: physical_table},
        "LogicalTableMap": {table_uuid: {"Alias": logical_table_alias, "Source": {"PhysicalTableId": table_uuid}}},
    }
    trans: list[dict[str, dict[str, Any]]] = _generate_transformations(
        rename_columns=rename_columns, cast_columns_types=cast_columns_types, tag_columns=tag_columns
    )
    if trans:
        args["LogicalTableMap"][table_uuid]["DataTransforms"] = trans

    permissions: list[dict[str, str | list[str]]] = _generate_permissions(
        resource="dataset",
        namespace=namespace,
        account_id=account_id,
        boto3_session=boto3_session,
        allowed_users_to_use=_get_principal_names(allowed_to_use, "users"),
        allowed_users_to_manage=_get_principal_names(allowed_to_manage, "users"),
        allowed_groups_to_use=_get_principal_names(allowed_to_use, "groups"),
        allowed_groups_to_manage=_get_principal_names(allowed_to_manage, "groups"),
    )
    if permissions:
        args["Permissions"] = permissions
    if tags is not None:
        _tags: list[dict[str, str]] = [{"Key": k, "Value": v} for k, v in tags.items()]
        args["Tags"] = _tags
    client.create_data_set(**args)
    return dataset_id


def create_ingestion(
    dataset_name: str | None = None,
    dataset_id: str | None = None,
    ingestion_id: str | None = None,
    account_id: str | None = None,
    boto3_session: boto3.Session | None = None,
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
    if account_id is None:
        account_id = sts.get_account_id(boto3_session=boto3_session)
    if (dataset_name is None) and (dataset_id is None):
        raise exceptions.InvalidArgument("You must pass a not None dataset_name or dataset_id argument.")
    if (dataset_id is None) and (dataset_name is not None):
        dataset_id = get_dataset_id(name=dataset_name, account_id=account_id, boto3_session=boto3_session)
    if ingestion_id is None:
        ingestion_id = uuid.uuid4().hex

    client = _utils.client(service_name="quicksight", session=boto3_session)
    dataset_id = cast(str, dataset_id)

    response = client.create_ingestion(DataSetId=dataset_id, IngestionId=ingestion_id, AwsAccountId=account_id)
    return response["IngestionId"]
