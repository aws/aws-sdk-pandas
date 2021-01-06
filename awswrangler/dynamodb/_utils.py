"""Amazon DynamoDB Utils Module (PRIVATE)."""

from typing import Any, Dict, List, Mapping, Optional, Union

import boto3

from awswrangler import _utils, exceptions


def get_table(
    table_name: str,
    boto3_session: Optional[boto3.Session] = None,
) -> boto3.resource:
    """Get DynamoDB table object for specified table name.

    Parameters
    ----------
    table_name : str
        Name of the Amazon DynamoDB table.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.

    Returns
    -------
    dynamodb_table : boto3.resources.dynamodb.Table
        Boto3 DynamoDB.Table object.
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table
    """
    dynamodb_resource = _utils.resource(service_name="dynamodb", session=boto3_session)
    dynamodb_table = dynamodb_resource.Table(table_name)

    return dynamodb_table


def _validate_items(
    items: Union[List[Dict[str, Any]], List[Mapping[str, Any]]], dynamodb_table: boto3.resource
) -> None:
    """Validate if all items have the required keys for the Amazon DynamoDB table.

    Parameters
    ----------
    items : Union[List[Dict[str, Any]], List[Mapping[str, Any]]]
        List which contains the items that will be validated.
    dynamodb_table : boto3.resources.dynamodb.Table
        Amazon DynamoDB Table object.

    Returns
    -------
    None
        None.
    """
    table_keys = [schema["AttributeName"] for schema in dynamodb_table.key_schema]
    if not all(key in item for item in items for key in table_keys):
        raise exceptions.InvalidArgumentValue("All items need to contain the required keys for the table.")
