"""Amazon DynamoDB Delete Module (PRIVATE)."""

import logging
from typing import Any, Dict, List, Optional

import boto3

from ._utils import _validate_items, get_table

_logger: logging.Logger = logging.getLogger(__name__)


def delete_items(
    items: List[Dict[str, Any]],
    table_name: str,
    boto3_session: Optional[boto3.Session] = None,
) -> None:
    """Delete all items in the specified DynamoDB table.

    Parameters
    ----------
    items : List[Dict[str, Any]]
        List which contains the items that will be deleted.
    table_name : str
        Name of the Amazon DynamoDB table.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.

    Returns
    -------
    None
        None.

    Examples
    --------
    Writing rows of DataFrame

    >>> import awswrangler as wr
    >>> wr.dynamodb.delete_items(
    ...     items=[{'key': 1}, {'key': 2, 'value': 'Hello'}],
    ...     table_name='table'
    ... )
    """
    _logger.debug("Deleting items from DynamoDB table")

    dynamodb_table = get_table(table_name=table_name, boto3_session=boto3_session)
    _validate_items(items=items, dynamodb_table=dynamodb_table)
    table_keys = [schema["AttributeName"] for schema in dynamodb_table.key_schema]
    with dynamodb_table.batch_writer() as writer:
        for item in items:
            writer.delete_item(Key={key: item[key] for key in table_keys})
