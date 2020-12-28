"""Amazon DynamoDB Delete Module (PRIVATE)."""

import logging
from typing import Dict, List, Optional, Union

import boto3

from ._utils import _validate_items, get_table

_logger: logging.Logger = logging.getLogger(__name__)


def delete_items(
    items: List[Dict[str, Union[str, int, float, bool]]],
    table_name: str,
    boto3_session: Optional[boto3.Session] = None,
) -> None:
    """Delete all items in the specified DynamoDB table.

    Parameters
    ----------
    items : List[Dict[str, Union[str, int, float, bool]]]
        List which contains the items that will be inserted.
    table_name : str
        Name of the Amazon DynamoDB table.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.

    Returns
    -------
    None
        None.
    """
    _logger.debug("Deleting items from DynamoDB table")

    dynamodb_table = get_table(table_name=table_name, boto3_session=boto3_session)
    _validate_items(items=items, dynamodb_table=dynamodb_table)
    table_keys = [schema["AttributeName"] for schema in dynamodb_table.key_schema]
    with dynamodb_table.batch_writer() as writer:
        for item in items:
            writer.delete_item(Key={key: item[key] for key in table_keys})
