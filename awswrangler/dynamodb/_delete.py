"""Amazon DynamoDB Delete Module (PRIVATE)."""

from __future__ import annotations

import logging
from typing import Any

import boto3
from boto3.dynamodb.types import TypeSerializer

from awswrangler import _utils
from awswrangler._config import apply_configs

from ._utils import _TableBatchWriter, _validate_items

_logger: logging.Logger = logging.getLogger(__name__)


@apply_configs
def delete_items(
    items: list[dict[str, Any]],
    table_name: str,
    boto3_session: boto3.Session | None = None,
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
    _logger.debug("Deleting items from DynamoDB table %s", table_name)

    dynamodb_client = _utils.client(service_name="dynamodb", session=boto3_session)
    serializer = TypeSerializer()

    key_schema = dynamodb_client.describe_table(TableName=table_name)["Table"]["KeySchema"]
    _validate_items(items=items, key_schema=key_schema)

    table_keys = [schema["AttributeName"] for schema in key_schema]

    with _TableBatchWriter(table_name, dynamodb_client) as writer:
        for item in items:
            writer.delete_item(
                key={key: serializer.serialize(item[key]) for key in table_keys},
            )
