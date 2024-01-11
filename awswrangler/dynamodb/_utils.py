"""Amazon DynamoDB Utils Module (PRIVATE)."""

from __future__ import annotations

import logging
from types import TracebackType
from typing import TYPE_CHECKING, Any, Iterator, Mapping, TypedDict

import boto3
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.exceptions import ClientError
from typing_extensions import NotRequired, Required

from awswrangler import _utils, exceptions
from awswrangler._config import apply_configs
from awswrangler.annotations import Deprecated

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.client import DynamoDBClient
    from mypy_boto3_dynamodb.service_resource import Table
    from mypy_boto3_dynamodb.type_defs import (
        AttributeValueTypeDef,
        ExecuteStatementOutputTypeDef,
        KeySchemaElementTypeDef,
        TableAttributeValueTypeDef,
        WriteRequestTypeDef,
    )


_logger: logging.Logger = logging.getLogger(__name__)


@apply_configs
@Deprecated
def get_table(
    table_name: str,
    boto3_session: boto3.Session | None = None,
) -> "Table":
    """Get DynamoDB table object for specified table name.

    Parameters
    ----------
    table_name : str
        Name of the Amazon DynamoDB table.
    boto3_session : Optional[boto3.Session()]
        Boto3 Session. If None, the default boto3 Session is used.

    Returns
    -------
    dynamodb_table : boto3.resources.dynamodb.Table
        Boto3 DynamoDB.Table object.
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table
    """
    dynamodb_resource = _utils.resource(service_name="dynamodb", session=boto3_session)
    dynamodb_table = dynamodb_resource.Table(table_name)

    return dynamodb_table


def _serialize_item(
    item: Mapping[str, "TableAttributeValueTypeDef"], serializer: TypeSerializer | None = None
) -> dict[str, "AttributeValueTypeDef"]:
    serializer = serializer if serializer else TypeSerializer()
    return {k: serializer.serialize(v) for k, v in item.items()}


def _deserialize_item(
    item: Mapping[str, "AttributeValueTypeDef"], deserializer: TypeDeserializer | None = None
) -> dict[str, "TableAttributeValueTypeDef"]:
    deserializer = deserializer if deserializer else TypeDeserializer()
    return {k: deserializer.deserialize(v) for k, v in item.items()}


class _ReadExecuteStatementKwargs(TypedDict):
    Statement: Required[str]
    ConsistentRead: Required[bool]
    Parameters: NotRequired[list["AttributeValueTypeDef"]]
    NextToken: NotRequired[str]


def _execute_statement(
    kwargs: _ReadExecuteStatementKwargs,
    dynamodb_client: "DynamoDBClient",
) -> "ExecuteStatementOutputTypeDef":
    try:
        response = dynamodb_client.execute_statement(**kwargs)
    except ClientError as err:
        if err.response["Error"]["Code"] == "ResourceNotFoundException":
            _logger.error("Couldn't execute PartiQL: '%s' because the table does not exist.", kwargs["Statement"])
        else:
            _logger.error(
                "Couldn't execute PartiQL: '%s'. %s: %s",
                kwargs["Statement"],
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
        raise
    return response


def _read_execute_statement(
    kwargs: _ReadExecuteStatementKwargs,
    dynamodb_client: "DynamoDBClient",
) -> Iterator[list[dict[str, Any]]]:
    next_token: str | None = "init_token"  # Dummy token
    deserializer = TypeDeserializer()

    while next_token:
        response = _execute_statement(kwargs=kwargs, dynamodb_client=dynamodb_client)
        yield [_deserialize_item(item, deserializer) for item in response["Items"]]

        next_token = response.get("NextToken", None)
        if next_token:
            kwargs["NextToken"] = next_token


def execute_statement(
    statement: str,
    parameters: list[Any] | None = None,
    consistent_read: bool = False,
    boto3_session: boto3.Session | None = None,
) -> Iterator[list[dict[str, Any]]] | None:
    """Run a PartiQL statement against a DynamoDB table.

    Parameters
    ----------
    statement : str
        The PartiQL statement.
    parameters : Optional[List[Any]]
        The list of PartiQL parameters. These are applied to the statement in the order they are listed.
    consistent_read: bool
        The consistency of a read operation. If `True`, then a strongly consistent read is used. False by default.
    boto3_session : Optional[boto3.Session]
        Boto3 Session. If None, the default boto3 Session is used.

    Returns
    -------
    Optional[Iterator[Dict[str, Any]]]
        An iterator of the items from the statement response, if any.

    Examples
    --------
    Insert an item

    >>> import awswrangler as wr
    >>> wr.dynamodb.execute_statement(
    ...     statement="INSERT INTO movies VALUE {'title': ?, 'year': ?, 'info': ?}",
    ...     parameters=[title, year, {"plot": plot, "rating": rating}],
    ... )

    Select items

    >>> wr.dynamodb.execute_statement(
    ...     statement="SELECT * FROM movies WHERE title=? AND year=?",
    ...     parameters=[title, year],
    ... )

    Update items

    >>> wr.dynamodb.execute_statement(
    ...     statement="UPDATE movies SET info.rating=? WHERE title=? AND year=?",
    ...     parameters=[rating, title, year],
    ... )

    Delete items

    >>> wr.dynamodb.execute_statement(
    ...     statement="DELETE FROM movies WHERE title=? AND year=?",
    ...     parameters=[title, year],
    ... )
    """
    kwargs: _ReadExecuteStatementKwargs = {"Statement": statement, "ConsistentRead": consistent_read}
    if parameters:
        serializer = TypeSerializer()
        kwargs["Parameters"] = [serializer.serialize(p) for p in parameters]

    dynamodb_client = _utils.client(service_name="dynamodb", session=boto3_session)

    if not statement.strip().upper().startswith("SELECT"):
        _execute_statement(kwargs=kwargs, dynamodb_client=dynamodb_client)
        return None
    return _read_execute_statement(kwargs=kwargs, dynamodb_client=dynamodb_client)


def _validate_items(
    items: list[dict[str, Any]] | list[Mapping[str, Any]], key_schema: list["KeySchemaElementTypeDef"]
) -> None:
    """
    Validate if all items have the required keys for the Amazon DynamoDB table.

    Parameters
    ----------
    items: Union[List[Dict[str, Any]], List[Mapping[str, Any]]]
        List which contains the items that will be validated.
    key_schema: List[KeySchemaElementTableTypeDef]
        The primary key structure for the table.
        Each element consists of the attribute name and it's type (HASH or RANGE).
    """
    table_keys = [schema["AttributeName"] for schema in key_schema]
    if not all(key in item for item in items for key in table_keys):
        raise exceptions.InvalidArgumentValue("All items need to contain the required keys for the table.")


# Based on https://github.com/boto/boto3/blob/fcc24f39cc0a923fa578587fcd1f781e820488a1/boto3/dynamodb/table.py#L63
class _TableBatchWriter:
    """Automatically handle batch writes to DynamoDB for a single table."""

    def __init__(
        self,
        table_name: str,
        client: "DynamoDBClient",
        flush_amount: int = 25,
        overwrite_by_pkeys: list[str] | None = None,
    ):
        self._table_name = table_name
        self._client = client
        self._items_buffer: list["WriteRequestTypeDef"] = []
        self._flush_amount = flush_amount
        self._overwrite_by_pkeys = overwrite_by_pkeys

    def put_item(self, item: dict[str, "AttributeValueTypeDef"]) -> None:
        """
        Add a new put item request to the batch.

        Parameters
        ----------
        item: Dict[str, AttributeValueTypeDef]
            The item to add.
        """
        self._add_request_and_process({"PutRequest": {"Item": item}})

    def delete_item(self, key: dict[str, "AttributeValueTypeDef"]) -> None:
        """
        Add a new delete request to the batch.

        Parameters
        ----------
        key: Dict[str, AttributeValueTypeDef]
            The key of the item to delete.
        """
        self._add_request_and_process({"DeleteRequest": {"Key": key}})

    def _add_request_and_process(self, request: "WriteRequestTypeDef") -> None:
        if self._overwrite_by_pkeys:
            self._remove_dup_pkeys_request_if_any(request, self._overwrite_by_pkeys)

        self._items_buffer.append(request)
        self._flush_if_needed()

    def _remove_dup_pkeys_request_if_any(self, request: "WriteRequestTypeDef", overwrite_by_pkeys: list[str]) -> None:
        pkey_values_new = self._extract_pkey_values(request, overwrite_by_pkeys)
        for item in self._items_buffer:
            if self._extract_pkey_values(item, overwrite_by_pkeys) == pkey_values_new:
                self._items_buffer.remove(item)
                _logger.debug(
                    "With overwrite_by_pkeys enabled, skipping " "request:%s",
                    item,
                )

    def _extract_pkey_values(self, request: "WriteRequestTypeDef", overwrite_by_pkeys: list[str]) -> list[Any] | None:
        if request.get("PutRequest"):
            return [request["PutRequest"]["Item"][key] for key in overwrite_by_pkeys]
        if request.get("DeleteRequest"):
            return [request["DeleteRequest"]["Key"][key] for key in overwrite_by_pkeys]
        return None

    def _flush_if_needed(self) -> None:
        if len(self._items_buffer) >= self._flush_amount:
            self._flush()

    def _flush(self) -> None:
        items_to_send = self._items_buffer[: self._flush_amount]
        self._items_buffer = self._items_buffer[self._flush_amount :]
        response = self._client.batch_write_item(RequestItems={self._table_name: items_to_send})

        unprocessed_items = response["UnprocessedItems"]
        if not unprocessed_items:
            unprocessed_items = {}
        item_list = unprocessed_items.get(self._table_name, [])

        # Any unprocessed_items are immediately added to the
        # next batch we send.
        self._items_buffer.extend(item_list)
        _logger.debug(
            "Batch write sent %s, unprocessed: %s",
            len(items_to_send),
            len(self._items_buffer),
        )

    def __enter__(self) -> "_TableBatchWriter":
        return self

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        # When we exit, we need to keep flushing whatever's left
        # until there's nothing left in our items buffer.
        while self._items_buffer:
            self._flush()

        return None
