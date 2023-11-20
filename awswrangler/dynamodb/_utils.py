"""Amazon DynamoDB Utils Module (PRIVATE)."""

import logging
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Mapping, Optional, Union

import boto3
from boto3.dynamodb.conditions import ConditionExpressionBuilder
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.exceptions import ClientError

from awswrangler import _utils, exceptions
from awswrangler._config import apply_configs
from awswrangler.annotations import Deprecated

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.client import DynamoDBClient
    from mypy_boto3_dynamodb.service_resource import Table
    from mypy_boto3_dynamodb.type_defs import (
        AttributeValueTypeDef,
        ExecuteStatementOutputTypeDef,
        KeySchemaElementTableTypeDef,
    )


_logger: logging.Logger = logging.getLogger(__name__)


@apply_configs
@Deprecated
def get_table(
    table_name: str,
    boto3_session: Optional[boto3.Session] = None,
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


def _execute_statement(
    kwargs: Dict[str, Union[str, bool, List[Any]]],
    dynamodb_client: "DynamoDBClient",
) -> "ExecuteStatementOutputTypeDef":
    try:
        response = dynamodb_client.execute_statement(**kwargs)  # type: ignore[arg-type]
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


def _serialize_item(
    item: Dict[str, Any], serializer: Optional[TypeSerializer] = None
) -> Dict[str, "AttributeValueTypeDef"]:
    serializer = serializer if serializer else TypeSerializer()
    return {k: serializer.serialize(v) for k, v in item.items()}


def _deserialize_item(
    item: Dict[str, "AttributeValueTypeDef"], deserializer: Optional[TypeDeserializer] = None
) -> Dict[str, Any]:
    deserializer = deserializer if deserializer else TypeDeserializer()
    return {k: deserializer.deserialize(v) for k, v in item.items()}


def _read_execute_statement(
    kwargs: Dict[str, Union[str, bool, List[Any]]],
    dynamodb_client: "DynamoDBClient",
) -> Iterator[Dict[str, Any]]:
    next_token: Optional[str] = "init_token"  # Dummy token
    deserializer = TypeDeserializer()

    while next_token:
        response = _execute_statement(kwargs=kwargs, dynamodb_client=dynamodb_client)
        next_token = response.get("NextToken", None)
        kwargs["NextToken"] = next_token  # type: ignore[assignment]
        yield [_deserialize_item(item, deserializer) for item in response["Items"]]


def execute_statement(
    statement: str,
    parameters: Optional[List[Any]] = None,
    consistent_read: bool = False,
    boto3_session: Optional[boto3.Session] = None,
) -> Optional[Iterator[Dict[str, Any]]]:
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
    kwargs: Dict[str, Union[str, bool, List[Any]]] = {"Statement": statement, "ConsistentRead": consistent_read}
    if parameters:
        serializer = TypeSerializer()
        kwargs["Parameters"] = [serializer.serialize(p) for p in parameters]

    dynamodb_client = _utils.client(service_name="dynamodb", session=boto3_session)

    if not statement.strip().upper().startswith("SELECT"):
        _execute_statement(kwargs=kwargs, dynamodb_client=dynamodb_client)
        return None
    return _read_execute_statement(kwargs=kwargs, dynamodb_client=dynamodb_client)


def _validate_items(
    items: Union[List[Dict[str, Any]], List[Mapping[str, Any]]], key_schema: List["KeySchemaElementTableTypeDef"]
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
    table_keys = [schema["AttributeName"] for schema in key_schema]
    if not all(key in item for item in items for key in table_keys):
        raise exceptions.InvalidArgumentValue("All items need to contain the required keys for the table.")
