"""Amazon DynamoDB Utils Module (PRIVATE)."""

import logging
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Mapping, Optional, Union

import boto3
from boto3.dynamodb.conditions import ConditionExpressionBuilder
from botocore.exceptions import ClientError

from awswrangler import _utils, exceptions
from awswrangler._config import apply_configs

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.service_resource import Table
    from mypy_boto3_dynamodb.type_defs import ExecuteStatementOutputTypeDef

_logger: logging.Logger = logging.getLogger(__name__)


@apply_configs
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
    boto3_session: Optional[boto3.Session],
) -> "ExecuteStatementOutputTypeDef":
    dynamodb_resource = _utils.resource(service_name="dynamodb", session=boto3_session)
    try:
        response = dynamodb_resource.meta.client.execute_statement(**kwargs)  # type: ignore[arg-type]
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
    kwargs: Dict[str, Union[str, bool, List[Any]]], boto3_session: Optional[boto3.Session]
) -> Iterator[Dict[str, Any]]:
    next_token: Optional[str] = "init_token"  # Dummy token
    while next_token:
        response = _execute_statement(kwargs=kwargs, boto3_session=boto3_session)
        next_token = response.get("NextToken", None)
        kwargs["NextToken"] = next_token  # type: ignore[assignment]
        yield response["Items"]  # type: ignore[misc]


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
        kwargs["Parameters"] = parameters

    if not statement.strip().upper().startswith("SELECT"):
        _execute_statement(kwargs=kwargs, boto3_session=boto3_session)
        return None
    return _read_execute_statement(kwargs=kwargs, boto3_session=boto3_session)


def _serialize_kwargs(kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """Serialize a DynamoDB input arguments dictionary.

    Parameters
    ----------
    kwargs : Dict[str, Any]
        Dictionary to serialize.

    Returns
    -------
    Dict[str, Any]
        Serialized dictionary.
    """
    names: Dict[str, Any] = {}
    values: Dict[str, Any] = {}
    serializer = boto3.dynamodb.types.TypeSerializer()

    if "FilterExpression" in kwargs and not isinstance(kwargs["FilterExpression"], str):
        builder = ConditionExpressionBuilder()
        exp_string, names, values = builder.build_expression(kwargs["FilterExpression"], False)  # type: ignore[assignment]
        kwargs["FilterExpression"] = exp_string

    if "ExpressionAttributeNames" in kwargs:
        kwargs["ExpressionAttributeNames"].update(names)
    else:
        if names:
            kwargs["ExpressionAttributeNames"] = names

    values = {k: serializer.serialize(v) for k, v in values.items()}
    if "ExpressionAttributeValues" in kwargs:
        kwargs["ExpressionAttributeValues"] = {
            k: serializer.serialize(v) for k, v in kwargs["ExpressionAttributeValues"].items()
        }
        kwargs["ExpressionAttributeValues"].update(values)
    else:
        if values:
            kwargs["ExpressionAttributeValues"] = values

    return kwargs


def _validate_items(items: Union[List[Dict[str, Any]], List[Mapping[str, Any]]], dynamodb_table: "Table") -> None:
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
