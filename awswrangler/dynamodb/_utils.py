"""Amazon DynamoDB Utils Module (PRIVATE)."""

import logging
from typing import Any, Dict, List, Mapping, Optional, Union

import boto3
from botocore.exceptions import ClientError

from awswrangler import _utils, exceptions
from awswrangler._config import apply_configs

_logger: logging.Logger = logging.getLogger(__name__)


@apply_configs
def get_table(
    table_name: str,
    boto3_session: Optional[boto3.Session] = None,
) -> boto3.resource:
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


def execute_statement(
    statement: str,
    parameters: Optional[List[Any]] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> List[Optional[Dict[str, Any]]]:
    """Run a PartiQL statement against a DynamoDB table.

    Parameters
    ----------
    statement : str
        The PartiQL statement.
    parameters : Optional[List[Any]]
        The list of PartiQL parameters. These are applied to the statement in the order they are listed.
    boto3_session : Optional[boto3.Session]
        Boto3 Session. If None, the default boto3 Session is used.

    Returns
    -------
    List[Optional[Dict[str, Any]]]
        The items from the statement response, if any.

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
    dynamodb_resource = _utils.resource(service_name="dynamodb", session=boto3_session)
    kwargs: Dict[str, Union[str, Optional[List[Any]]]] = {"Statement": statement}
    if parameters:
        kwargs["Parameters"] = parameters
    next_token: str = "init_token"  # Dummy token
    items: List[Optional[Dict[str, Any]]] = []

    while next_token:
        try:
            response: Dict[str, Any] = dynamodb_resource.meta.client.execute_statement(**kwargs)
        except ClientError as err:
            if err.response["Error"]["Code"] == "ResourceNotFoundException":
                _logger.error("Couldn't execute PartiQL '%s' because the table does not exist.", statement)
            else:
                _logger.error(
                    "Couldn't execute PartiQL '%s'. %s: %s",
                    statement,
                    err.response["Error"]["Code"],
                    err.response["Error"]["Message"],
                )
            raise
        else:
            if "Items" in response:
                items.extend(response["Items"])
            next_token = response.get("NextToken", None)
            kwargs["NextToken"] = next_token
    return items


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
