"""Amazon DynamoDB Write Module (PRIVATE)."""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Union

import boto3
import pandas as pd

from awswrangler import _utils, exceptions

_logger: logging.Logger = logging.getLogger(__name__)


def put_json(
    path: Union[str, Path],
    table: str,
    boto3_session: Optional[boto3.Session] = None,
) -> None:
    """Write all items from JSON file to a DynamoDB.

    The JSON file can either contain a single item which will be inserted in the DynamoDB or an array of items
    which all be inserted.

    Parameters
    ----------
    path : Union[str, Path]
        Path as str or Path object to the JSON file which contains the items.
    table : str
        Name of the Amazon DynamoDB table
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.
    """
    # Loading data from file
    with open(path, "r") as f:
        items = json.load(f)
    if isinstance(items, dict):
        items = [items]

    # Initializing connection to database
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    dynamodb_resource = session.resource("dynamodb")
    dynamodb_table = dynamodb_resource.Table(table)

    _validate_items(items=items, dynamodb_table=dynamodb_table)

    _put_items(items=items, dynamodb_table=dynamodb_table)


def put_csv(
    path: Union[str, Path],
    table: str,
    boto3_session: Optional[boto3.Session] = None,
) -> None:
    """Write all items from a CSV file to a DynamoDB.

    Parameters
    ----------
    path : Union[str, Path]
        Path as str or Path object to the CSV file which contains the items.
    table : str
        Name of the Amazon DynamoDB table
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.

    Returns
    -------
    None
        None.
    """
    # Loading data from file
    df = pd.read_csv(path)

    put_df(df=df, table=table, boto3_session=boto3_session)


def put_df(
    df: pd.DataFrame,
    table: str,
    boto3_session: Optional[boto3.Session] = None,
) -> None:
    """Write all items from a DataFrame to a DynamoDB.

    Parameters
    ----------
    df : pd.DataFrame
        Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    table : str
        Name of the Amazon DynamoDB table
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.

    Returns
    -------
    None
        None.
    """
    items: List[Dict[str, Union[str, int, float, bool]]] = [v.dropna().to_dict() for k, v in df.iterrows()]

    # Initializing connection to database
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    dynamodb_resource = session.resource("dynamodb")
    dynamodb_table = dynamodb_resource.Table(table)

    _validate_items(items=items, dynamodb_table=dynamodb_table)

    _put_items(items=items, dynamodb_table=dynamodb_table)


def _put_items(  # type: ignore[no-untyped-def]
    items: List[Dict[str, Union[str, int, float, bool]]], dynamodb_table
) -> None:
    """Insert all items to the specified DynamoDB table.

    Parameters
    ----------
    items : List[Dict[str, Union[str, int, float, bool]]]
        List which contains the items that will be inserted.
    dynamodb_table : boto3.resources.dynamodb.Table
        Amazon DynamoDB Table object.

    Returns
    -------
    None
        None.
    """
    with dynamodb_table.batch_writer() as writer:
        for item in items:
            writer.put_item(Item=item)


def _validate_items(  # type: ignore[no-untyped-def]
    items: List[Dict[str, Union[str, int, float, bool]]], dynamodb_table
) -> None:
    """Validate if all items have the required keys for the Amazon DynamoDB table.

    Parameters
    ----------
    items : List[Dict[str, Union[str, int, float, bool]]]
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
        raise exceptions.InvalidFile("All items need to contain the required keys for the table.")
