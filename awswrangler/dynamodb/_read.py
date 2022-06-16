"""Amazon DynamoDB Read Module (PRIVATE)."""

import logging
import re
from typing import Any, Dict, List, Optional, Tuple

import boto3
import pandas as pd
from boto3.dynamodb.types import TypeDeserializer

from awswrangler import _utils
from awswrangler._config import apply_configs

_logger: logging.Logger = logging.getLogger(__name__)


def _get_terms_groups(terms: List[str]) -> Tuple[List[str], List[str], List[str]]:
    """Determine to which group of a PartiQL query each term belongs, e.g. it describes a column, table or filter."""
    is_select_term = False
    is_from_term = False
    is_where_term = False
    select_terms, from_terms, where_terms = [], [], []
    for term in terms:
        if term.upper() == "SELECT":
            is_select_term = True
            continue
        if term.upper() == "FROM":
            is_select_term = False
            is_from_term = True
            continue
        if term.upper() == "WHERE":
            is_from_term = False
            is_where_term = True
            continue
        if is_select_term:
            select_terms.append(term)
        if is_from_term:
            from_terms.append(term)
        if is_where_term:
            where_terms.append(term)
    return select_terms, from_terms, where_terms


def _get_scan_response(
    table_name: str, select_terms: List[str], boto3_session: Optional[boto3.Session] = None
) -> List[Dict[str, Any]]:
    """Perform a scan to the Dynamo DB table and returns the data fetched."""
    client_dynamodb = _utils.client(service_name="dynamodb", session=boto3_session)
    scan_config: Dict[str, Any] = {"TableName": table_name}
    if len(select_terms) > 1 or select_terms[0] != "*":
        scan_config["AttributesToGet"] = select_terms
    # get all responses even if pagination is necessary
    response = client_dynamodb.scan(**scan_config)
    data: List[Dict[str, Any]] = response["Items"]
    while "LastEvaluatedKey" in response:
        scan_config["ExclusiveStartKey"] = response["LastEvaluatedKey"]
        response = client_dynamodb.scan(**scan_config)
        data.extend(response["Items"])
    return data


def _get_items(query: str, boto3_session: Optional[boto3.Session] = None) -> List[Dict[str, Any]]:
    # clean input query from possible excessive whitespace
    query = re.sub(" +", " ", query).strip()
    # generate terms list from query
    terms = re.split(" |,", query)
    if terms[0].upper() != "SELECT":
        raise ValueError("The PartiQL query does not start with a 'SELECT'.")
    select_terms, from_terms, _ = _get_terms_groups(terms)
    if len(from_terms) > 1:
        raise ValueError("The PartiQL query contains multiple tables but only one needed.")
    if len(from_terms) == 0:
        raise ValueError("The PartiQL query contains no tables.")
    table_name = from_terms[0]
    return _get_scan_response(table_name=table_name, select_terms=select_terms, boto3_session=boto3_session)


def _deserialize_value(value: Any) -> Any:
    if not pd.isna(value):
        return TypeDeserializer().deserialize(value)
    return value


def _deserialize_data(df: pd.DataFrame, columns: pd.Index) -> pd.DataFrame:
    if df.shape[0] > 0:
        for column in columns:
            df[column] = df[column].apply(_deserialize_value)
    return df


def _parse_dynamodb_items(
    items: List[Dict[str, Any]],
    dtype: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
    df = pd.DataFrame(items)
    df = _deserialize_data(df, df.columns)
    return df.astype(dtype=dtype) if dtype else df


@apply_configs
def read_partiql_query(
    query: str,
    dtype: Optional[Dict[str, str]] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> pd.DataFrame:
    """Read data from a DynamoDB table via a PartiQL query.

    Parameters
    ----------
    query : str
        The PartiQL query that will be executed.
    dtype : Dict, optional
        The data types of the DataFrame columns.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.

    Returns
    -------
    pd.DataFrame
        Result as Pandas DataFrame.

    Examples
    --------
    Select all contents from a table

    >>> import awswrangler as wr
    >>> wr.dynamodb.read_partiql_query(
    ...     query='SELECT * FROM table'
    ... )

    Select specific columns from a table

    >>> import awswrangler as wr
    >>> wr.dynamodb.read_partiql_query(
    ...     query='SELECT key FROM table'
    ... )

    Select all contents with dtype set

    >>> import awswrangler as wr
    >>> wr.dynamodb.read_partiql_query(
    ...     query='SELECT * FROM table',
    ...     dtype={'key': int}
    ... )
    """
    _logger.debug("Reading results for PartiQL query:  %s", query)
    items = _get_items(query=query, boto3_session=boto3_session)
    return _parse_dynamodb_items(items=items, dtype=dtype)
