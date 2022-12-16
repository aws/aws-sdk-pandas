"""Amazon DynamoDB Read Module (PRIVATE)."""

import logging
from typing import Any, Dict, Iterator, List, Optional, Union

import boto3
import pandas as pd

from awswrangler.dynamodb._utils import execute_statement

_logger: logging.Logger = logging.getLogger(__name__)


def _read_chunked(iterator: Iterator[Dict[str, Any]], dtype: Optional[Dict[str, str]]) -> Iterator[pd.DataFrame]:
    for item in iterator:
        yield pd.DataFrame(item).astype(dtype=dtype) if dtype else pd.DataFrame(item)


def read_partiql_query(
    query: str,
    parameters: Optional[List[Any]] = None,
    chunked: bool = False,
    dtype: Optional[Dict[str, str]] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Read data from a DynamoDB table via a PartiQL query.

    Parameters
    ----------
    query : str
        The PartiQL statement.
    parameters : Optional[List[Any]]
        The list of PartiQL parameters. These are applied to the statement in the order they are listed.
    chunked : bool
        If `True` an iterable of DataFrames is returned. False by default.
    dtype : Optional[Dict[str, str]]
        The data types of the DataFrame columns.
    boto3_session : Optional[boto3.Session]
        Boto3 Session. If None, the default boto3 Session is used.

    Returns
    -------
    Union[pd.DataFrame, Iterator[pd.DataFrame]]
        Result as Pandas DataFrame.

    Examples
    --------
    Select all contents from a table

    >>> import awswrangler as wr
    >>> wr.dynamodb.read_partiql_query(
    ...     query="SELECT * FROM my_table WHERE title=? AND year=?",
    ...     parameters=[title, year],
    ... )

    Select specific columns from a table

    >>> wr.dynamodb.read_partiql_query(
    ...     query="SELECT id FROM table"
    ... )

    Select all contents with dtype set and chunked

    >>> wr.dynamodb.read_partiql_query(
    ...     query="SELECT * FROM table",
    ...     chunked=True,
    ...     dtype={'key': int},
    ... )
    """
    _logger.debug("Reading results for PartiQL query:  '%s'", query)
    iterator: Iterator[Dict[str, Any]] = execute_statement(  # type: ignore
        query, parameters=parameters, boto3_session=boto3_session
    )
    if chunked:
        return _read_chunked(iterator=iterator, dtype=dtype)
    df = pd.DataFrame([item for sublist in iterator for item in sublist])
    return df.astype(dtype=dtype) if dtype else df
