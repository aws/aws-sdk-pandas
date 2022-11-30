"""Amazon DynamoDB Read Module (PRIVATE)."""

import logging
from typing import Any, Dict, List, Optional

import boto3
import pandas as pd

from awswrangler._config import apply_configs
from awswrangler.dynamodb._utils import execute_statement

_logger: logging.Logger = logging.getLogger(__name__)


@apply_configs
def read_partiql_query(
    query: str,
    parameters: Optional[List[Any]] = None,
    dtype: Optional[Dict[str, str]] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> pd.DataFrame:
    """Read data from a DynamoDB table via a PartiQL query.

    Parameters
    ----------
    query : str
        The PartiQL statement.
    parameters : Optional[List[Any]]
        The list of PartiQL parameters. These are applied to the statement in the order they are listed.
    dtype : Optional[Dict[str, str]]
        The data types of the DataFrame columns.
    boto3_session : Optional[boto3.Session]
        Boto3 Session. If None, the default boto3 Session is used.

    Returns
    -------
    pd.DataFrame
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

    Select all contents with dtype set

    >>> wr.dynamodb.read_partiql_query(
    ...     query="SELECT * FROM table",
    ...     dtype={'key': int}
    ... )
    """
    _logger.debug("Reading results for PartiQL query:  %s", query)
    items = execute_statement(query, parameters=parameters, boto3_session=boto3_session)
    df = pd.DataFrame(items)
    return df.astype(dtype=dtype) if dtype else df
