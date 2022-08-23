"""Utilities Module for AWS Glue Catalog."""
import logging
import re
import unicodedata
import warnings
from typing import Any, Dict, List, Optional, Tuple

import boto3
import pandas as pd

from awswrangler import _data_types, _utils, exceptions
from awswrangler._config import apply_configs

_logger: logging.Logger = logging.getLogger(__name__)


def _catalog_id(catalog_id: Optional[str] = None, **kwargs: Any) -> Dict[str, Any]:
    if catalog_id is not None:
        kwargs["CatalogId"] = catalog_id
    return kwargs


def _transaction_id(
    transaction_id: Optional[str] = None, query_as_of_time: Optional[str] = None, **kwargs: Any
) -> Dict[str, Any]:
    if transaction_id is not None and query_as_of_time is not None:
        raise exceptions.InvalidArgumentCombination(
            "Please pass only one of `transaction_id` or `query_as_of_time`, not both"
        )
    if transaction_id is not None:
        kwargs["TransactionId"] = transaction_id
    elif query_as_of_time is not None:
        kwargs["QueryAsOfTime"] = query_as_of_time
    return kwargs


def _sanitize_name(name: str) -> str:
    name = "".join(c for c in unicodedata.normalize("NFD", name) if unicodedata.category(c) != "Mn")  # strip accents
    return re.sub("[^A-Za-z0-9_]+", "_", name).lower()  # Replacing non alphanumeric characters by underscore


def _extract_dtypes_from_table_details(response: Dict[str, Any]) -> Dict[str, str]:
    dtypes: Dict[str, str] = {}
    for col in response["Table"]["StorageDescriptor"]["Columns"]:
        dtypes[col["Name"]] = col["Type"]
    if "PartitionKeys" in response["Table"]:
        for par in response["Table"]["PartitionKeys"]:
            dtypes[par["Name"]] = par["Type"]
    return dtypes


@apply_configs
def does_table_exist(
    database: str,
    table: str,
    boto3_session: Optional[boto3.Session] = None,
    catalog_id: Optional[str] = None,
    transaction_id: Optional[str] = None,
) -> bool:
    """Check if the table exists.

    Parameters
    ----------
    database : str
        Database name.
    table : str
        Table name.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    transaction_id: str, optional
        The ID of the transaction (i.e. used with GOVERNED tables).

    Returns
    -------
    bool
        True if exists, otherwise False.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.catalog.does_table_exist(database='default', table='my_table')
    """
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
    try:
        client_glue.get_table(
            **_catalog_id(
                catalog_id=catalog_id,
                **_transaction_id(transaction_id=transaction_id, DatabaseName=database, Name=table),
            )
        )
        return True
    except client_glue.exceptions.EntityNotFoundException:
        return False


def sanitize_column_name(column: str) -> str:
    """Convert the column name to be compatible with Amazon Athena and the AWS Glue Catalog.

    https://docs.aws.amazon.com/athena/latest/ug/tables-databases-columns-names.html

    Possible transformations:
    - Strip accents
    - Remove non alphanumeric characters

    Parameters
    ----------
    column : str
        Column name.

    Returns
    -------
    str
        Normalized column name.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.catalog.sanitize_column_name('MyNewColumn')
    'mynewcolumn'

    """
    return _sanitize_name(name=column)


def rename_duplicated_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Append an incremental number to duplicate column names to conform with Amazon Athena.

    Note
    ----
    This transformation will run `inplace` and will make changes to the original DataFrame.

    Note
    ----
    Also handles potential new column duplicate conflicts by appending an additional `_n`.

    Parameters
    ----------
    df : pandas.DataFrame
        Original Pandas DataFrame.

    Returns
    -------
    pandas.DataFrame
        DataFrame with duplicated column names renamed.

    Examples
    --------
    >>> df = pd.DataFrame({'a': [1, 2], 'b': [3, 4], 'c': [4, 6]})
    >>> df.columns = ['a', 'a', 'a_1']
    >>> wr.catalog.rename_duplicated_columns(df=df)
    a	a_1	a_1_1
    1	3	4
    2	4	6
    """
    names = df.columns
    set_names = set(names)
    if len(names) == len(set_names):
        return df
    d = {key: [name + f"_{i}" if i > 0 else name for i, name in enumerate(names[names == key])] for key in set_names}
    df.rename(columns=lambda c: d[c].pop(0), inplace=True)
    while df.columns.duplicated().any():
        # Catches edge cases where pd.DataFrame({"A": [1, 2], "a": [3, 4], "a_1": [5, 6]})
        df = rename_duplicated_columns(df)
    return df


def sanitize_dataframe_columns_names(
    df: pd.DataFrame, handle_duplicate_columns: Optional[str] = "warn"
) -> pd.DataFrame:
    """Normalize all columns names to be compatible with Amazon Athena.

    https://docs.aws.amazon.com/athena/latest/ug/tables-databases-columns-names.html

    Possible transformations:
    - Strip accents
    - Remove non alphanumeric characters

    Note
    ----
    After transformation, some column names might not be unique anymore.
    Example: the columns ["A", "a"] will be sanitized to ["a", "a"]

    Parameters
    ----------
    df : pandas.DataFrame
        Original Pandas DataFrame.
    handle_duplicate_columns : str, optional
        How to handle duplicate columns. Can be "warn" or "drop" or "rename".
        "drop" will drop all but the first duplicated column.
        "rename" will rename all duplicated columns with an incremental number.
        Defaults to "warn".

    Returns
    -------
    pandas.DataFrame
        Original Pandas DataFrame with columns names normalized.

    Examples
    --------
    >>> import awswrangler as wr
    >>> df_normalized = wr.catalog.sanitize_dataframe_columns_names(df=pd.DataFrame({"A": [1, 2]}))
    >>> df_normalized_drop = wr.catalog.sanitize_dataframe_columns_names(
            df=pd.DataFrame({"A": [1, 2], "a": [3, 4]}), handle_duplicate_columns="drop"
        )
    >>> df_normalized_rename = wr.catalog.sanitize_dataframe_columns_names(
            df=pd.DataFrame({"A": [1, 2], "a": [3, 4], "a_1": [4, 6]}), handle_duplicate_columns="rename"
        )

    """
    df.columns = [sanitize_column_name(x) for x in df.columns]
    df.index.names = [None if x is None else sanitize_column_name(x) for x in df.index.names]
    if df.columns.duplicated().any():  # type: ignore
        if handle_duplicate_columns == "warn":
            warnings.warn(
                "Duplicate columns were detected, consider using `handle_duplicate_columns='[drop|rename]'`",
                UserWarning,
            )
        elif handle_duplicate_columns == "drop":
            df = drop_duplicated_columns(df)
        elif handle_duplicate_columns == "rename":
            df = rename_duplicated_columns(df)
        else:
            raise ValueError("handle_duplicate_columns must be one of ['warn', 'drop', 'rename']")
    return df


def sanitize_table_name(table: str) -> str:
    """Convert the table name to be compatible with Amazon Athena and the AWS Glue Catalog.

    https://docs.aws.amazon.com/athena/latest/ug/tables-databases-columns-names.html

    Possible transformations:
    - Strip accents
    - Remove non alphanumeric characters

    Parameters
    ----------
    table : str
        Table name.

    Returns
    -------
    str
        Normalized table name.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.catalog.sanitize_table_name('MyNewTable')
    'mynewtable'

    """
    return _sanitize_name(name=table)


def drop_duplicated_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Drop all repeated columns (duplicated names).

    Note
    ----
    This transformation will run `inplace` and will make changes in the original DataFrame.

    Note
    ----
    It is different from Panda's drop_duplicates() function which considers the column values.
    wr.catalog.drop_duplicated_columns() will deduplicate by column name.

    Parameters
    ----------
    df : pandas.DataFrame
        Original Pandas DataFrame.

    Returns
    -------
    pandas.DataFrame
        Pandas DataFrame without duplicated columns.

    Examples
    --------
    >>> import awswrangler as wr
    >>> df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
    >>> df.columns = ["A", "A"]
    >>> wr.catalog.drop_duplicated_columns(df=df)
       A
    0  1
    1  2

    """
    duplicated = df.columns.duplicated()
    if duplicated.any():
        _logger.warning("Dropping duplicated columns...")
        columns = df.columns.values
        columns[duplicated] = "AWSWranglerDuplicatedMarker"
        df.columns = columns
        df.drop(columns="AWSWranglerDuplicatedMarker", inplace=True)
    return df


def extract_athena_types(
    df: pd.DataFrame,
    index: bool = False,
    partition_cols: Optional[List[str]] = None,
    dtype: Optional[Dict[str, str]] = None,
    file_format: str = "parquet",
) -> Tuple[Dict[str, str], Dict[str, str]]:
    """Extract columns and partitions types (Amazon Athena) from Pandas DataFrame.

    https://docs.aws.amazon.com/athena/latest/ug/data-types.html

    Parameters
    ----------
    df : pandas.DataFrame
        Pandas DataFrame.
    index : bool
        Should consider the DataFrame index as a column?.
    partition_cols : List[str], optional
        List of partitions names.
    dtype: Dict[str, str], optional
        Dictionary of columns names and Athena/Glue types to be casted.
        Useful when you have columns with undetermined or mixed data types.
        (e.g. {'col name': 'bigint', 'col2 name': 'int'})
    file_format : str, optional
        File format to be considered to place the index column: "parquet" | "csv".

    Returns
    -------
    Tuple[Dict[str, str], Dict[str, str]]
        columns_types: Dictionary with keys as column names and values as
        data types (e.g. {'col0': 'bigint', 'col1': 'double'}). /
        partitions_types: Dictionary with keys as partition names
        and values as data types (e.g. {'col2': 'date'}).

    Examples
    --------
    >>> import awswrangler as wr
    >>> columns_types, partitions_types = wr.catalog.extract_athena_types(
    ...     df=df, index=False, partition_cols=["par0", "par1"], file_format="csv"
    ... )

    """
    if file_format == "parquet":
        index_left: bool = False
    elif file_format == "csv":
        index_left = True
    else:
        raise exceptions.InvalidArgumentValue("file_format argument must be parquet or csv")
    return _data_types.athena_types_from_pandas_partitioned(
        df=df, index=index, partition_cols=partition_cols, dtype=dtype, index_left=index_left
    )
