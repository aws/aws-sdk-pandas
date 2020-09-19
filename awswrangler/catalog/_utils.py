"""Utilities Module for AWS Glue Catalog."""
import logging
import re
import unicodedata
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


def _sanitize_name(name: str) -> str:
    name = "".join(c for c in unicodedata.normalize("NFD", name) if unicodedata.category(c) != "Mn")  # strip accents
    name = re.sub("[^A-Za-z0-9_]+", "_", name)  # Replacing non alphanumeric characters by underscore
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()  # Converting CamelCase to snake_case


def _extract_dtypes_from_table_details(response: Dict[str, Any]) -> Dict[str, str]:
    dtypes: Dict[str, str] = {}
    for col in response["Table"]["StorageDescriptor"]["Columns"]:
        dtypes[col["Name"]] = col["Type"]
    if "PartitionKeys" in response["Table"]:
        for par in response["Table"]["PartitionKeys"]:
            dtypes[par["Name"]] = par["Type"]
    return dtypes


@apply_configs
def does_table_exist(database: str, table: str, boto3_session: Optional[boto3.Session] = None) -> bool:
    """Check if the table exists.

    Parameters
    ----------
    database : str
        Database name.
    table : str
        Table name.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

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
        client_glue.get_table(DatabaseName=database, Name=table)
        return True
    except client_glue.exceptions.EntityNotFoundException:
        return False


def sanitize_column_name(column: str) -> str:
    """Convert the column name to be compatible with Amazon Athena.

    https://docs.aws.amazon.com/athena/latest/ug/tables-databases-columns-names.html

    Possible transformations:
    - Strip accents
    - Remove non alphanumeric characters
    - Convert CamelCase to snake_case

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
    'my_new_column'

    """
    return _sanitize_name(name=column)


def sanitize_dataframe_columns_names(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize all columns names to be compatible with Amazon Athena.

    https://docs.aws.amazon.com/athena/latest/ug/tables-databases-columns-names.html

    Possible transformations:
    - Strip accents
    - Remove non alphanumeric characters
    - Convert CamelCase to snake_case

    Note
    ----
    After transformation, some column names might not be unique anymore.
    Example: the columns ["A", "a"] will be sanitized to ["a", "a"]

    Parameters
    ----------
    df : pandas.DataFrame
        Original Pandas DataFrame.

    Returns
    -------
    pandas.DataFrame
        Original Pandas DataFrame with columns names normalized.

    Examples
    --------
    >>> import awswrangler as wr
    >>> df_normalized = wr.catalog.sanitize_dataframe_columns_names(df=pd.DataFrame({'A': [1, 2]}))

    """
    df.columns = [sanitize_column_name(x) for x in df.columns]
    df.index.names = [None if x is None else sanitize_column_name(x) for x in df.index.names]
    return df


def sanitize_table_name(table: str) -> str:
    """Convert the table name to be compatible with Amazon Athena.

    https://docs.aws.amazon.com/athena/latest/ug/tables-databases-columns-names.html

    Possible transformations:
    - Strip accents
    - Remove non alphanumeric characters
    - Convert CamelCase to snake_case

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
    'my_new_table'

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
        columns[duplicated] = "AWSDataWranglerDuplicatedMarker"
        df.columns = columns
        df.drop(columns="AWSDataWranglerDuplicatedMarker", inplace=True)
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
        File format to be consided to place the index column: "parquet" | "csv".

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
