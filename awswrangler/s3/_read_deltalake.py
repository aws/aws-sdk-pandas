"""Amazon S3 Read Delta Lake Module (PRIVATE)."""

import logging
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
import pyarrow.fs as pa_fs
from deltalake import DataCatalog, DeltaTable

_logger: logging.Logger = logging.getLogger(__name__)


def read_deltalake(
    path: str,
    version: Optional[int] = None,
    storage_options: Optional[Dict[str, str]] = None,
    without_files: bool = False,
    partitions: Optional[List[Tuple[str, str, Any]]] = None,
    columns: Optional[List[str]] = None,
    filesystem: Optional[Union[str, pa_fs.FileSystem]] = None,
) -> pd.DataFrame:
    """Load data from Deltalake.

    This function requires the `deltalake package
    <https://delta-io.github.io/delta-rs/python>`__.
    See the `How to load a Delta table
    <https://delta-io.github.io/delta-rs/python/usage.html#loading-a-delta-table>`__
    guide for loading instructions.

    Parameters
    ----------
    path: str
        The path of the DeltaTable.
    version: Optional[int]
        The version of the DeltaTable.
    storage_options: Optional[Dict[str, str]]
        A dictionary of the options to use for the storage backend.
    without_files: bool
        If True, will load table without tracking files.
        Some append-only applications might have no need of tracking any files.
        So, the DeltaTable will be loaded with a significant memory reduction.
    partitions: Optional[List[Tuple[str, str, Any]]
        A list of partition filters, see help(DeltaTable.files_by_partitions)
        for filter syntax.
    columns: Optional[List[str]]
        The columns to project. This can be a list of column names to include
        (order and duplicates will be preserved).
    filesystem: Optional[Union[str, pa_fs.FileSystem]]
        A concrete implementation of the Pyarrow FileSystem or
        a fsspec-compatible interface. If None, the first file path will be used
        to determine the right FileSystem.

    Returns
    -------
    df: DataFrame
        DataFrame including the results.

    See Also
    --------
    deltalake.DeltaTable : Create a DeltaTable instance with the deltalake library.
    """
    table = DeltaTable(
        table_uri=path,
        version=version,
        storage_options=storage_options,
        without_files=without_files,
    )
    return table.to_pandas(partitions=partitions, columns=columns, filesystem=filesystem)


def read_deltalake_from_glue(
    database: str,
    table: str,
    catalog_id: Optional[str] = None,
    version: Optional[int] = None,
    partitions: Optional[List[Tuple[str, str, Any]]] = None,
    columns: Optional[List[str]] = None,
    filesystem: Optional[Union[str, pa_fs.FileSystem]] = None,
) -> pd.DataFrame:
    """Load data from Deltalake from AWS Glue.

    This function requires the `deltalake package
    <https://delta-io.github.io/delta-rs/python>`__.
    See the `How to load a Delta table
    <https://delta-io.github.io/delta-rs/python/usage.html#loading-a-delta-table>`__
    guide for loading instructions.

    Parameters
    ----------
    database : str
        Glue/Athena catalog: Database name.
    table : str
        Glue/Athena catalog: Table name.
    catalog_id: Optional[str]
        The optional Glue catalog id.
    version: Optional[int]
        The version of the DeltaTable.
    partitions: Optional[List[Tuple[str, str, Any]]
        A list of partition filters, see help(DeltaTable.files_by_partitions)
        for filter syntax.
    columns: Optional[List[str]]
        The columns to project. This can be a list of column names to include
        (order and duplicates will be preserved).
    filesystem: Optional[Union[str, pa_fs.FileSystem]]
        A concrete implementation of the Pyarrow FileSystem or
        a fsspec-compatible interface. If None, the first file path will be used
        to determine the right FileSystem.

    Returns
    -------
    df: DataFrame
        DataFrame including the results.

    See Also
    --------
    deltalake.DeltaTable : Create a DeltaTable instance with the deltalake library.
    """
    return DeltaTable.from_data_catalog(
        data_catalog=DataCatalog.AWS.value,
        data_catalog_id=catalog_id,
        database_name=database,
        table_name=table,
        version=version,
    ).to_pandas(partitions=partitions, columns=columns, filesystem=filesystem)
