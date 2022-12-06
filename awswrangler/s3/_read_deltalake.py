"""Amazon S3 Read Delta Lake Module (PRIVATE)."""
import importlib.util
import logging
from typing import Any, Dict, List, Optional, Tuple

import boto3
import pandas as pd

from awswrangler import _utils

_deltalake_found = importlib.util.find_spec("deltalake")
if _deltalake_found:
    from deltalake import DataCatalog, DeltaTable  # pylint: disable=import-error

_logger: logging.Logger = logging.getLogger(__name__)


def read_deltalake(
    path: str,
    version: Optional[int] = None,
    partitions: Optional[List[Tuple[str, str, Any]]] = None,
    columns: Optional[List[str]] = None,
    without_files: bool = False,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
    pyarrow_additional_kwargs: Optional[Dict[str, Any]] = None,
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
    partitions: Optional[List[Tuple[str, str, Any]]
        A list of partition filters, see help(DeltaTable.files_by_partitions)
        for filter syntax.
    columns: Optional[List[str]]
        The columns to project. This can be a list of column names to include
        (order and duplicates will be preserved).
    without_files: bool
        If True, will load table without tracking files.
        Some append-only applications might have no need of tracking any files.
        So, the DeltaTable will be loaded with a significant memory reduction.
    boto3_session: boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    s3_additional_kwargs: Optional[Dict[str, str]]
        Forward to the Delta Table class for the storage options of the S3 backend.
    pyarrow_additional_kwargs: Optional[Dict[str, str]]
        Forward to the PyArrow to_pandas method.

    Returns
    -------
    df: DataFrame
        DataFrame including the results.

    See Also
    --------
    deltalake.DeltaTable : Create a DeltaTable instance with the deltalake library.
    """
    session: boto3.Session = _utils.ensure_session(boto3_session)
    storage_options = _set_default_storage_options_kwargs(s3_additional_kwargs, session)
    pyarrow_args = _set_default_pyarrow_additional_kwargs(pyarrow_additional_kwargs)

    table = DeltaTable(
        table_uri=path,
        version=version,
        storage_options=storage_options,
        without_files=without_files,
    )

    return table.to_pyarrow_table(partitions=partitions, columns=columns).to_pandas(pyarrow_args)


def read_deltalake_from_glue(
    database: str,
    table: str,
    catalog_id: Optional[str] = None,
    version: Optional[int] = None,
    partitions: Optional[List[Tuple[str, str, Any]]] = None,
    columns: Optional[List[str]] = None,
    pyarrow_additional_kwargs: Optional[Dict[str, Any]] = None,
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
    pyarrow_additional_kwargs: Optional[Dict[str, str]]
        Forward to the PyArrow to_pandas method.

    Returns
    -------
    df: DataFrame
        DataFrame including the results.

    See Also
    --------
    deltalake.DeltaTable : Create a DeltaTable instance with the deltalake library.
    """
    pyarrow_args = _set_default_pyarrow_additional_kwargs(pyarrow_additional_kwargs)
    delta_table = DeltaTable.from_data_catalog(
        data_catalog=DataCatalog.AWS.value,
        data_catalog_id=catalog_id,
        database_name=database,
        table_name=table,
        version=version,
    )
    return delta_table.to_pyarrow_table(partitions=partitions, columns=columns).to_pandas(pyarrow_args)


def _set_default_pyarrow_additional_kwargs(pyarrow_additional_kwargs: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if pyarrow_additional_kwargs is None:
        pyarrow_additional_kwargs = {}
    defaults = {
        "coerce_int96_timestamp_unit": None,
        "timestamp_as_object": False,
    }
    defaulted_args = {
        **defaults,
        **pyarrow_additional_kwargs,
    }
    return defaulted_args


def _set_default_storage_options_kwargs(
    s3_additional_kwargs: Optional[Dict[str, Any]], session: boto3.Session
) -> Dict[str, Any]:
    if s3_additional_kwargs is None:
        s3_additional_kwargs = {}
    defaults = {key.upper(): value for key, value in _utils.boto3_to_primitives(boto3_session=session).items()}
    defaulted_args = {
        **defaults,
        **s3_additional_kwargs,
    }
    return defaulted_args
