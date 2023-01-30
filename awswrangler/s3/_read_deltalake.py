"""Amazon S3 Read Delta Lake Module (PRIVATE)."""
import importlib.util
from typing import Any, Dict, List, Optional, Tuple

import boto3
import pandas as pd

from awswrangler import _utils

_deltalake_found = importlib.util.find_spec("deltalake")
if _deltalake_found:
    from deltalake import DeltaTable  # pylint: disable=import-error


def _set_default_storage_options_kwargs(
    boto3_session: boto3.Session, s3_additional_kwargs: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    defaults = {key.upper(): value for key, value in _utils.boto3_to_primitives(boto3_session=boto3_session).items()}
    s3_additional_kwargs = s3_additional_kwargs or {}
    return {
        **defaults,
        **s3_additional_kwargs,
    }


def read_deltalake(
    path: Optional[str] = None,
    version: Optional[int] = None,
    partitions: Optional[List[Tuple[str, str, Any]]] = None,
    columns: Optional[List[str]] = None,
    without_files: bool = False,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
    pyarrow_additional_kwargs: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """Load a Deltalake table data from an S3 path.

    This function requires the `deltalake package
    <https://delta-io.github.io/delta-rs/python>`__.
    See the `How to load a Delta table
    <https://delta-io.github.io/delta-rs/python/usage.html#loading-a-delta-table>`__
    guide for loading instructions.

    Parameters
    ----------
    path: Optional[str]
        The path of the DeltaTable.
    version: Optional[int]
        The version of the DeltaTable.
    partitions: Optional[List[Tuple[str, str, Any]]
        A list of partition filters, see help(DeltaTable.files_by_partitions)
        for filter syntax.
    columns: Optional[List[str]]
        The columns to project. This can be a list of column names to include
        (order and duplicates are preserved).
    without_files: bool
        If True, load the table without tracking files (memory-friendly).
        Some append-only applications might not need to track files.
    boto3_session: Optional[boto3.Session()]
        Boto3 Session. If None, the default boto3 session is used.
    s3_additional_kwargs: Optional[Dict[str, str]]
        Forwarded to the Delta Table class for the storage options of the S3 backend.
    pyarrow_additional_kwargs: Optional[Dict[str, str]]
        Forwarded to the PyArrow to_pandas method.

    Returns
    -------
    df: pd.DataFrame
        DataFrame with the results.

    See Also
    --------
    deltalake.DeltaTable : Create a DeltaTable instance with the deltalake library.
    """
    pyarrow_additional_kwargs = pyarrow_additional_kwargs or {}  # TODO: Use defaults in 3.0.0 # pylint: disable=fixme
    storage_options = _set_default_storage_options_kwargs(boto3_session, s3_additional_kwargs)
    return (
        DeltaTable(
            table_uri=path,
            version=version,
            storage_options=storage_options,
            without_files=without_files,
        )
        .to_pyarrow_table(partitions=partitions, columns=columns)
        .to_pandas(**pyarrow_additional_kwargs)
    )
