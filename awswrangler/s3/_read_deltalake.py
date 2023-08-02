"""Amazon S3 Read Delta Lake Module (PRIVATE)."""

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

import boto3
from typing_extensions import Literal

import awswrangler.pandas as pd
from awswrangler import _data_types, _utils
from awswrangler._config import apply_configs

if TYPE_CHECKING:
    try:
        import deltalake
    except ImportError:
        pass
else:
    deltalake = _utils.import_optional_dependency("deltalake")


def _set_default_storage_options_kwargs(
    boto3_session: Optional[boto3.Session], s3_additional_kwargs: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    defaults = {key.upper(): value for key, value in _utils.boto3_to_primitives(boto3_session=boto3_session).items()}
    defaults["AWS_REGION"] = defaults.pop("REGION_NAME")
    s3_additional_kwargs = s3_additional_kwargs or {}
    return {
        **defaults,
        **s3_additional_kwargs,
    }


@_utils.check_optional_dependency(deltalake, "deltalake")
@apply_configs
def read_deltalake(
    path: str,
    version: Optional[int] = None,
    partitions: Optional[List[Tuple[str, str, Any]]] = None,
    columns: Optional[List[str]] = None,
    without_files: bool = False,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = "numpy_nullable",
    use_threads: bool = True,
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
    path: str
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
    dtype_backend: str, optional
        Which dtype_backend to use, e.g. whether a DataFrame should have NumPy arrays,
        nullable dtypes are used for all dtypes that have a nullable implementation when
        “numpy_nullable” is set, pyarrow is used for all dtypes if “pyarrow” is set.

        The dtype_backends are still experimential. The "pyarrow" backend is only supported with Pandas 2.0 or above.
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        When enabled, os.cpu_count() is used as the max number of threads.
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
    arrow_kwargs = _data_types.pyarrow2pandas_defaults(
        use_threads=use_threads, kwargs=pyarrow_additional_kwargs, dtype_backend=dtype_backend
    )
    storage_options = _set_default_storage_options_kwargs(boto3_session, s3_additional_kwargs)
    return (
        deltalake.DeltaTable(
            table_uri=path,
            version=version,
            storage_options=storage_options,
            without_files=without_files,
        )
        .to_pyarrow_table(partitions=partitions, columns=columns)
        .to_pandas(**arrow_kwargs)
    )
