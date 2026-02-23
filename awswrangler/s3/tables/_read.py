"""Amazon S3 Tables Read Module (PRIVATE)."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import boto3
from typing_extensions import Literal

import awswrangler.pandas as pd
from awswrangler import _data_types, _utils, exceptions
from awswrangler._config import apply_configs

if TYPE_CHECKING:
    try:
        import pyiceberg
    except ImportError:
        pass
else:
    pyiceberg = _utils.import_optional_dependency("pyiceberg")


@_utils.check_optional_dependency(pyiceberg, "pyiceberg")
@apply_configs
def from_iceberg(
    table_bucket_arn: str,
    namespace: str,
    table_name: str,
    columns: list[str] | None = None,
    row_filter: str | None = None,
    snapshot_id: int | None = None,
    limit: int | None = None,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = "numpy_nullable",
    boto3_session: boto3.Session | None = None,
    pyarrow_additional_kwargs: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Read an S3 Table into a Pandas DataFrame via PyIceberg.

    This function requires the ``pyiceberg`` package.
    Install it with ``pip install awswrangler[pyiceberg]``.

    Parameters
    ----------
    table_bucket_arn : str
        The ARN of the S3 table bucket.
    namespace : str
        The namespace of the table.
    table_name : str
        The name of the table to read.
    columns : list[str], optional
        List of column names to read. If None, all columns are read.
    row_filter : str, optional
        A row filter expression (e.g. ``"col > 5"``). If None, all rows are read.
    snapshot_id : int, optional
        A specific snapshot ID to read. If None, the latest snapshot is read.
    limit : int, optional
        Maximum number of rows to return. If None, all rows are returned.
    dtype_backend : str, optional
        Which dtype_backend to use. ``"numpy_nullable"`` or ``"pyarrow"``.
    boto3_session : boto3.Session, optional
        Boto3 Session. If None, the default boto3 session is used.
    pyarrow_additional_kwargs : dict[str, Any], optional
        Additional keyword arguments forwarded to PyArrow's ``to_pandas()`` method.

    Returns
    -------
    pd.DataFrame
        DataFrame with the table data.

    Examples
    --------
    >>> import awswrangler as wr
    >>> df = wr.s3.tables.from_iceberg(
    ...     table_bucket_arn="arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
    ...     namespace="my_namespace",
    ...     table_name="my_table",
    ... )
    """
    from pyiceberg.exceptions import NoSuchTableError, RESTError
    from pyiceberg.expressions import AlwaysTrue
    from pyiceberg.table import StaticTable

    from awswrangler.s3.tables._catalog import _load_catalog

    catalog = _load_catalog(table_bucket_arn, boto3_session)
    identifier = f"{namespace}.{table_name}"

    try:
        table = catalog.load_table(identifier)
    except RESTError as e:
        if "no_such_bucket" in str(e):
            raise exceptions.InvalidArgumentValue(
                f"Table bucket not found: {table_bucket_arn}. "
                "Create it with wr.s3.tables.create_table_bucket()."
            ) from e
        raise
    except NoSuchTableError as e:
        raise exceptions.InvalidArgumentValue(
            f"Table '{table_name}' not found in namespace '{namespace}'. "
            "Verify the namespace exists (wr.s3.tables.create_namespace()) "
            "and the table name is correct."
        ) from e

    scan_kwargs: dict[str, Any] = {}
    if columns is not None:
        scan_kwargs["selected_fields"] = tuple(columns)
    if row_filter is not None:
        scan_kwargs["row_filter"] = row_filter
    else:
        scan_kwargs["row_filter"] = AlwaysTrue()
    if snapshot_id is not None:
        scan_kwargs["snapshot_id"] = snapshot_id
    if limit is not None:
        scan_kwargs["limit"] = limit

    scan = table.scan(**scan_kwargs)
    arrow_table = scan.to_arrow()

    arrow_kwargs = _data_types.pyarrow2pandas_defaults(
        use_threads=True,
        kwargs=pyarrow_additional_kwargs,
        dtype_backend=dtype_backend,
    )

    return arrow_table.to_pandas(**arrow_kwargs)
