"""Amazon S3 Tables Iceberg Read/Write Module (PRIVATE)."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Literal

import boto3
import pyarrow as pa

import awswrangler.pandas as pd
from awswrangler import _data_types, _utils, exceptions
from awswrangler._arrow import _df_to_table, _table_to_df
from awswrangler._config import apply_configs

if TYPE_CHECKING:
    try:
        import pyiceberg
    except ImportError:
        pass
else:
    pyiceberg = _utils.import_optional_dependency("pyiceberg")

_logger: logging.Logger = logging.getLogger(__name__)


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
    Reading an entire table:

    >>> import awswrangler as wr
    >>> df = wr.s3.from_iceberg(
    ...     table_bucket_arn="arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
    ...     namespace="my_namespace",
    ...     table_name="my_table",
    ... )

    Reading with row filtering and limit:

    >>> df = wr.s3.from_iceberg(
    ...     table_bucket_arn="arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
    ...     namespace="my_namespace",
    ...     table_name="my_table",
    ...     row_filter="amount > 50.0",
    ...     limit=100,
    ... )
    """
    from pyiceberg.exceptions import NoSuchTableError, RESTError  # noqa: PLC0415
    from pyiceberg.expressions import AlwaysTrue  # noqa: PLC0415

    from awswrangler.s3._s3_tables_catalog import _load_catalog  # noqa: PLC0415

    catalog = _load_catalog(table_bucket_arn, boto3_session)
    identifier = f"{namespace}.{table_name}"

    try:
        table = catalog.load_table(identifier)
    except RESTError as e:
        if "no_such_bucket" in str(e):
            raise exceptions.InvalidArgumentValue(
                f"Table bucket not found: {table_bucket_arn}. Create it with wr.s3.create_table_bucket()."
            ) from e
        raise
    except NoSuchTableError as e:
        raise exceptions.InvalidArgumentValue(
            f"Table '{table_name}' not found in namespace '{namespace}'. "
            "Verify the namespace exists (wr.s3.create_namespace()) "
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

    return _table_to_df(table=arrow_table, kwargs=arrow_kwargs)


@_utils.check_optional_dependency(pyiceberg, "pyiceberg")
def to_iceberg(
    df: pd.DataFrame,
    table_bucket_arn: str,
    namespace: str,
    table_name: str,
    mode: Literal["append", "overwrite"] = "append",
    index: bool = False,
    dtype: dict[str, str] | None = None,
    boto3_session: boto3.Session | None = None,
) -> None:
    """Write a Pandas DataFrame to an S3 Table via PyIceberg.

    If the table does not exist, it is automatically created with a schema
    inferred from the DataFrame.

    This function requires the ``pyiceberg`` package.
    Install it with ``pip install awswrangler[pyiceberg]``.

    Parameters
    ----------
    df : pd.DataFrame
        Pandas DataFrame to write.
    table_bucket_arn : str
        The ARN of the S3 table bucket.
    namespace : str
        The namespace of the table.
    table_name : str
        The name of the table to write to.
    mode : str, optional
        Write mode. ``"append"`` (default) adds rows to the table.
        ``"overwrite"`` replaces all existing data.
    index : bool, optional
        If True, include the DataFrame index as a column. Default is False.
    dtype : dict[str, str], optional
        Dictionary of column names and Athena/Glue types to cast.
        (e.g. ``{"col_name": "bigint", "col2_name": "int"}``).
    boto3_session : boto3.Session, optional
        Boto3 Session. If None, the default boto3 session is used.

    Examples
    --------
    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_iceberg(
    ...     df=pd.DataFrame({"col": [1, 2, 3]}),
    ...     table_bucket_arn="arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
    ...     namespace="my_namespace",
    ...     table_name="my_table",
    ... )
    """
    from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError, RESTError  # noqa: PLC0415

    from awswrangler.s3._s3_tables_catalog import _load_catalog  # noqa: PLC0415

    dtype = dtype if dtype else {}
    schema: pa.Schema = _data_types.pyarrow_schema_from_pandas(df=df, index=index, ignore_cols=None, dtype=dtype)
    arrow_table: pa.Table = _df_to_table(df, schema, index, dtype)

    catalog = _load_catalog(table_bucket_arn, boto3_session)
    identifier = f"{namespace}.{table_name}"

    try:
        iceberg_table = catalog.load_table(identifier)
    except RESTError as e:
        if "no_such_bucket" in str(e):
            raise exceptions.InvalidArgumentValue(
                f"Table bucket not found: {table_bucket_arn}. Create it with wr.s3.create_table_bucket()."
            ) from e
        raise
    except NoSuchTableError:
        _logger.info("Table %s does not exist, creating it.", identifier)
        try:
            iceberg_table = catalog.create_table(identifier=identifier, schema=arrow_table.schema)
        except NoSuchNamespaceError as e:
            raise exceptions.InvalidArgumentValue(
                f"Namespace '{namespace}' not found in table bucket {table_bucket_arn}. "
                "Create it with wr.s3.create_namespace()."
            ) from e

    if mode == "overwrite":
        iceberg_table.overwrite(arrow_table)
    else:
        iceberg_table.append(arrow_table)
