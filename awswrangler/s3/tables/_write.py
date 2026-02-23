"""Amazon S3 Tables Write Module (PRIVATE)."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Literal

import boto3
import pyarrow as pa

import awswrangler.pandas as pd
from awswrangler import _data_types, _utils, exceptions
from awswrangler._arrow import _df_to_table

if TYPE_CHECKING:
    try:
        import pyiceberg
    except ImportError:
        pass
else:
    pyiceberg = _utils.import_optional_dependency("pyiceberg")

_logger: logging.Logger = logging.getLogger(__name__)


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
    >>> wr.s3.tables.to_iceberg(
    ...     df=pd.DataFrame({"col": [1, 2, 3]}),
    ...     table_bucket_arn="arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
    ...     namespace="my_namespace",
    ...     table_name="my_table",
    ... )
    """
    from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError, RESTError

    from awswrangler.s3.tables._catalog import _load_catalog

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
                f"Table bucket not found: {table_bucket_arn}. "
                "Create it with wr.s3.tables.create_table_bucket()."
            ) from e
        raise
    except NoSuchTableError:
        _logger.info("Table %s does not exist, creating it.", identifier)
        try:
            iceberg_table = catalog.create_table(identifier=identifier, schema=arrow_table.schema)
        except NoSuchNamespaceError as e:
            raise exceptions.InvalidArgumentValue(
                f"Namespace '{namespace}' not found in table bucket {table_bucket_arn}. "
                "Create it with wr.s3.tables.create_namespace()."
            ) from e

    if mode == "overwrite":
        iceberg_table.overwrite(arrow_table)
    else:
        iceberg_table.append(arrow_table)
