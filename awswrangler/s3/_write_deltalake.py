"""Amazon S3 Writer Delta Lake Module (PRIVATE)."""

from typing import TYPE_CHECKING, Any, Dict, List, Optional

import boto3
import pandas as pd
import pyarrow as pa

from awswrangler import _data_types, _utils
from awswrangler._arrow import _df_to_table

if TYPE_CHECKING:
    try:
        import deltalake
    except ImportError:
        pass
else:
    deltalake = _utils.import_optional_dependency("deltalake")


def _set_default_storage_options_kwargs(
    boto3_session: Optional[boto3.Session], s3_additional_kwargs: Optional[Dict[str, Any]], s3_allow_unsafe_rename: bool
) -> Dict[str, Any]:
    defaults = {key.upper(): value for key, value in _utils.boto3_to_primitives(boto3_session=boto3_session).items()}
    s3_additional_kwargs = s3_additional_kwargs or {}
    return {
        **defaults,
        **s3_additional_kwargs,
        "AWS_S3_ALLOW_UNSAFE_RENAME": "TRUE" if s3_allow_unsafe_rename else "FALSE",
    }


@_utils.check_optional_dependency(deltalake, "deltalake")
def to_deltalake(
    df: pd.DataFrame,
    path: Optional[str] = None,
    index: bool = False,
    mode: Optional[str] = None,
    dtype: Optional[Dict[str, str]] = None,
    partition_cols: Optional[List[str]] = None,
    overwrite_schema: bool = False,
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
    s3_allow_unsafe_rename: bool = True,
) -> None:
    mode = "append" if mode is None else mode
    dtype = dtype if dtype else {}

    schema: pa.Schema = _data_types.pyarrow_schema_from_pandas(
        df=df, index=index, ignore_cols=None, dtype=dtype
    )
    table: pa.Table = _df_to_table(df, schema, index, dtype)

    storage_options = _set_default_storage_options_kwargs(boto3_session, s3_additional_kwargs, s3_allow_unsafe_rename)
    deltalake.write_deltalake(
        table_or_uri=path,
        data=table,
        partition_by=partition_cols,
        mode=mode,
        overwrite_schema=overwrite_schema,
        storage_options=storage_options,
    )
