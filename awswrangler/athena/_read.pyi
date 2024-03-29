from typing import Any, Iterator, Literal, overload

import boto3
import pandas as pd

from awswrangler import typing
from awswrangler.athena._utils import _QueryMetadata

@overload
def get_query_results(
    query_execution_id: str,
    use_threads: bool | int = ...,
    boto3_session: boto3.Session | None = ...,
    categories: list[str] | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunksize: None | Literal[False] = ...,
    s3_additional_kwargs: dict[str, Any] | None = ...,
    pyarrow_additional_kwargs: dict[str, Any] | None = ...,
) -> pd.DataFrame: ...
@overload
def get_query_results(
    query_execution_id: str,
    *,
    use_threads: bool | int = ...,
    boto3_session: boto3.Session | None = ...,
    categories: list[str] | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunksize: Literal[True],
    s3_additional_kwargs: dict[str, Any] | None = ...,
    pyarrow_additional_kwargs: dict[str, Any] | None = ...,
) -> Iterator[pd.DataFrame]: ...
@overload
def get_query_results(
    query_execution_id: str,
    *,
    use_threads: bool | int = ...,
    boto3_session: boto3.Session | None = ...,
    categories: list[str] | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    chunksize: bool,
    s3_additional_kwargs: dict[str, Any] | None = ...,
    pyarrow_additional_kwargs: dict[str, Any] | None = ...,
) -> pd.DataFrame | Iterator[pd.DataFrame]: ...
@overload
def get_query_results(
    query_execution_id: str,
    *,
    use_threads: bool | int = ...,
    boto3_session: boto3.Session | None = ...,
    categories: list[str] | None = ...,
    chunksize: int,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    s3_additional_kwargs: dict[str, Any] | None = ...,
    pyarrow_additional_kwargs: dict[str, Any] | None = ...,
) -> Iterator[pd.DataFrame]: ...
@overload
def read_sql_query(
    sql: str,
    database: str,
    ctas_approach: bool = ...,
    unload_approach: bool = ...,
    ctas_parameters: typing.AthenaCTASSettings | None = ...,
    unload_parameters: typing.AthenaUNLOADSettings | None = ...,
    categories: list[str] | None = ...,
    chunksize: None | Literal[False] = ...,
    s3_output: str | None = ...,
    workgroup: str = ...,
    encryption: str | None = ...,
    kms_key: str | None = ...,
    keep_files: bool = ...,
    use_threads: bool | int = ...,
    boto3_session: boto3.Session | None = ...,
    athena_cache_settings: typing.AthenaCacheSettings | None = ...,
    data_source: str | None = ...,
    athena_query_wait_polling_delay: float = ...,
    params: dict[str, Any] | list[str] | None = ...,
    paramstyle: Literal["qmark", "named"] = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    s3_additional_kwargs: dict[str, Any] | None = ...,
    pyarrow_additional_kwargs: dict[str, Any] | None = ...,
) -> pd.DataFrame: ...
@overload
def read_sql_query(
    sql: str,
    database: str,
    *,
    ctas_approach: bool = ...,
    unload_approach: bool = ...,
    ctas_parameters: typing.AthenaCTASSettings | None = ...,
    unload_parameters: typing.AthenaUNLOADSettings | None = ...,
    categories: list[str] | None = ...,
    chunksize: Literal[True],
    s3_output: str | None = ...,
    workgroup: str = ...,
    encryption: str | None = ...,
    kms_key: str | None = ...,
    keep_files: bool = ...,
    use_threads: bool | int = ...,
    boto3_session: boto3.Session | None = ...,
    athena_cache_settings: typing.AthenaCacheSettings | None = ...,
    data_source: str | None = ...,
    athena_query_wait_polling_delay: float = ...,
    params: dict[str, Any] | list[str] | None = ...,
    paramstyle: Literal["qmark", "named"] = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    s3_additional_kwargs: dict[str, Any] | None = ...,
    pyarrow_additional_kwargs: dict[str, Any] | None = ...,
) -> Iterator[pd.DataFrame]: ...
@overload
def read_sql_query(
    sql: str,
    database: str,
    *,
    ctas_approach: bool = ...,
    unload_approach: bool = ...,
    ctas_parameters: typing.AthenaCTASSettings | None = ...,
    unload_parameters: typing.AthenaUNLOADSettings | None = ...,
    categories: list[str] | None = ...,
    chunksize: bool,
    s3_output: str | None = ...,
    workgroup: str = ...,
    encryption: str | None = ...,
    kms_key: str | None = ...,
    keep_files: bool = ...,
    use_threads: bool | int = ...,
    boto3_session: boto3.Session | None = ...,
    athena_cache_settings: typing.AthenaCacheSettings | None = ...,
    data_source: str | None = ...,
    athena_query_wait_polling_delay: float = ...,
    params: dict[str, Any] | list[str] | None = ...,
    paramstyle: Literal["qmark", "named"] = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    s3_additional_kwargs: dict[str, Any] | None = ...,
    pyarrow_additional_kwargs: dict[str, Any] | None = ...,
) -> pd.DataFrame | Iterator[pd.DataFrame]: ...
@overload
def read_sql_query(
    sql: str,
    database: str,
    *,
    ctas_approach: bool = ...,
    unload_approach: bool = ...,
    ctas_parameters: typing.AthenaCTASSettings | None = ...,
    unload_parameters: typing.AthenaUNLOADSettings | None = ...,
    categories: list[str] | None = ...,
    chunksize: int,
    s3_output: str | None = ...,
    workgroup: str = ...,
    encryption: str | None = ...,
    kms_key: str | None = ...,
    keep_files: bool = ...,
    use_threads: bool | int = ...,
    boto3_session: boto3.Session | None = ...,
    athena_cache_settings: typing.AthenaCacheSettings | None = ...,
    data_source: str | None = ...,
    athena_query_wait_polling_delay: float = ...,
    params: dict[str, Any] | list[str] | None = ...,
    paramstyle: Literal["qmark", "named"] = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    s3_additional_kwargs: dict[str, Any] | None = ...,
    pyarrow_additional_kwargs: dict[str, Any] | None = ...,
) -> Iterator[pd.DataFrame]: ...
@overload
def read_sql_query(
    sql: str,
    database: str,
    *,
    ctas_approach: bool = ...,
    unload_approach: bool = ...,
    ctas_parameters: typing.AthenaCTASSettings | None = ...,
    unload_parameters: typing.AthenaUNLOADSettings | None = ...,
    categories: list[str] | None = ...,
    chunksize: int | bool | None,
    s3_output: str | None = ...,
    workgroup: str = ...,
    encryption: str | None = ...,
    kms_key: str | None = ...,
    keep_files: bool = ...,
    use_threads: bool | int = ...,
    boto3_session: boto3.Session | None = ...,
    athena_cache_settings: typing.AthenaCacheSettings | None = ...,
    data_source: str | None = ...,
    athena_query_wait_polling_delay: float = ...,
    params: dict[str, Any] | list[str] | None = ...,
    paramstyle: Literal["qmark", "named"] = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    s3_additional_kwargs: dict[str, Any] | None = ...,
    pyarrow_additional_kwargs: dict[str, Any] | None = ...,
) -> pd.DataFrame | Iterator[pd.DataFrame]: ...
@overload
def read_sql_table(
    table: str,
    database: str,
    *,
    unload_approach: bool = ...,
    unload_parameters: typing.AthenaUNLOADSettings | None = ...,
    ctas_approach: bool = ...,
    ctas_parameters: typing.AthenaCTASSettings | None = ...,
    categories: list[str] | None = ...,
    chunksize: None | Literal[False] = ...,
    s3_output: str | None = ...,
    workgroup: str = ...,
    encryption: str | None = ...,
    kms_key: str | None = ...,
    keep_files: bool = ...,
    use_threads: bool | int = ...,
    boto3_session: boto3.Session | None = ...,
    athena_cache_settings: typing.AthenaCacheSettings | None = ...,
    data_source: str | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    s3_additional_kwargs: dict[str, Any] | None = ...,
    pyarrow_additional_kwargs: dict[str, Any] | None = ...,
) -> pd.DataFrame: ...
@overload
def read_sql_table(
    table: str,
    database: str,
    *,
    unload_approach: bool = ...,
    unload_parameters: typing.AthenaUNLOADSettings | None = ...,
    ctas_approach: bool = ...,
    ctas_parameters: typing.AthenaCTASSettings | None = ...,
    categories: list[str] | None = ...,
    chunksize: Literal[True],
    s3_output: str | None = ...,
    workgroup: str = ...,
    encryption: str | None = ...,
    kms_key: str | None = ...,
    keep_files: bool = ...,
    use_threads: bool | int = ...,
    boto3_session: boto3.Session | None = ...,
    athena_cache_settings: typing.AthenaCacheSettings | None = ...,
    data_source: str | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    s3_additional_kwargs: dict[str, Any] | None = ...,
    pyarrow_additional_kwargs: dict[str, Any] | None = ...,
) -> Iterator[pd.DataFrame]: ...
@overload
def read_sql_table(
    table: str,
    database: str,
    *,
    unload_approach: bool = ...,
    unload_parameters: typing.AthenaUNLOADSettings | None = ...,
    ctas_approach: bool = ...,
    ctas_parameters: typing.AthenaCTASSettings | None = ...,
    categories: list[str] | None = ...,
    chunksize: bool,
    s3_output: str | None = ...,
    workgroup: str = ...,
    encryption: str | None = ...,
    kms_key: str | None = ...,
    keep_files: bool = ...,
    use_threads: bool | int = ...,
    boto3_session: boto3.Session | None = ...,
    athena_cache_settings: typing.AthenaCacheSettings | None = ...,
    data_source: str | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    s3_additional_kwargs: dict[str, Any] | None = ...,
    pyarrow_additional_kwargs: dict[str, Any] | None = ...,
) -> pd.DataFrame | Iterator[pd.DataFrame]: ...
@overload
def read_sql_table(
    table: str,
    database: str,
    *,
    unload_approach: bool = ...,
    unload_parameters: typing.AthenaUNLOADSettings | None = ...,
    ctas_approach: bool = ...,
    ctas_parameters: typing.AthenaCTASSettings | None = ...,
    categories: list[str] | None = ...,
    chunksize: int,
    s3_output: str | None = ...,
    workgroup: str = ...,
    encryption: str | None = ...,
    kms_key: str | None = ...,
    keep_files: bool = ...,
    use_threads: bool | int = ...,
    boto3_session: boto3.Session | None = ...,
    athena_cache_settings: typing.AthenaCacheSettings | None = ...,
    data_source: str | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    s3_additional_kwargs: dict[str, Any] | None = ...,
    pyarrow_additional_kwargs: dict[str, Any] | None = ...,
) -> Iterator[pd.DataFrame]: ...
@overload
def read_sql_table(
    table: str,
    database: str,
    *,
    unload_approach: bool = ...,
    unload_parameters: typing.AthenaUNLOADSettings | None = ...,
    ctas_approach: bool = ...,
    ctas_parameters: typing.AthenaCTASSettings | None = ...,
    categories: list[str] | None = ...,
    chunksize: int | bool | None,
    s3_output: str | None = ...,
    workgroup: str = ...,
    encryption: str | None = ...,
    kms_key: str | None = ...,
    keep_files: bool = ...,
    use_threads: bool | int = ...,
    boto3_session: boto3.Session | None = ...,
    athena_cache_settings: typing.AthenaCacheSettings | None = ...,
    data_source: str | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    s3_additional_kwargs: dict[str, Any] | None = ...,
    pyarrow_additional_kwargs: dict[str, Any] | None = ...,
) -> pd.DataFrame | Iterator[pd.DataFrame]: ...
def unload(
    sql: str,
    path: str,
    database: str,
    file_format: str = ...,
    compression: str | None = ...,
    field_delimiter: str | None = ...,
    partitioned_by: list[str] | None = ...,
    workgroup: str = ...,
    encryption: str | None = ...,
    kms_key: str | None = ...,
    boto3_session: boto3.Session | None = ...,
    data_source: str | None = ...,
    params: dict[str, Any] | list[str] | None = ...,
    paramstyle: Literal["qmark", "named"] = ...,
    athena_query_wait_polling_delay: float = ...,
) -> _QueryMetadata: ...
