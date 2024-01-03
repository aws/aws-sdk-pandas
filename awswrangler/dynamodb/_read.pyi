from typing import (
    TYPE_CHECKING,
    Any,
    Iterator,
    Literal,
    Sequence,
    overload,
)

import boto3
import pyarrow as pa
from boto3.dynamodb.conditions import ConditionBase

import awswrangler.pandas as pd

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.client import DynamoDBClient

_ItemsListType = list[dict[str, Any]]

def _read_scan(
    dynamodb_client: "DynamoDBClient" | None,
    as_dataframe: bool,
    kwargs: dict[str, Any],
    schema: pa.Schema | None,
    segment: int,
) -> pa.Table | _ItemsListType: ...
@overload
def read_partiql_query(
    query: str,
    parameters: list[Any] | None = ...,
    chunked: Literal[False] = ...,
    boto3_session: boto3.Session | None = ...,
) -> pd.DataFrame: ...
@overload
def read_partiql_query(
    query: str,
    *,
    parameters: list[Any] | None = ...,
    chunked: Literal[True],
    boto3_session: boto3.Session | None = ...,
) -> Iterator[pd.DataFrame]: ...
@overload
def read_partiql_query(
    query: str,
    *,
    parameters: list[Any] | None = ...,
    chunked: bool,
    boto3_session: boto3.Session | None = ...,
) -> pd.DataFrame | Iterator[pd.DataFrame]: ...
@overload
def read_items(
    table_name: str,
    index_name: str | None = ...,
    partition_values: Sequence[Any] | None = ...,
    sort_values: Sequence[Any] | None = ...,
    filter_expression: ConditionBase | str | None = ...,
    key_condition_expression: ConditionBase | str | None = ...,
    expression_attribute_names: dict[str, str] | None = ...,
    expression_attribute_values: dict[str, Any] | None = ...,
    consistent: bool = ...,
    columns: Sequence[str] | None = ...,
    allow_full_scan: bool = ...,
    max_items_evaluated: int | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    as_dataframe: Literal[True] = ...,
    chunked: Literal[False] = ...,
    use_threads: bool | int = ...,
    boto3_session: boto3.Session | None = ...,
    pyarrow_additional_kwargs: dict[str, Any] | None = ...,
) -> pd.DataFrame: ...
@overload
def read_items(
    table_name: str,
    *,
    index_name: str | None = ...,
    partition_values: Sequence[Any] | None = ...,
    sort_values: Sequence[Any] | None = ...,
    filter_expression: ConditionBase | str | None = ...,
    key_condition_expression: ConditionBase | str | None = ...,
    expression_attribute_names: dict[str, str] | None = ...,
    expression_attribute_values: dict[str, Any] | None = ...,
    consistent: bool = ...,
    columns: Sequence[str] | None = ...,
    allow_full_scan: bool = ...,
    max_items_evaluated: int | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    as_dataframe: Literal[True] = ...,
    chunked: Literal[True],
    use_threads: bool | int = ...,
    boto3_session: boto3.Session | None = ...,
    pyarrow_additional_kwargs: dict[str, Any] | None = ...,
) -> Iterator[pd.DataFrame]: ...
@overload
def read_items(
    table_name: str,
    *,
    index_name: str | None = ...,
    partition_values: Sequence[Any] | None = ...,
    sort_values: Sequence[Any] | None = ...,
    filter_expression: ConditionBase | str | None = ...,
    key_condition_expression: ConditionBase | str | None = ...,
    expression_attribute_names: dict[str, str] | None = ...,
    expression_attribute_values: dict[str, Any] | None = ...,
    consistent: bool = ...,
    columns: Sequence[str] | None = ...,
    allow_full_scan: bool = ...,
    max_items_evaluated: int | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    as_dataframe: Literal[False],
    chunked: Literal[False] = ...,
    use_threads: bool | int = ...,
    boto3_session: boto3.Session | None = ...,
    pyarrow_additional_kwargs: dict[str, Any] | None = ...,
) -> _ItemsListType: ...
@overload
def read_items(
    table_name: str,
    *,
    index_name: str | None = ...,
    partition_values: Sequence[Any] | None = ...,
    sort_values: Sequence[Any] | None = ...,
    filter_expression: ConditionBase | str | None = ...,
    key_condition_expression: ConditionBase | str | None = ...,
    expression_attribute_names: dict[str, str] | None = ...,
    expression_attribute_values: dict[str, Any] | None = ...,
    consistent: bool = ...,
    columns: Sequence[str] | None = ...,
    allow_full_scan: bool = ...,
    max_items_evaluated: int | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    as_dataframe: Literal[False],
    chunked: Literal[True],
    use_threads: bool | int = ...,
    boto3_session: boto3.Session | None = ...,
    pyarrow_additional_kwargs: dict[str, Any] | None = ...,
) -> Iterator[_ItemsListType]: ...
@overload
def read_items(
    table_name: str,
    *,
    index_name: str | None = ...,
    partition_values: Sequence[Any] | None = ...,
    sort_values: Sequence[Any] | None = ...,
    filter_expression: ConditionBase | str | None = ...,
    key_condition_expression: ConditionBase | str | None = ...,
    expression_attribute_names: dict[str, str] | None = ...,
    expression_attribute_values: dict[str, Any] | None = ...,
    consistent: bool = ...,
    columns: Sequence[str] | None = ...,
    allow_full_scan: bool = ...,
    max_items_evaluated: int | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    as_dataframe: bool,
    chunked: Literal[False] = ...,
    use_threads: bool | int = ...,
    boto3_session: boto3.Session | None = ...,
    pyarrow_additional_kwargs: dict[str, Any] | None = ...,
) -> pd.DataFrame | _ItemsListType: ...
@overload
def read_items(
    table_name: str,
    *,
    index_name: str | None = ...,
    partition_values: Sequence[Any] | None = ...,
    sort_values: Sequence[Any] | None = ...,
    filter_expression: ConditionBase | str | None = ...,
    key_condition_expression: ConditionBase | str | None = ...,
    expression_attribute_names: dict[str, str] | None = ...,
    expression_attribute_values: dict[str, Any] | None = ...,
    consistent: bool = ...,
    columns: Sequence[str] | None = ...,
    allow_full_scan: bool = ...,
    max_items_evaluated: int | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    as_dataframe: bool,
    chunked: Literal[True],
    use_threads: bool | int = ...,
    boto3_session: boto3.Session | None = ...,
    pyarrow_additional_kwargs: dict[str, Any] | None = ...,
) -> Iterator[pd.DataFrame] | Iterator[_ItemsListType]: ...
@overload
def read_items(
    table_name: str,
    *,
    index_name: str | None = ...,
    partition_values: Sequence[Any] | None = ...,
    sort_values: Sequence[Any] | None = ...,
    filter_expression: ConditionBase | str | None = ...,
    key_condition_expression: ConditionBase | str | None = ...,
    expression_attribute_names: dict[str, str] | None = ...,
    expression_attribute_values: dict[str, Any] | None = ...,
    consistent: bool = ...,
    columns: Sequence[str] | None = ...,
    allow_full_scan: bool = ...,
    max_items_evaluated: int | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    as_dataframe: Literal[True],
    chunked: bool,
    use_threads: bool | int = ...,
    boto3_session: boto3.Session | None = ...,
    pyarrow_additional_kwargs: dict[str, Any] | None = ...,
) -> pd.DataFrame | Iterator[pd.DataFrame]: ...
@overload
def read_items(
    table_name: str,
    *,
    index_name: str | None = ...,
    partition_values: Sequence[Any] | None = ...,
    sort_values: Sequence[Any] | None = ...,
    filter_expression: ConditionBase | str | None = ...,
    key_condition_expression: ConditionBase | str | None = ...,
    expression_attribute_names: dict[str, str] | None = ...,
    expression_attribute_values: dict[str, Any] | None = ...,
    consistent: bool = ...,
    columns: Sequence[str] | None = ...,
    allow_full_scan: bool = ...,
    max_items_evaluated: int | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    as_dataframe: Literal[False],
    chunked: bool,
    use_threads: bool | int = ...,
    boto3_session: boto3.Session | None = ...,
    pyarrow_additional_kwargs: dict[str, Any] | None = ...,
) -> _ItemsListType | Iterator[_ItemsListType]: ...
@overload
def read_items(
    table_name: str,
    *,
    index_name: str | None = ...,
    partition_values: Sequence[Any] | None = ...,
    sort_values: Sequence[Any] | None = ...,
    filter_expression: ConditionBase | str | None = ...,
    key_condition_expression: ConditionBase | str | None = ...,
    expression_attribute_names: dict[str, str] | None = ...,
    expression_attribute_values: dict[str, Any] | None = ...,
    consistent: bool = ...,
    columns: Sequence[str] | None = ...,
    allow_full_scan: bool = ...,
    max_items_evaluated: int | None = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    as_dataframe: bool,
    chunked: bool,
    use_threads: bool | int = ...,
    boto3_session: boto3.Session | None = ...,
    pyarrow_additional_kwargs: dict[str, Any] | None = ...,
) -> pd.DataFrame | Iterator[pd.DataFrame] | _ItemsListType | Iterator[_ItemsListType]: ...
