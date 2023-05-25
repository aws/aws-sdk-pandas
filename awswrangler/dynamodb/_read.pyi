from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterator,
    List,
    Literal,
    Optional,
    Sequence,
    Union,
    overload,
)

import boto3
import pyarrow as pa
from boto3.dynamodb.conditions import ConditionBase

import awswrangler.pandas as pd

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.client import DynamoDBClient

_ItemsListType = List[Dict[str, Any]]

def _read_scan(
    dynamodb_client: Optional["DynamoDBClient"],
    as_dataframe: bool,
    kwargs: Dict[str, Any],
    segment: int,
) -> Union[pa.Table, _ItemsListType]: ...
@overload
def read_partiql_query(
    query: str,
    parameters: Optional[List[Any]] = ...,
    chunked: Literal[False] = ...,
    boto3_session: Optional[boto3.Session] = ...,
) -> pd.DataFrame: ...
@overload
def read_partiql_query(
    query: str,
    *,
    parameters: Optional[List[Any]] = ...,
    chunked: Literal[True],
    boto3_session: Optional[boto3.Session] = ...,
) -> Iterator[pd.DataFrame]: ...
@overload
def read_partiql_query(
    query: str,
    *,
    parameters: Optional[List[Any]] = ...,
    chunked: bool,
    boto3_session: Optional[boto3.Session] = ...,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]: ...
@overload
def read_items(
    table_name: str,
    index_name: Optional[str] = ...,
    partition_values: Optional[Sequence[Any]] = ...,
    sort_values: Optional[Sequence[Any]] = ...,
    filter_expression: Optional[Union[ConditionBase, str]] = ...,
    key_condition_expression: Optional[Union[ConditionBase, str]] = ...,
    expression_attribute_names: Optional[Dict[str, str]] = ...,
    expression_attribute_values: Optional[Dict[str, Any]] = ...,
    consistent: bool = ...,
    columns: Optional[Sequence[str]] = ...,
    allow_full_scan: bool = ...,
    max_items_evaluated: Optional[int] = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    as_dataframe: Literal[True] = ...,
    chunked: Literal[False] = ...,
    use_threads: Union[bool, int] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    pyarrow_additional_kwargs: Optional[Dict[str, Any]] = ...,
) -> pd.DataFrame: ...
@overload
def read_items(
    table_name: str,
    *,
    index_name: Optional[str] = ...,
    partition_values: Optional[Sequence[Any]] = ...,
    sort_values: Optional[Sequence[Any]] = ...,
    filter_expression: Optional[Union[ConditionBase, str]] = ...,
    key_condition_expression: Optional[Union[ConditionBase, str]] = ...,
    expression_attribute_names: Optional[Dict[str, str]] = ...,
    expression_attribute_values: Optional[Dict[str, Any]] = ...,
    consistent: bool = ...,
    columns: Optional[Sequence[str]] = ...,
    allow_full_scan: bool = ...,
    max_items_evaluated: Optional[int] = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    as_dataframe: Literal[True] = ...,
    chunked: Literal[True],
    use_threads: Union[bool, int] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    pyarrow_additional_kwargs: Optional[Dict[str, Any]] = ...,
) -> Iterator[pd.DataFrame]: ...
@overload
def read_items(
    table_name: str,
    *,
    index_name: Optional[str] = ...,
    partition_values: Optional[Sequence[Any]] = ...,
    sort_values: Optional[Sequence[Any]] = ...,
    filter_expression: Optional[Union[ConditionBase, str]] = ...,
    key_condition_expression: Optional[Union[ConditionBase, str]] = ...,
    expression_attribute_names: Optional[Dict[str, str]] = ...,
    expression_attribute_values: Optional[Dict[str, Any]] = ...,
    consistent: bool = ...,
    columns: Optional[Sequence[str]] = ...,
    allow_full_scan: bool = ...,
    max_items_evaluated: Optional[int] = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    as_dataframe: Literal[False],
    chunked: Literal[False] = ...,
    use_threads: Union[bool, int] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    pyarrow_additional_kwargs: Optional[Dict[str, Any]] = ...,
) -> _ItemsListType: ...
@overload
def read_items(
    table_name: str,
    *,
    index_name: Optional[str] = ...,
    partition_values: Optional[Sequence[Any]] = ...,
    sort_values: Optional[Sequence[Any]] = ...,
    filter_expression: Optional[Union[ConditionBase, str]] = ...,
    key_condition_expression: Optional[Union[ConditionBase, str]] = ...,
    expression_attribute_names: Optional[Dict[str, str]] = ...,
    expression_attribute_values: Optional[Dict[str, Any]] = ...,
    consistent: bool = ...,
    columns: Optional[Sequence[str]] = ...,
    allow_full_scan: bool = ...,
    max_items_evaluated: Optional[int] = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    as_dataframe: Literal[False],
    chunked: Literal[True],
    use_threads: Union[bool, int] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    pyarrow_additional_kwargs: Optional[Dict[str, Any]] = ...,
) -> Iterator[_ItemsListType]: ...
@overload
def read_items(
    table_name: str,
    *,
    index_name: Optional[str] = ...,
    partition_values: Optional[Sequence[Any]] = ...,
    sort_values: Optional[Sequence[Any]] = ...,
    filter_expression: Optional[Union[ConditionBase, str]] = ...,
    key_condition_expression: Optional[Union[ConditionBase, str]] = ...,
    expression_attribute_names: Optional[Dict[str, str]] = ...,
    expression_attribute_values: Optional[Dict[str, Any]] = ...,
    consistent: bool = ...,
    columns: Optional[Sequence[str]] = ...,
    allow_full_scan: bool = ...,
    max_items_evaluated: Optional[int] = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    as_dataframe: bool,
    chunked: Literal[False] = ...,
    use_threads: Union[bool, int] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    pyarrow_additional_kwargs: Optional[Dict[str, Any]] = ...,
) -> Union[pd.DataFrame, _ItemsListType]: ...
@overload
def read_items(
    table_name: str,
    *,
    index_name: Optional[str] = ...,
    partition_values: Optional[Sequence[Any]] = ...,
    sort_values: Optional[Sequence[Any]] = ...,
    filter_expression: Optional[Union[ConditionBase, str]] = ...,
    key_condition_expression: Optional[Union[ConditionBase, str]] = ...,
    expression_attribute_names: Optional[Dict[str, str]] = ...,
    expression_attribute_values: Optional[Dict[str, Any]] = ...,
    consistent: bool = ...,
    columns: Optional[Sequence[str]] = ...,
    allow_full_scan: bool = ...,
    max_items_evaluated: Optional[int] = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    as_dataframe: bool,
    chunked: Literal[True],
    use_threads: Union[bool, int] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    pyarrow_additional_kwargs: Optional[Dict[str, Any]] = ...,
) -> Union[Iterator[pd.DataFrame], Iterator[_ItemsListType]]: ...
@overload
def read_items(
    table_name: str,
    *,
    index_name: Optional[str] = ...,
    partition_values: Optional[Sequence[Any]] = ...,
    sort_values: Optional[Sequence[Any]] = ...,
    filter_expression: Optional[Union[ConditionBase, str]] = ...,
    key_condition_expression: Optional[Union[ConditionBase, str]] = ...,
    expression_attribute_names: Optional[Dict[str, str]] = ...,
    expression_attribute_values: Optional[Dict[str, Any]] = ...,
    consistent: bool = ...,
    columns: Optional[Sequence[str]] = ...,
    allow_full_scan: bool = ...,
    max_items_evaluated: Optional[int] = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    as_dataframe: Literal[True],
    chunked: bool,
    use_threads: Union[bool, int] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    pyarrow_additional_kwargs: Optional[Dict[str, Any]] = ...,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]: ...
@overload
def read_items(
    table_name: str,
    *,
    index_name: Optional[str] = ...,
    partition_values: Optional[Sequence[Any]] = ...,
    sort_values: Optional[Sequence[Any]] = ...,
    filter_expression: Optional[Union[ConditionBase, str]] = ...,
    key_condition_expression: Optional[Union[ConditionBase, str]] = ...,
    expression_attribute_names: Optional[Dict[str, str]] = ...,
    expression_attribute_values: Optional[Dict[str, Any]] = ...,
    consistent: bool = ...,
    columns: Optional[Sequence[str]] = ...,
    allow_full_scan: bool = ...,
    max_items_evaluated: Optional[int] = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    as_dataframe: Literal[False],
    chunked: bool,
    use_threads: Union[bool, int] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    pyarrow_additional_kwargs: Optional[Dict[str, Any]] = ...,
) -> Union[_ItemsListType, Iterator[_ItemsListType]]: ...
@overload
def read_items(
    table_name: str,
    *,
    index_name: Optional[str] = ...,
    partition_values: Optional[Sequence[Any]] = ...,
    sort_values: Optional[Sequence[Any]] = ...,
    filter_expression: Optional[Union[ConditionBase, str]] = ...,
    key_condition_expression: Optional[Union[ConditionBase, str]] = ...,
    expression_attribute_names: Optional[Dict[str, str]] = ...,
    expression_attribute_values: Optional[Dict[str, Any]] = ...,
    consistent: bool = ...,
    columns: Optional[Sequence[str]] = ...,
    allow_full_scan: bool = ...,
    max_items_evaluated: Optional[int] = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
    as_dataframe: bool,
    chunked: bool,
    use_threads: Union[bool, int] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    pyarrow_additional_kwargs: Optional[Dict[str, Any]] = ...,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame], _ItemsListType, Iterator[_ItemsListType]]: ...
