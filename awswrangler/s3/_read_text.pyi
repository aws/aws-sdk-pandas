import datetime as dt
from typing import Any, Callable, Dict, Iterator, List, Literal, Optional, Union, overload

import boto3
import pandas as pd

@overload
def read_csv(
    path: Union[str, List[str]],
    chunksize: Literal[None, False] = ...,
    path_suffix: Union[str, List[str], None] = ...,
    path_ignore_suffix: Union[str, List[str], None] = ...,
    version_id: Optional[Union[str, Dict[str, str]]] = ...,
    ignore_empty: bool = ...,
    use_threads: Union[bool, int] = ...,
    last_modified_begin: Optional[dt.datetime] = ...,
    last_modified_end: Optional[dt.datetime] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    s3_additional_kwargs: Optional[Dict[str, Any]] = ...,
    dataset: bool = ...,
    partition_filter: Optional[Callable[[Dict[str, str]], bool]] = ...,
    parallelism: int = ...,
    **pandas_kwargs: Any,
) -> pd.DataFrame: ...
@overload
def read_csv(
    path: Union[str, List[str]],
    chunksize: Literal[True],
    path_suffix: Union[str, List[str], None] = ...,
    path_ignore_suffix: Union[str, List[str], None] = ...,
    version_id: Optional[Union[str, Dict[str, str]]] = ...,
    ignore_empty: bool = ...,
    use_threads: Union[bool, int] = ...,
    last_modified_begin: Optional[dt.datetime] = ...,
    last_modified_end: Optional[dt.datetime] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    s3_additional_kwargs: Optional[Dict[str, Any]] = ...,
    dataset: bool = ...,
    partition_filter: Optional[Callable[[Dict[str, str]], bool]] = ...,
    parallelism: int = ...,
    **pandas_kwargs: Any,
) -> Iterator[pd.DataFrame]: ...
@overload
def read_csv(
    path: Union[str, List[str]],
    chunksize: Union[int, bool],
    path_suffix: Union[str, List[str], None] = ...,
    path_ignore_suffix: Union[str, List[str], None] = ...,
    version_id: Optional[Union[str, Dict[str, str]]] = ...,
    ignore_empty: bool = ...,
    use_threads: Union[bool, int] = ...,
    last_modified_begin: Optional[dt.datetime] = ...,
    last_modified_end: Optional[dt.datetime] = ...,
    boto3_session: Optional[boto3.Session] = ...,
    s3_additional_kwargs: Optional[Dict[str, Any]] = ...,
    dataset: bool = ...,
    partition_filter: Optional[Callable[[Dict[str, str]], bool]] = ...,
    parallelism: int = ...,
    **pandas_kwargs: Any,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]: ...
