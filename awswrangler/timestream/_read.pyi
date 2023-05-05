from typing import Any, Dict, Iterator, Literal, Optional, Union, overload

import boto3
import pandas as pd

@overload
def query(
    sql: str,
    chunked: Literal[False] = ...,
    pagination_config: Optional[Dict[str, Any]] = ...,
    boto3_session: Optional[boto3.Session] = ...,
) -> pd.DataFrame: ...
@overload
def query(
    sql: str,
    chunked: Literal[True],
    pagination_config: Optional[Dict[str, Any]] = ...,
    boto3_session: Optional[boto3.Session] = ...,
) -> Iterator[pd.DataFrame]: ...
@overload
def query(
    sql: str,
    chunked: bool,
    pagination_config: Optional[Dict[str, Any]] = ...,
    boto3_session: Optional[boto3.Session] = ...,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]: ...
