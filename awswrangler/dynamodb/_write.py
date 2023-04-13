"""Amazon DynamoDB Write Module (PRIVATE)."""

import itertools
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Union

import boto3

import awswrangler.pandas as pd
from awswrangler import _utils
from awswrangler._config import apply_configs
from awswrangler._distributed import engine
from awswrangler._executor import _get_executor
from awswrangler.distributed.ray import ray_get

from ._utils import _validate_items, get_table

_logger: logging.Logger = logging.getLogger(__name__)


@apply_configs
def put_json(
    path: Union[str, Path],
    table_name: str,
    boto3_session: Optional[boto3.Session] = None,
    use_threads: Union[bool, int] = True,
) -> None:
    """Write all items from JSON file to a DynamoDB.

    The JSON file can either contain a single item which will be inserted in the DynamoDB or an array of items
    which all be inserted.

    Parameters
    ----------
    path : Union[str, Path]
        Path as str or Path object to the JSON file which contains the items.
    table_name : str
        Name of the Amazon DynamoDB table.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.
    use_threads : Union[bool, int]
        Used for Parallel Write requests. True (default) to enable concurrency, False to disable multiple threads.
        If enabled os.cpu_count() is used as the max number of threads.
        If integer is provided, specified number is used.

    Returns
    -------
    None
        None.

    Examples
    --------
    Writing contents of JSON file

    >>> import awswrangler as wr
    >>> wr.dynamodb.put_json(
    ...     path='items.json',
    ...     table_name='table'
    ... )
    """
    # Loading data from file
    with open(path, "r") as f:  # pylint: disable=W1514
        items = json.load(f)
    if isinstance(items, dict):
        items = [items]

    put_items(items=items, table_name=table_name, boto3_session=boto3_session, use_threads=use_threads)


@apply_configs
def put_csv(
    path: Union[str, Path],
    table_name: str,
    boto3_session: Optional[boto3.Session] = None,
    use_threads: Union[bool, int] = True,
    **pandas_kwargs: Any,
) -> None:
    """Write all items from a CSV file to a DynamoDB.

    Parameters
    ----------
    path : Union[str, Path]
        Path as str or Path object to the CSV file which contains the items.
    table_name : str
        Name of the Amazon DynamoDB table.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.
    use_threads : Union[bool, int]
        Used for Parallel Write requests. True (default) to enable concurrency, False to disable multiple threads.
        If enabled os.cpu_count() is used as the max number of threads.
        If integer is provided, specified number is used.
    pandas_kwargs :
        KEYWORD arguments forwarded to pandas.read_csv(). You can NOT pass `pandas_kwargs` explicit, just add valid
        Pandas arguments in the function call and awswrangler will accept it.
        e.g. wr.dynamodb.put_csv('items.csv', 'my_table', sep='|', na_values=['null', 'none'], skip_blank_lines=True)
        https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html

    Returns
    -------
    None
        None.

    Examples
    --------
    Writing contents of CSV file

    >>> import awswrangler as wr
    >>> wr.dynamodb.put_csv(
    ...     path='items.csv',
    ...     table_name='table'
    ... )

    Writing contents of CSV file using pandas_kwargs

    >>> import awswrangler as wr
    >>> wr.dynamodb.put_csv(
    ...     path='items.csv',
    ...     table_name='table',
    ...     sep='|',
    ...     na_values=['null', 'none']
    ... )
    """
    # Loading data from file
    df = pd.read_csv(path, **pandas_kwargs)

    put_df(df=df, table_name=table_name, boto3_session=boto3_session, use_threads=use_threads)


@engine.dispatch_on_engine
def _put_df(
    boto3_session: Optional[boto3.Session],
    df: pd.DataFrame,
    table_name: str,
) -> None:
    items: List[Mapping[str, Any]] = [v.dropna().to_dict() for _, v in df.iterrows()]

    put_items_func = engine.dispatch_func(_put_items, "python")
    put_items_func(items=items, table_name=table_name, boto3_session=boto3_session)


@apply_configs
@_utils.validate_distributed_kwargs(
    unsupported_kwargs=["boto3_session"],
)
def put_df(
    df: pd.DataFrame,
    table_name: str,
    boto3_session: Optional[boto3.Session] = None,
    use_threads: Union[bool, int] = True,
) -> None:
    """Write all items from a DataFrame to a DynamoDB.

    Parameters
    ----------
    df: pd.DataFrame
        Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    table_name: str
        Name of the Amazon DynamoDB table.
    use_threads: Union[bool, int]
        Used for Parallel Write requests. True (default) to enable concurrency, False to disable multiple threads.
        If enabled os.cpu_count() is used as the max number of threads.
        If integer is provided, specified number is used.
    boto3_session: boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.

    Returns
    -------
    None
        None.

    Examples
    --------
    Writing rows of DataFrame

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.dynamodb.put_df(
    ...     df=pd.DataFrame({'key': [1, 2, 3]}),
    ...     table_name='table'
    ... )
    """
    _logger.debug("Inserting data frame into DynamoDB table: %s", table_name)

    concurrency = _utils.ensure_worker_or_thread_count(use_threads=use_threads)
    executor = _get_executor(use_threads=use_threads, ray_parallelism=concurrency)

    dfs = _utils.split_pandas_frame(df, concurrency)

    ray_get(
        executor.map(
            _put_df,
            boto3_session,  # type: ignore[arg-type]
            dfs,
            itertools.repeat(table_name),
        )
    )


@engine.dispatch_on_engine
def _put_items(
    boto3_session: Optional[boto3.Session],
    items: Union[List[Dict[str, Any]], List[Mapping[str, Any]]],
    table_name: str,
) -> None:
    _logger.debug("Inserting %d items", len(items))

    dynamodb_table = get_table(table_name=table_name, boto3_session=boto3_session)
    _validate_items(items=items, dynamodb_table=dynamodb_table)
    with dynamodb_table.batch_writer() as writer:
        for item in items:
            writer.put_item(Item=item)  # type: ignore[arg-type]


@apply_configs
@_utils.validate_distributed_kwargs(
    unsupported_kwargs=["boto3_session"],
)
def put_items(
    items: Union[List[Dict[str, Any]], List[Mapping[str, Any]]],
    table_name: str,
    boto3_session: Optional[boto3.Session] = None,
    use_threads: Union[bool, int] = True,
) -> None:
    """Insert all items to the specified DynamoDB table.

    Parameters
    ----------
    items: Union[List[Dict[str, Any]], List[Mapping[str, Any]]]
        List which contains the items that will be inserted.
    table_name: str
        Name of the Amazon DynamoDB table.
    boto3_session: boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.
    use_threads: Union[bool, int]
        Used for Parallel Write requests. True (default) to enable concurrency, False to disable multiple threads.
        If enabled os.cpu_count() is used as the max number of threads.
        If integer is provided, specified number is used.

    Returns
    -------
    None
        None.

    Examples
    --------
    Writing items

    >>> import awswrangler as wr
    >>> wr.dynamodb.put_items(
    ...     items=[{'key': 1}, {'key': 2, 'value': 'Hello'}],
    ...     table_name='table'
    ... )
    """
    _logger.debug("Inserting items into DynamoDB table: %s", table_name)

    executor = _get_executor(use_threads=use_threads)
    batches = _utils.chunkify(  # type: ignore[misc]
        items,
        num_chunks=_utils.ensure_worker_or_thread_count(use_threads=use_threads),
    )

    ray_get(
        executor.map(
            _put_items,
            boto3_session,  # type: ignore[arg-type]
            batches,
            itertools.repeat(table_name),
        )
    )
