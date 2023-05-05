"""Amazon DynamoDB Read Module (PRIVATE)."""

import itertools
import logging
from functools import wraps
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Sequence,
    TypeVar,
    Union,
    cast,
)

import boto3
import pyarrow as pa
from boto3.dynamodb.conditions import ConditionBase
from boto3.dynamodb.types import Binary
from botocore.exceptions import ClientError

import awswrangler.pandas as pd
from awswrangler import _data_types, _utils, exceptions
from awswrangler._distributed import engine
from awswrangler._executor import _BaseExecutor, _get_executor
from awswrangler.distributed.ray import ray_get
from awswrangler.dynamodb._utils import _serialize_kwargs, execute_statement, get_table

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.client import DynamoDBClient

_logger: logging.Logger = logging.getLogger(__name__)


_ItemsListType = List[Dict[str, Any]]


def _read_chunked(iterator: Iterator[Dict[str, Any]]) -> Iterator[pd.DataFrame]:
    for item in iterator:
        yield pd.DataFrame(item)


def read_partiql_query(
    query: str,
    parameters: Optional[List[Any]] = None,
    chunked: bool = False,
    boto3_session: Optional[boto3.Session] = None,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Read data from a DynamoDB table via a PartiQL query.

    Parameters
    ----------
    query : str
        The PartiQL statement.
    parameters : Optional[List[Any]]
        The list of PartiQL parameters. These are applied to the statement in the order they are listed.
    chunked : bool
        If `True` an iterable of DataFrames is returned. False by default.
    boto3_session : Optional[boto3.Session]
        Boto3 Session. If None, the default boto3 Session is used.

    Returns
    -------
    Union[pd.DataFrame, Iterator[pd.DataFrame]]
        Result as Pandas DataFrame.

    Examples
    --------
    Select all contents from a table

    >>> import awswrangler as wr
    >>> wr.dynamodb.read_partiql_query(
    ...     query="SELECT * FROM my_table WHERE title=? AND year=?",
    ...     parameters=[title, year],
    ... )

    Select specific columns from a table

    >>> wr.dynamodb.read_partiql_query(
    ...     query="SELECT id FROM table"
    ... )
    """
    _logger.debug("Reading results for PartiQL query:  '%s'", query)
    iterator: Iterator[Dict[str, Any]] = execute_statement(  # type: ignore[assignment]
        query, parameters=parameters, boto3_session=boto3_session
    )
    if chunked:
        return _read_chunked(iterator=iterator)
    return pd.DataFrame([item for sublist in iterator for item in sublist])


def _get_invalid_kwarg(msg: str) -> Optional[str]:
    """Detect which keyword argument contains reserved keywords based on given error message.

    Parameters
    ----------
    msg : str
        Botocore client error message.

    Returns
    -------
    str, optional
        Detected invalid keyword argument if any, None otherwise.
    """
    for kwarg in ("ProjectionExpression", "KeyConditionExpression", "FilterExpression"):
        if msg.startswith(f"Invalid {kwarg}: Attribute name is a reserved keyword; reserved keyword: "):
            return kwarg
    return None


# SEE: https://stackoverflow.com/a/72295070
# CustomCallable = TypeVar("CustomCallable", bound=Callable[[Any], Union[_ItemsListType, Iterator[_ItemsListType]]])
CustomCallable = TypeVar("CustomCallable", bound=Callable[..., Any])


def _handle_reserved_keyword_error(func: CustomCallable) -> CustomCallable:
    """Handle automatic replacement of DynamoDB reserved keywords.

    For reserved keywords reference:
    https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ReservedWords.html.
    """

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except ClientError as e:
            error_code, error_message = (e.response["Error"]["Code"], e.response["Error"]["Message"])
            # Check caught error to verify its message
            kwarg = _get_invalid_kwarg(error_message)
            if (error_code == "ValidationException") and kwarg:
                reserved_keyword = error_message.split("keyword: ")[-1]
                sanitized_keyword = f"#{reserved_keyword}"
                kwargs[kwarg] = kwargs[kwarg].replace(reserved_keyword, sanitized_keyword)
                kwargs["ExpressionAttributeNames"] = {
                    **kwargs.get("ExpressionAttributeNames", {}),
                    sanitized_keyword: reserved_keyword,
                }
                # SEE: recursive approach guarantees that each reserved keyword will be properly replaced,
                # even if it will require as many calls as the reserved keywords involved (not so efficient...)
                return wrapper(*args, **kwargs)
            # Otherwise raise it
            raise e

    # SEE: https://github.com/python/mypy/issues/3157#issue-221120895
    return cast(CustomCallable, wrapper)


def _convert_items(
    items: _ItemsListType,
    as_dataframe: bool,
    arrow_kwargs: Dict[str, Any],
) -> Union[pd.DataFrame, _ItemsListType]:
    return (
        _utils.table_refs_to_df(
            [
                _utils.list_to_arrow_table(
                    # Convert DynamoDB "Binary" type to native Python data type
                    mapping=[
                        {k: v.value if isinstance(v, Binary) else v for k, v in d.items()}  # type: ignore[attr-defined]
                        for d in items
                    ]
                )
            ],
            arrow_kwargs,
        )
        if as_dataframe
        else items
    )


def _convert_items_chunked(
    items_iterator: Iterator[_ItemsListType],
    as_dataframe: bool,
    arrow_kwargs: Dict[str, Any],
) -> Union[Iterator[pd.DataFrame], Iterator[_ItemsListType]]:
    for items in items_iterator:
        yield _convert_items(items, as_dataframe, arrow_kwargs)


def _read_scan_chunked(
    dynamodb_client: Optional["DynamoDBClient"],
    as_dataframe: bool,
    kwargs: Dict[str, Any],
    segment: Optional[int] = None,
) -> Union[Iterator[pa.Table], Iterator[_ItemsListType]]:
    # SEE: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.ParallelScan
    client_dynamodb = dynamodb_client if dynamodb_client else _utils.client(service_name="dynamodb")

    deserializer = boto3.dynamodb.types.TypeDeserializer()
    next_token = "init_token"  # Dummy token

    kwargs = dict(kwargs)
    if segment is not None:
        kwargs["Segment"] = segment

    while next_token:
        response = _handle_reserved_keyword_error(client_dynamodb.scan)(**kwargs)
        # Unlike a resource, the DynamoDB client returns serialized results, so they must be deserialized
        # Additionally, the DynamoDB "Binary" type is converted to a native Python data type
        # SEE: https://boto3.amazonaws.com/v1/documentation/api/latest/_modules/boto3/dynamodb/types.html
        items = [
            {k: v["B"] if list(v.keys())[0] == "B" else deserializer.deserialize(v) for k, v in d.items()}
            for d in response.get("Items", [])
        ]
        yield _utils.list_to_arrow_table(mapping=items) if as_dataframe else items

        next_token = response.get("LastEvaluatedKey", None)  # type: ignore[assignment]
        if next_token:
            kwargs["ExclusiveStartKey"] = next_token


@engine.dispatch_on_engine
@_utils.retry(
    ex=ClientError,
    ex_code="ProvisionedThroughputExceededException",
)
def _read_scan(
    dynamodb_client: Optional["DynamoDBClient"],
    as_dataframe: bool,
    kwargs: Dict[str, Any],
    segment: int,
) -> Union[pa.Table, _ItemsListType]:
    items_iterator: Iterator[_ItemsListType] = _read_scan_chunked(dynamodb_client, False, kwargs, segment)

    items = list(itertools.chain.from_iterable(items_iterator))

    return _utils.list_to_arrow_table(mapping=items) if as_dataframe else items


def _read_query_chunked(
    table_name: str, boto3_session: Optional[boto3.Session] = None, **kwargs: Any
) -> Iterator[_ItemsListType]:
    table = get_table(table_name=table_name, boto3_session=boto3_session)
    response = table.query(**kwargs)
    yield response.get("Items", [])

    # Handle pagination
    while "LastEvaluatedKey" in response:
        kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]
        response = table.query(**kwargs)
        yield response.get("Items", [])


@_handle_reserved_keyword_error
def _read_query(
    table_name: str, chunked: bool, boto3_session: Optional[boto3.Session] = None, **kwargs: Any
) -> Union[_ItemsListType, Iterator[_ItemsListType]]:
    items_iterator = _read_query_chunked(table_name, boto3_session, **kwargs)

    if chunked:
        return items_iterator
    else:
        return list(itertools.chain.from_iterable(items_iterator))


def _read_batch_items_chunked(
    table_name: str, boto3_session: Optional[boto3.Session] = None, **kwargs: Any
) -> Iterator[_ItemsListType]:
    resource = _utils.resource(service_name="dynamodb", session=boto3_session)

    response = resource.batch_get_item(RequestItems={table_name: kwargs})  # type: ignore[dict-item]
    yield response.get("Responses", {table_name: []}).get(table_name, [])  # type: ignore[arg-type]

    # SEE: handle possible unprocessed keys. As suggested in Boto3 docs,
    # this approach should involve exponential backoff, but this should be
    # already managed by AWS SDK itself, as stated
    # [here](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.Errors.html)
    while response["UnprocessedKeys"]:
        kwargs["Keys"] = response["UnprocessedKeys"][table_name]["Keys"]

        response = resource.batch_get_item(RequestItems={table_name: kwargs})  # type: ignore[dict-item]
        yield response.get("Responses", {table_name: []}).get(table_name, [])  # type: ignore[arg-type]


@_handle_reserved_keyword_error
def _read_batch_items(
    table_name: str, chunked: bool, boto3_session: Optional[boto3.Session] = None, **kwargs: Any
) -> Union[_ItemsListType, Iterator[_ItemsListType]]:
    items_iterator = _read_batch_items_chunked(table_name, boto3_session, **kwargs)

    if chunked:
        return items_iterator
    else:
        return list(itertools.chain.from_iterable(items_iterator))


@_handle_reserved_keyword_error
def _read_item(
    table_name: str, chunked: bool = False, boto3_session: Optional[boto3.Session] = None, **kwargs: Any
) -> Union[_ItemsListType, Iterator[_ItemsListType]]:
    table = get_table(table_name=table_name, boto3_session=boto3_session)
    item_list: _ItemsListType = [table.get_item(**kwargs).get("Item", {})]

    return [item_list] if chunked else item_list


def _read_items_scan(
    table_name: str,
    as_dataframe: bool,
    arrow_kwargs: Dict[str, Any],
    use_threads: Union[bool, int],
    chunked: bool,
    boto3_session: Optional[boto3.Session] = None,
    **kwargs: Any,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame], _ItemsListType, Iterator[_ItemsListType]]:
    dynamodb_client = _utils.client(service_name="dynamodb", session=boto3_session)

    kwargs = _serialize_kwargs(kwargs)
    kwargs["TableName"] = table_name

    if chunked:
        _logger.debug("Scanning DynamoDB table %s and returning results in an iterator", table_name)
        scan_iterator = _read_scan_chunked(dynamodb_client, as_dataframe, kwargs)
        if as_dataframe:
            return (_utils.table_refs_to_df([items], arrow_kwargs) for items in scan_iterator)

        return scan_iterator

    # Use Parallel Scan
    executor: _BaseExecutor = _get_executor(use_threads=use_threads)
    total_segments = _utils.ensure_worker_or_thread_count(use_threads=use_threads)
    kwargs["TotalSegments"] = total_segments

    _logger.debug("Scanning DynamoDB table %s with %d segments", table_name, total_segments)

    items = executor.map(
        _read_scan,
        dynamodb_client,
        itertools.repeat(as_dataframe),
        itertools.repeat(kwargs),
        range(total_segments),
    )

    if as_dataframe:
        return _utils.table_refs_to_df(items, arrow_kwargs)

    return list(itertools.chain(*ray_get(items)))


def _read_items(
    table_name: str,
    as_dataframe: bool,
    arrow_kwargs: Dict[str, Any],
    use_threads: Union[bool, int],
    chunked: bool,
    boto3_session: Optional[boto3.Session] = None,
    **kwargs: Any,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame], _ItemsListType, Iterator[_ItemsListType]]:
    # Extract 'Keys' and 'IndexName' from provided kwargs: if needed, will be reinserted later on
    keys = kwargs.pop("Keys", None)
    index = kwargs.pop("IndexName", None)

    # Conditionally define optimal reading strategy
    use_get_item = (keys is not None) and (len(keys) == 1)
    use_batch_get_item = (keys is not None) and (len(keys) > 1)
    use_query = (keys is None) and ("KeyConditionExpression" in kwargs)

    # Single Item
    if use_get_item:
        kwargs["Key"] = keys[0]
        items = _read_item(table_name, chunked, boto3_session, **kwargs)

    # Batch of Items
    elif use_batch_get_item:
        kwargs["Keys"] = keys
        items = _read_batch_items(table_name, chunked, boto3_session, **kwargs)

    else:
        if index:
            kwargs["IndexName"] = index

        if use_query:
            # Query
            _logger.debug("Query DynamoDB table %s", table_name)
            items = _read_query(table_name, chunked, boto3_session, **kwargs)
        else:
            # Last resort use Scan
            return _read_items_scan(
                table_name=table_name,
                as_dataframe=as_dataframe,
                arrow_kwargs=arrow_kwargs,
                use_threads=use_threads,
                chunked=chunked,
                boto3_session=boto3_session,
                **kwargs,
            )

    if chunked:
        return _convert_items_chunked(
            items_iterator=cast(Iterator[_ItemsListType], items), as_dataframe=as_dataframe, arrow_kwargs=arrow_kwargs
        )
    else:
        return _convert_items(items=cast(_ItemsListType, items), as_dataframe=as_dataframe, arrow_kwargs=arrow_kwargs)


@_utils.validate_distributed_kwargs(
    unsupported_kwargs=["boto3_session"],
)
def read_items(  # pylint: disable=too-many-branches
    table_name: str,
    index_name: Optional[str] = None,
    partition_values: Optional[Sequence[Any]] = None,
    sort_values: Optional[Sequence[Any]] = None,
    filter_expression: Optional[Union[ConditionBase, str]] = None,
    key_condition_expression: Optional[Union[ConditionBase, str]] = None,
    expression_attribute_names: Optional[Dict[str, str]] = None,
    expression_attribute_values: Optional[Dict[str, Any]] = None,
    consistent: bool = False,
    columns: Optional[Sequence[str]] = None,
    allow_full_scan: bool = False,
    max_items_evaluated: Optional[int] = None,
    as_dataframe: bool = True,
    chunked: bool = False,
    use_threads: Union[bool, int] = True,
    boto3_session: Optional[boto3.Session] = None,
    pyarrow_additional_kwargs: Optional[Dict[str, Any]] = None,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame], _ItemsListType, Iterator[_ItemsListType]]:
    """Read items from given DynamoDB table.

    This function aims to gracefully handle (some of) the complexity of read actions
    available in Boto3 towards a DynamoDB table, abstracting it away while providing
    a single, unified entry point.

    Under the hood, it wraps all the four available read actions: `get_item`, `batch_get_item`,
    `query` and `scan`.

    Note
    ----
    Number of Parallel Scan segments is based on the `use_threads` argument.
    A parallel scan with a large number of workers could consume all the provisioned throughput
    of the table or index.
    See: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.ParallelScan

    Parameters
    ----------
    table_name : str
        DynamoDB table name.
    index_name : str, optional
        Name of the secondary global or local index on the table. Defaults to None.
    partition_values : Sequence[Any], optional
        Partition key values to retrieve. Defaults to None.
    sort_values : Sequence[Any], optional
        Sort key values to retrieve. Defaults to None.
    filter_expression : Union[ConditionBase, str], optional
        Filter expression as string or combinations of boto3.dynamodb.conditions.Attr conditions. Defaults to None.
    key_condition_expression : Union[ConditionBase, str], optional
        Key condition expression as string or combinations of boto3.dynamodb.conditions.Key conditions.
        Defaults to None.
    expression_attribute_names : Mapping[str, str], optional
        Mapping of placeholder and target attributes. Defaults to None.
    expression_attribute_values : Mapping[str, Any], optional
        Mapping of placeholder and target values. Defaults to None.
    consistent : bool
        If True, ensure that the performed read operation is strongly consistent, otherwise eventually consistent.
        Defaults to False.
    columns : Sequence[str], optional
        Attributes to retain in the returned items. Defaults to None (all attributes).
    allow_full_scan : bool
        If True, allow full table scan without any filtering. Defaults to False.
    max_items_evaluated : int, optional
        Limit the number of items evaluated in case of query or scan operations. Defaults to None (all matching items).
    as_dataframe : bool
        If True, return items as pd.DataFrame, otherwise as list/dict. Defaults to True.
    chunked : bool
        If `True` an iterable of DataFrames/lists is returned. False by default.
    use_threads : Union[bool, int]
        Used for Parallel Scan requests. True (default) to enable concurrency, False to disable multiple threads.
        If enabled os.cpu_count() is used as the max number of threads.
        If integer is provided, specified number is used.
    boto3_session : boto3.Session, optional
        Boto3 Session. Defaults to None (the default boto3 Session will be used).
    pyarrow_additional_kwargs : Dict[str, Any], optional
        Forwarded to `to_pandas` method converting from PyArrow tables to Pandas DataFrame.
        Valid values include "split_blocks", "self_destruct", "ignore_metadata".
        e.g. pyarrow_additional_kwargs={'split_blocks': True}.

    Raises
    ------
    exceptions.InvalidArgumentType
        When the specified table has also a sort key but only the partition values are specified.
    exceptions.InvalidArgumentCombination
        When both partition and sort values sequences are specified but they have different lengths,
        or when provided parameters are not enough informative to proceed with a read operation.

    Returns
    -------
    pd.DataFrame | list[dict[str, Any]] | Iterable[pd.DataFrame] | Iterable[list[dict[str, Any]]]
        A Data frame containing the retrieved items, or a dictionary of returned items.
        Alternatively, the return type can be an iterable of either type when `chunked=True`.

    Examples
    --------
    Reading 5 random items from a table

    >>> import awswrangler as wr
    >>> df = wr.dynamodb.read_items(table_name='my-table', max_items_evaluated=5)

    Strongly-consistent reading of a given partition value from a table

    >>> import awswrangler as wr
    >>> df = wr.dynamodb.read_items(table_name='my-table', partition_values=['my-value'], consistent=True)

    Reading items pairwise-identified by partition and sort values, from a table with a composite primary key

    >>> import awswrangler as wr
    >>> df = wr.dynamodb.read_items(
    ...     table_name='my-table',
    ...     partition_values=['pv_1', 'pv_2'],
    ...     sort_values=['sv_1', 'sv_2']
    ... )

    Reading items while retaining only specified attributes, automatically handling possible collision
    with DynamoDB reserved keywords

    >>> import awswrangler as wr
    >>> df = wr.dynamodb.read_items(
    ...     table_name='my-table',
    ...     partition_values=['my-value'],
    ...     columns=['connection', 'other_col'] # connection is a reserved keyword, managed under the hood!
    ... )

    Reading all items from a table explicitly allowing full scan

    >>> import awswrangler as wr
    >>> df = wr.dynamodb.read_items(table_name='my-table', allow_full_scan=True)

    Reading items matching a KeyConditionExpression expressed with boto3.dynamodb.conditions.Key

    >>> import awswrangler as wr
    >>> from boto3.dynamodb.conditions import Key
    >>> df = wr.dynamodb.read_items(
    ...     table_name='my-table',
    ...     key_condition_expression=(Key('key_1').eq('val_1') & Key('key_2').eq('val_2'))
    ... )

    Same as above, but with KeyConditionExpression as string

    >>> import awswrangler as wr
    >>> df = wr.dynamodb.read_items(
    ...     table_name='my-table',
    ...     key_condition_expression='key_1 = :v1 and key_2 = :v2',
    ...     expression_attribute_values={':v1': 'val_1', ':v2': 'val_2'},
    ... )

    Reading items matching a FilterExpression expressed with boto3.dynamodb.conditions.Attr

    >>> import awswrangler as wr
    >>> from boto3.dynamodb.conditions import Attr
    >>> df = wr.dynamodb.read_items(
    ...     table_name='my-table',
    ...     filter_expression=Attr('my_attr').eq('this-value')
    ... )

    Same as above, but with FilterExpression as string

    >>> import awswrangler as wr
    >>> df = wr.dynamodb.read_items(
    ...     table_name='my-table',
    ...     filter_expression='my_attr = :v',
    ...     expression_attribute_values={':v': 'this-value'}
    ... )

    Reading items involving an attribute which collides with DynamoDB reserved keywords

    >>> import awswrangler as wr
    >>> df = wr.dynamodb.read_items(
    ...     table_name='my-table',
    ...     filter_expression='#operator = :v',
    ...     expression_attribute_names={'#operator': 'operator'},
    ...     expression_attribute_values={':v': 'this-value'}
    ... )

    """
    arrow_kwargs = _data_types.pyarrow2pandas_defaults(use_threads=use_threads, kwargs=pyarrow_additional_kwargs)

    # Extract key schema
    table_key_schema = get_table(table_name=table_name, boto3_session=boto3_session).key_schema

    # Detect sort key, if any
    if len(table_key_schema) == 1:
        partition_key, sort_key = table_key_schema[0]["AttributeName"], None
    else:
        partition_key, sort_key = (
            next(filter(lambda x: x["KeyType"] == "HASH", table_key_schema))["AttributeName"],
            next(filter(lambda x: x["KeyType"] == "RANGE", table_key_schema))["AttributeName"],
        )

    # Build kwargs shared by read methods
    kwargs: Dict[str, Any] = {"ConsistentRead": consistent}
    if partition_values:
        if sort_key is None:
            keys = [{partition_key: pv} for pv in partition_values]
        else:
            if not sort_values:
                raise exceptions.InvalidArgumentType(
                    f"Kwarg sort_values must be specified: table {table_name} has {sort_key} as sort key."
                )
            if len(sort_values) != len(partition_values):
                raise exceptions.InvalidArgumentCombination("Partition and sort values must have the same length.")
            keys = [{partition_key: pv, sort_key: sv} for pv, sv in zip(partition_values, sort_values)]
        kwargs["Keys"] = keys
    if index_name:
        kwargs["IndexName"] = index_name
    if key_condition_expression:
        kwargs["KeyConditionExpression"] = key_condition_expression
    if filter_expression:
        kwargs["FilterExpression"] = filter_expression
    if columns:
        kwargs["ProjectionExpression"] = ", ".join(columns)
    if expression_attribute_names:
        kwargs["ExpressionAttributeNames"] = expression_attribute_names
    if expression_attribute_values:
        kwargs["ExpressionAttributeValues"] = expression_attribute_values
    if max_items_evaluated:
        kwargs["Limit"] = max_items_evaluated

    _logger.debug("DynamoDB scan/query kwargs: %s", kwargs)
    # If kwargs are sufficiently informative, proceed with actual read op
    if any((partition_values, key_condition_expression, filter_expression, allow_full_scan, max_items_evaluated)):
        return _read_items(
            table_name=table_name,
            as_dataframe=as_dataframe,
            arrow_kwargs=arrow_kwargs,
            use_threads=use_threads,
            boto3_session=boto3_session,
            chunked=chunked,
            **kwargs,
        )
    # Raise otherwise
    _args = (
        "partition_values",
        "key_condition_expression",
        "filter_expression",
        "allow_full_scan",
        "max_items_evaluated",
    )
    raise exceptions.InvalidArgumentCombination(f"Please provide at least one of these arguments: {', '.join(_args)}.")
