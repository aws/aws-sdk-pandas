"""Amazon DynamoDB Read Module (PRIVATE)."""

from __future__ import annotations

import itertools
import logging
import warnings
from functools import wraps
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    NamedTuple,
    Sequence,
    TypeVar,
    cast,
)

import boto3
import pyarrow as pa
from boto3.dynamodb.conditions import ConditionBase, ConditionExpressionBuilder
from boto3.dynamodb.types import Binary, TypeDeserializer, TypeSerializer
from botocore.exceptions import ClientError
from typing_extensions import Literal

import awswrangler.pandas as pd
from awswrangler import _data_types, _utils, exceptions
from awswrangler._distributed import engine
from awswrangler._executor import _BaseExecutor, _get_executor
from awswrangler.distributed.ray import ray_get
from awswrangler.dynamodb._utils import _deserialize_item, _serialize_item, execute_statement

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.client import DynamoDBClient
    from mypy_boto3_dynamodb.type_defs import TableAttributeValueTypeDef

_logger: logging.Logger = logging.getLogger(__name__)


_ItemsListType = List[Dict[str, "TableAttributeValueTypeDef"]]


def _read_chunked(iterator: Iterator[dict[str, "TableAttributeValueTypeDef"]]) -> Iterator[pd.DataFrame]:
    for item in iterator:
        yield pd.DataFrame(item)


def read_partiql_query(
    query: str,
    parameters: list[Any] | None = None,
    chunked: bool = False,
    boto3_session: boto3.Session | None = None,
) -> pd.DataFrame | Iterator[pd.DataFrame]:
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
    iterator: Iterator[dict[str, Any]] = execute_statement(  # type: ignore[assignment]
        query, parameters=parameters, boto3_session=boto3_session
    )
    if chunked:
        return _read_chunked(iterator=iterator)
    return pd.DataFrame([item for sublist in iterator for item in sublist])


def _get_invalid_kwarg(msg: str) -> str | None:
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
    arrow_kwargs: dict[str, Any],
) -> pd.DataFrame | _ItemsListType:
    return (
        _utils.table_refs_to_df(
            [
                _utils.list_to_arrow_table(
                    # Convert DynamoDB "Binary" type to native Python data type
                    mapping=[
                        {k: v.value if isinstance(v, Binary) else v for k, v in d.items()}  # type: ignore[attr-defined]
                        for d in items
                    ],
                    schema=arrow_kwargs.pop("schema", None),
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
    arrow_kwargs: dict[str, Any],
) -> Iterator[pd.DataFrame] | Iterator[_ItemsListType]:
    for items in items_iterator:
        yield _convert_items(items, as_dataframe, arrow_kwargs)


def _read_scan_chunked(
    dynamodb_client: "DynamoDBClient" | None,
    as_dataframe: bool,
    kwargs: dict[str, Any],
    schema: pa.Schema | None = None,
    segment: int | None = None,
) -> Iterator[pa.Table] | Iterator[_ItemsListType]:
    # SEE: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.ParallelScan
    client_dynamodb = dynamodb_client if dynamodb_client else _utils.client(service_name="dynamodb")

    deserializer = TypeDeserializer()
    next_token: str | None = "init_token"  # Dummy token
    total_items = 0

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
        total_items += len(items)
        yield _utils.list_to_arrow_table(mapping=items, schema=schema) if as_dataframe else items

        if ("Limit" in kwargs) and (total_items >= kwargs["Limit"]):
            break

        next_token = response.get("LastEvaluatedKey", None)
        if next_token:
            kwargs["ExclusiveStartKey"] = next_token


@engine.dispatch_on_engine
@_utils.retry(
    ex=ClientError,
    ex_code="ProvisionedThroughputExceededException",
)
def _read_scan(
    dynamodb_client: "DynamoDBClient" | None,
    as_dataframe: bool,
    kwargs: dict[str, Any],
    schema: pa.Schema | None,
    segment: int,
) -> pa.Table | _ItemsListType:
    items_iterator: Iterator[_ItemsListType] = _read_scan_chunked(dynamodb_client, False, kwargs, None, segment)

    items = list(itertools.chain.from_iterable(items_iterator))

    return _utils.list_to_arrow_table(mapping=items, schema=schema) if as_dataframe else items


def _read_query_chunked(table_name: str, dynamodb_client: "DynamoDBClient", **kwargs: Any) -> Iterator[_ItemsListType]:
    next_token: str | None = "init_token"  # Dummy token
    total_items = 0

    # Handle pagination
    while next_token:
        response = dynamodb_client.query(TableName=table_name, **kwargs)
        items = response.get("Items", [])
        total_items += len(items)
        yield [_deserialize_item(item) for item in items]

        if ("Limit" in kwargs) and (total_items >= kwargs["Limit"]):
            break

        next_token = response.get("LastEvaluatedKey", None)
        if next_token:
            kwargs["ExclusiveStartKey"] = next_token


@_handle_reserved_keyword_error
def _read_query(
    table_name: str, dynamodb_client: "DynamoDBClient", chunked: bool, **kwargs: Any
) -> _ItemsListType | Iterator[_ItemsListType]:
    items_iterator = _read_query_chunked(table_name, dynamodb_client, **kwargs)

    if chunked:
        return items_iterator
    else:
        return list(itertools.chain.from_iterable(items_iterator))


def _read_batch_items_chunked(
    table_name: str, dynamodb_client: "DynamoDBClient" | None, **kwargs: Any
) -> Iterator[_ItemsListType]:
    dynamodb_client = dynamodb_client if dynamodb_client else _utils.client("dynamodb")
    deserializer = TypeDeserializer()

    response = dynamodb_client.batch_get_item(RequestItems={table_name: kwargs})
    yield [_deserialize_item(d, deserializer) for d in response.get("Responses", {table_name: []}).get(table_name, [])]

    # SEE: handle possible unprocessed keys. As suggested in Boto3 docs,
    # this approach should involve exponential backoff, but this should be
    # already managed by AWS SDK itself, as stated
    # [here](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.Errors.html)
    while response["UnprocessedKeys"]:
        kwargs["Keys"] = response["UnprocessedKeys"][table_name]["Keys"]

        response = dynamodb_client.batch_get_item(RequestItems={table_name: kwargs})
        yield [
            _deserialize_item(d, deserializer) for d in response.get("Responses", {table_name: []}).get(table_name, [])
        ]


@_handle_reserved_keyword_error
def _read_batch_items(
    table_name: str, dynamodb_client: "DynamoDBClient" | None, chunked: bool, **kwargs: Any
) -> _ItemsListType | Iterator[_ItemsListType]:
    items_iterator = _read_batch_items_chunked(table_name, dynamodb_client, **kwargs)

    if chunked:
        return items_iterator
    else:
        return list(itertools.chain.from_iterable(items_iterator))


@_handle_reserved_keyword_error
def _read_item(
    table_name: str,
    dynamodb_client: "DynamoDBClient",
    chunked: bool = False,
    **kwargs: Any,
) -> _ItemsListType | Iterator[_ItemsListType]:
    item = dynamodb_client.get_item(TableName=table_name, **kwargs).get("Item", {})
    item_list: _ItemsListType = [_deserialize_item(item)]

    return [item_list] if chunked else item_list


def _read_items_scan(
    table_name: str,
    as_dataframe: bool,
    arrow_kwargs: dict[str, Any],
    use_threads: bool | int,
    dynamodb_client: "DynamoDBClient",
    chunked: bool,
    **kwargs: Any,
) -> pd.DataFrame | Iterator[pd.DataFrame] | _ItemsListType | Iterator[_ItemsListType]:
    kwargs["TableName"] = table_name
    schema = arrow_kwargs.pop("schema", None)

    if chunked:
        _logger.debug("Scanning DynamoDB table %s and returning results in an iterator", table_name)
        scan_iterator = _read_scan_chunked(dynamodb_client, as_dataframe, kwargs, schema)
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
        itertools.repeat(schema),
        range(total_segments),
    )

    if as_dataframe:
        return _utils.table_refs_to_df(items, arrow_kwargs)

    return list(itertools.chain(*ray_get(items)))


def _read_items(
    table_name: str,
    as_dataframe: bool,
    arrow_kwargs: dict[str, Any],
    use_threads: bool | int,
    chunked: bool,
    dynamodb_client: "DynamoDBClient",
    **kwargs: Any,
) -> pd.DataFrame | Iterator[pd.DataFrame] | _ItemsListType | Iterator[_ItemsListType]:
    # Extract 'Keys', 'IndexName' and 'Limit' from provided kwargs: if needed, will be reinserted later on
    keys = kwargs.pop("Keys", None)
    index = kwargs.pop("IndexName", None)
    limit = kwargs.pop("Limit", None)

    # Conditionally define optimal reading strategy
    use_get_item = (keys is not None) and (len(keys) == 1)
    use_batch_get_item = (keys is not None) and (len(keys) > 1)
    use_query = (keys is None) and ("KeyConditionExpression" in kwargs)

    # Single Item
    if use_get_item:
        kwargs["Key"] = keys[0]
        items = _read_item(table_name, dynamodb_client, chunked, **kwargs)

    # Batch of Items
    elif use_batch_get_item:
        kwargs["Keys"] = keys
        items = _read_batch_items(table_name, dynamodb_client, chunked, **kwargs)

    else:
        if limit:
            kwargs["Limit"] = limit
            _logger.debug("`max_items_evaluated` argument detected, setting use_threads to False")
            use_threads = False

        if index:
            kwargs["IndexName"] = index

        if use_query:
            # Query
            _logger.debug("Query DynamoDB table %s", table_name)
            items = _read_query(table_name, dynamodb_client, chunked, **kwargs)
        else:
            # Last resort use Scan
            warnings.warn(
                f"Attempting DynamoDB Scan operation with arguments:\n{kwargs}",
                UserWarning,
            )
            return _read_items_scan(
                table_name=table_name,
                as_dataframe=as_dataframe,
                arrow_kwargs=arrow_kwargs,
                use_threads=use_threads,
                dynamodb_client=dynamodb_client,
                chunked=chunked,
                **kwargs,
            )

    if chunked:
        return _convert_items_chunked(
            items_iterator=cast(Iterator[_ItemsListType], items), as_dataframe=as_dataframe, arrow_kwargs=arrow_kwargs
        )
    else:
        return _convert_items(items=cast(_ItemsListType, items), as_dataframe=as_dataframe, arrow_kwargs=arrow_kwargs)


class _ExpressionTuple(NamedTuple):
    condition_expression: str
    attribute_name_placeholders: dict[str, str]
    attribute_value_placeholders: dict[str, Any]


def _convert_condition_base_to_expression(
    key_condition_expression: ConditionBase, is_key_condition: bool, serializer: TypeSerializer
) -> dict[str, Any]:
    builder = ConditionExpressionBuilder()

    # Use different namespaces for key and filter conditions
    if is_key_condition:
        builder._name_placeholder = "kn"
        builder._value_placeholder = "kv"

    expression = builder.build_expression(key_condition_expression, is_key_condition=is_key_condition)

    return _ExpressionTuple(
        condition_expression=expression.condition_expression,
        attribute_name_placeholders=expression.attribute_name_placeholders,
        attribute_value_placeholders=_serialize_item(expression.attribute_value_placeholders, serializer=serializer),
    )


@_utils.validate_distributed_kwargs(
    unsupported_kwargs=["boto3_session", "dtype_backend"],
)
def read_items(  # noqa: PLR0912
    table_name: str,
    index_name: str | None = None,
    partition_values: Sequence[Any] | None = None,
    sort_values: Sequence[Any] | None = None,
    filter_expression: ConditionBase | str | None = None,
    key_condition_expression: ConditionBase | str | None = None,
    expression_attribute_names: dict[str, str] | None = None,
    expression_attribute_values: dict[str, Any] | None = None,
    consistent: bool = False,
    columns: Sequence[str] | None = None,
    allow_full_scan: bool = False,
    max_items_evaluated: int | None = None,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = "numpy_nullable",
    as_dataframe: bool = True,
    chunked: bool = False,
    use_threads: bool | int = True,
    boto3_session: boto3.Session | None = None,
    pyarrow_additional_kwargs: dict[str, Any] | None = None,
) -> pd.DataFrame | Iterator[pd.DataFrame] | _ItemsListType | Iterator[_ItemsListType]:
    """Read items from given DynamoDB table.

    This function aims to gracefully handle (some of) the complexity of read actions
    available in Boto3 towards a DynamoDB table, abstracting it away while providing
    a single, unified entry point.

    Under the hood, it wraps all the four available read actions: `get_item`, `batch_get_item`,
    `query` and `scan`.

    Warning
    -------
    To avoid a potentially costly Scan operation, please make sure to pass arguments such as
    `partition_values` or `max_items_evaluated`. Note that `filter_expression` is applied AFTER a Scan

    Note
    ----
    Number of Parallel Scan segments is based on the `use_threads` argument.
    A parallel scan with a large number of workers could consume all the provisioned throughput
    of the table or index.
    See: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.ParallelScan

    Note
    ----
    If `max_items_evaluated` is specified, then `use_threads=False` is enforced. This is because
    it's not possible to limit the number of items in a Query/Scan operation across threads.

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
        When set, `use_threads` is enforced to False.
    dtype_backend: str, optional
        Which dtype_backend to use, e.g. whether a DataFrame should have NumPy arrays,
        nullable dtypes are used for all dtypes that have a nullable implementation when
        “numpy_nullable” is set, pyarrow is used for all dtypes if “pyarrow” is set.

        The dtype_backends are still experimential. The "pyarrow" backend is only supported with Pandas 2.0 or above.
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
    Note that FilterExpression is applied AFTER a Scan operation

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
    arrow_kwargs = _data_types.pyarrow2pandas_defaults(
        use_threads=use_threads, kwargs=pyarrow_additional_kwargs, dtype_backend=dtype_backend
    )

    # Extract key schema
    dynamodb_client = _utils.client(service_name="dynamodb", session=boto3_session)
    serializer = TypeSerializer()
    table_key_schema = dynamodb_client.describe_table(TableName=table_name)["Table"]["KeySchema"]

    # Detect sort key, if any
    if len(table_key_schema) == 1:
        partition_key, sort_key = table_key_schema[0]["AttributeName"], None
    else:
        partition_key, sort_key = (
            next(filter(lambda x: x["KeyType"] == "HASH", table_key_schema))["AttributeName"],
            next(filter(lambda x: x["KeyType"] == "RANGE", table_key_schema))["AttributeName"],
        )

    # Build kwargs shared by read methods
    kwargs: dict[str, Any] = {"ConsistentRead": consistent}
    if partition_values:
        if sort_key is None:
            keys = [{partition_key: serializer.serialize(pv)} for pv in partition_values]
        else:
            if not sort_values:
                raise exceptions.InvalidArgumentType(
                    f"Kwarg sort_values must be specified: table {table_name} has {sort_key} as sort key."
                )
            if len(sort_values) != len(partition_values):
                raise exceptions.InvalidArgumentCombination("Partition and sort values must have the same length.")
            keys = [
                {partition_key: serializer.serialize(pv), sort_key: serializer.serialize(sv)}
                for pv, sv in zip(partition_values, sort_values)
            ]
        kwargs["Keys"] = keys
    if index_name:
        kwargs["IndexName"] = index_name

    if key_condition_expression:
        if isinstance(key_condition_expression, str):
            kwargs["KeyConditionExpression"] = key_condition_expression
        else:
            expression_tuple = _convert_condition_base_to_expression(
                key_condition_expression, is_key_condition=True, serializer=serializer
            )
            kwargs["KeyConditionExpression"] = expression_tuple.condition_expression

            kwargs["ExpressionAttributeNames"] = {
                **kwargs.get("ExpressionAttributeNames", {}),
                **expression_tuple.attribute_name_placeholders,
            }
            kwargs["ExpressionAttributeValues"] = {
                **kwargs.get("ExpressionAttributeValues", {}),
                **expression_tuple.attribute_value_placeholders,
            }

    if filter_expression:
        if isinstance(filter_expression, str):
            kwargs["FilterExpression"] = filter_expression
        else:
            expression_tuple = _convert_condition_base_to_expression(
                filter_expression, is_key_condition=False, serializer=serializer
            )
            kwargs["FilterExpression"] = expression_tuple.condition_expression

            kwargs["ExpressionAttributeNames"] = {
                **kwargs.get("ExpressionAttributeNames", {}),
                **expression_tuple.attribute_name_placeholders,
            }
            kwargs["ExpressionAttributeValues"] = {
                **kwargs.get("ExpressionAttributeValues", {}),
                **expression_tuple.attribute_value_placeholders,
            }

    if columns:
        kwargs["ProjectionExpression"] = ", ".join(columns)
    if expression_attribute_names:
        kwargs["ExpressionAttributeNames"] = {
            **kwargs.get("ExpressionAttributeNames", {}),
            **expression_attribute_names,
        }
    if expression_attribute_values:
        kwargs["ExpressionAttributeValues"] = {
            **kwargs.get("ExpressionAttributeValues", {}),
            **_serialize_item(expression_attribute_values, serializer),
        }
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
            chunked=chunked,
            dynamodb_client=dynamodb_client,
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
