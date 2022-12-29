"""Amazon DynamoDB Read Module (PRIVATE)."""

import logging
import re
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, TypeVar, Union, cast

import boto3
import pandas as pd
from boto3.dynamodb.conditions import ConditionBase
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError

from awswrangler import _utils, exceptions
from awswrangler.dynamodb._utils import get_table

_logger: logging.Logger = logging.getLogger(__name__)


def _get_terms_groups(terms: List[str]) -> Tuple[List[str], List[str], List[str]]:
    """Determine to which group of a PartiQL query each term belongs, e.g. it describes a column, table or filter."""
    is_select_term = False
    is_from_term = False
    is_where_term = False
    select_terms, from_terms, where_terms = [], [], []
    for term in terms:
        if term.upper() == "SELECT":
            is_select_term = True
            continue
        if term.upper() == "FROM":
            is_select_term = False
            is_from_term = True
            continue
        if term.upper() == "WHERE":
            is_from_term = False
            is_where_term = True
            continue
        if is_select_term:
            select_terms.append(term)
        if is_from_term:
            from_terms.append(term)
        if is_where_term:
            where_terms.append(term)
    return select_terms, from_terms, where_terms


def _get_scan_response(
    table_name: str, select_terms: List[str], boto3_session: Optional[boto3.Session] = None
) -> List[Dict[str, Any]]:
    """Perform a scan to the Dynamo DB table and returns the data fetched."""
    client_dynamodb = _utils.client(service_name="dynamodb", session=boto3_session)
    scan_config: Dict[str, Any] = {"TableName": table_name}
    if len(select_terms) > 1 or select_terms[0] != "*":
        scan_config["AttributesToGet"] = select_terms
    # get all responses even if pagination is necessary
    response = client_dynamodb.scan(**scan_config)
    data: List[Dict[str, Any]] = response["Items"]
    while "LastEvaluatedKey" in response:
        scan_config["ExclusiveStartKey"] = response["LastEvaluatedKey"]
        response = client_dynamodb.scan(**scan_config)
        data.extend(response["Items"])
    return data


def _get_items(query: str, boto3_session: Optional[boto3.Session] = None) -> List[Dict[str, Any]]:
    # clean input query from possible excessive whitespace
    query = re.sub(" +", " ", query).strip()
    # generate terms list from query
    terms = re.split(" |,", query)
    if terms[0].upper() != "SELECT":
        raise ValueError("The PartiQL query does not start with a 'SELECT'.")
    select_terms, from_terms, _ = _get_terms_groups(terms)
    if len(from_terms) > 1:
        raise ValueError("The PartiQL query contains multiple tables but only one needed.")
    if len(from_terms) == 0:
        raise ValueError("The PartiQL query contains no tables.")
    table_name = from_terms[0]
    return _get_scan_response(table_name=table_name, select_terms=select_terms, boto3_session=boto3_session)


def _deserialize_value(value: Any) -> Any:
    if not pd.isna(value):
        return TypeDeserializer().deserialize(value)
    return value


def _deserialize_data(df: pd.DataFrame, columns: pd.Index) -> pd.DataFrame:
    if df.shape[0] > 0:
        for column in columns:
            df[column] = df[column].apply(_deserialize_value)
    return df


def _parse_dynamodb_items(
    items: List[Dict[str, Any]],
    dtype: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
    df = pd.DataFrame(items)
    df = _deserialize_data(df, df.columns)
    return df.astype(dtype=dtype) if dtype else df


def read_partiql_query(
    query: str,
    dtype: Optional[Dict[str, str]] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> pd.DataFrame:
    """Read data from a DynamoDB table via a PartiQL query.

    Parameters
    ----------
    query : str
        The PartiQL query that will be executed.
    dtype : Dict, optional
        The data types of the DataFrame columns.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.

    Returns
    -------
    pd.DataFrame
        Result as Pandas DataFrame.

    Examples
    --------
    Select all contents from a table

    >>> import awswrangler as wr
    >>> wr.dynamodb.read_partiql_query(
    ...     query='SELECT * FROM table'
    ... )

    Select specific columns from a table

    >>> import awswrangler as wr
    >>> wr.dynamodb.read_partiql_query(
    ...     query='SELECT key FROM table'
    ... )

    Select all contents with dtype set

    >>> import awswrangler as wr
    >>> wr.dynamodb.read_partiql_query(
    ...     query='SELECT * FROM table',
    ...     dtype={'key': int}
    ... )
    """
    _logger.debug("Reading results for PartiQL query:  %s", query)
    items = _get_items(query=query, boto3_session=boto3_session)
    return _parse_dynamodb_items(items=items, dtype=dtype)


def _get_invalid_kwarg(msg: str) -> Optional[str]:
    """Detect which kwarg contains reserved keywords based on given error message.

    Parameters
    ----------
    msg : str
        Botocore client error message.

    Returns
    -------
    str, optional
        Detected invalid kwarg if any, None otherwise.
    """
    for kwarg in ("ProjectionExpression", "KeyConditionExpression", "FilterExpression"):
        if msg.startswith(f"Invalid {kwarg}: Attribute name is a reserved keyword; reserved keyword: "):
            return kwarg
    return None


# SEE: https://stackoverflow.com/a/72295070
CustomCallable = TypeVar("CustomCallable", bound=Callable[[Any], Sequence[Dict[str, Any]]])


def _handle_reserved_keyword_error(func: CustomCallable) -> CustomCallable:
    """Handle automatic replacement of DynamoDB reserved keywords.

    For reserved keywords reference:
    https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ReservedWords.html.
    """

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Sequence[Dict[str, Any]]:
        try:
            return func(*args, **kwargs)
        except ClientError as e:
            error_code, error_message = (e.response["Error"]["Code"], e.response["Error"]["Message"])
            # Check catched error to verify its message
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


@_handle_reserved_keyword_error
def _read_items(
    table_name: str, boto3_session: Optional[boto3.Session] = None, **kwargs: Any
) -> Sequence[Dict[str, Any]]:
    """Read items from given DynamoDB table.

    This function set the optimal reading strategy based on the received kwargs.

    Parameters
    ----------
    table_name : str
        DynamoDB table name.
    boto3_session : boto3.Session, optional
        Boto3 Session. Defaults to None (the default boto3 Session will be used).

    Returns
    -------
    Sequence[Mapping[str, Any]]
        Retrieved items.
    """
    # Get DynamoDB resource and Table instance
    resource = _utils.resource(service_name="dynamodb", session=boto3_session)
    table = get_table(table_name=table_name, boto3_session=boto3_session)

    # Extract 'Keys' from provided kwargs: if needed, will be reinserted later on
    keys = kwargs.pop("Keys", None)

    # Conditionally define optimal reading strategy
    use_get_item = (keys is not None) and (len(keys) == 1)
    use_batch_get_item = (keys is not None) and (len(keys) > 1)
    use_query = (keys is None) and ("KeyConditionExpression" in kwargs)
    use_scan = (keys is None) and ("KeyConditionExpression" not in kwargs)

    # Read items
    if use_get_item:
        kwargs["Key"] = keys[0]
        items = [table.get_item(**kwargs).get("Item", {})]
    elif use_batch_get_item:
        kwargs["Keys"] = keys
        response = resource.batch_get_item(RequestItems={table_name: kwargs})
        items = response.get("Responses", {table_name: []}).get(table_name, [])
        # SEE: handle possible unprocessed keys. As suggested in Boto3 docs,
        # this approach should involve exponential backoff, but this should be
        # already managed by AWS SDK itself, as stated
        # [here](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.Errors.html)
        while response["UnprocessedKeys"]:
            kwargs["Keys"] = response["UnprocessedKeys"][table_name]["Keys"]
            response = resource.batch_get_item(RequestItems={table_name: kwargs})
            items.extend(response.get("Responses", {table_name: []}).get(table_name, []))
    elif use_query or use_scan:
        _read_method = table.query if use_query else table.scan
        response = _read_method(**kwargs)
        items = response.get("Items", [])

        # Handle pagination
        while "LastEvaluatedKey" in response:
            kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]
            response = _read_method(**kwargs)
            items.extend(response.get("Items", []))

    return items


def read_items(
    table_name: str,
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
    boto3_session: Optional[boto3.Session] = None,
) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
    """Read items from given DynamoDB table.

    This function aims to gracefully handle (some of) the complexity of read actions
    available in Boto3 towards a DynamoDB table, abstracting it away while providing
    a single, unified entrypoint.

    Under the hood, it wraps all the four available read actions: get_item, batch_get_item,
    query and scan.

    Parameters
    ----------
    table_name : str
        DynamoDB table name.
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
    boto3_session : boto3.Session, optional
        Boto3 Session. Defaults to None (the default boto3 Session will be used).

    Raises
    ------
    exceptions.InvalidArgumentType
        When the specified table has also a sort key but only the partition values are specified.
    exceptions.InvalidArgumentCombination
        When both partition and sort values sequences are specified but they have different lengths,
        or when provided parameters are not enough informative to proceed with a read operation.

    Returns
    -------
    Union[pd.DataFrame, List[Mapping[str, Any]]]
        A Data frame containing the retrieved items, or a dictionary of returned items.

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

    _logger.debug("kwargs: %s", kwargs)
    # If kwargs are sufficiently informative, proceed with actual read op
    if any((partition_values, key_condition_expression, filter_expression, allow_full_scan, max_items_evaluated)):
        items = _read_items(table_name, boto3_session, **kwargs)
    # Raise otherwise
    else:
        _args = (
            "partition_values",
            "key_condition_expression",
            "filter_expression",
            "allow_full_scan",
            "max_items_evaluated",
        )
        raise exceptions.InvalidArgumentCombination(
            f"Please provide at least one of these arguments: {', '.join(_args)}."
        )

    # Enforce DataFrame type if requested
    return pd.DataFrame(items) if as_dataframe else items
