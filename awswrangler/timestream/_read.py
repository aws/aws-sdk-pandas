"""Amazon Timestream Read Module."""

import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Literal, Optional, Union, cast

import boto3
import pandas as pd
from botocore.config import Config

from awswrangler import _utils
from awswrangler._config import apply_configs

_logger: logging.Logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from mypy_boto3_timestream_query.type_defs import PaginatorConfigTypeDef, QueryResponseTypeDef, RowTypeDef


def _cast_value(value: str, dtype: str) -> Any:  # pylint: disable=too-many-branches,too-many-return-statements
    if dtype == "VARCHAR":
        return value
    if dtype in ("INTEGER", "BIGINT"):
        return int(value)
    if dtype == "DOUBLE":
        return float(value)
    if dtype == "BOOLEAN":
        return value.lower() == "true"
    if dtype == "TIMESTAMP":
        return datetime.strptime(value[:-3], "%Y-%m-%d %H:%M:%S.%f")
    if dtype == "DATE":
        return datetime.strptime(value, "%Y-%m-%d").date()
    if dtype == "TIME":
        return datetime.strptime(value[:-3], "%H:%M:%S.%f").time()
    if dtype == "ARRAY":
        return str(value)
    raise ValueError(f"Not supported Amazon Timestream type: {dtype}")


def _process_row(schema: List[Dict[str, str]], row: "RowTypeDef") -> List[Any]:
    row_processed: List[Any] = []
    for col_schema, col in zip(schema, row["Data"]):
        if col.get("NullValue", False):
            row_processed.append(None)
        elif "ScalarValue" in col:
            row_processed.append(_cast_value(value=col["ScalarValue"], dtype=col_schema["type"]))
        elif "ArrayValue" in col:
            row_processed.append(_cast_value(value=col["ArrayValue"], dtype="ARRAY"))  # type: ignore[arg-type]
        else:
            raise ValueError(
                f"Query with non ScalarType/ArrayColumnInfo/NullValue for column {col_schema['name']}. "
                f"Expected {col_schema['type']} instead of {col}"
            )
    return row_processed


def _rows_to_df(
    rows: List[List[Any]], schema: List[Dict[str, str]], df_metadata: Optional[Dict[str, str]] = None
) -> pd.DataFrame:
    df = pd.DataFrame(data=rows, columns=[c["name"] for c in schema])
    if df_metadata:
        try:
            df.attrs = df_metadata
        except AttributeError as ex:
            # Modin does not support attribute assignment
            _logger.error(ex)
    for col in schema:
        if col["type"] == "VARCHAR":
            df[col["name"]] = df[col["name"]].astype("string")
    return df


def _process_schema(page: "QueryResponseTypeDef") -> List[Dict[str, str]]:
    schema: List[Dict[str, str]] = []
    for col in page["ColumnInfo"]:
        if "ScalarType" in col["Type"]:
            schema.append({"name": col["Name"], "type": col["Type"]["ScalarType"]})
        elif "ArrayColumnInfo" in col["Type"]:
            schema.append({"name": col["Name"], "type": col["Type"]["ArrayColumnInfo"]})
        else:
            raise ValueError(f"Query with non ScalarType or ArrayColumnInfo for column {col['Name']}: {col['Type']}")
    return schema


def _paginate_query(
    sql: str,
    chunked: bool,
    pagination_config: Optional["PaginatorConfigTypeDef"],
    boto3_session: Optional[boto3.Session] = None,
) -> Iterator[pd.DataFrame]:
    client = _utils.client(
        service_name="timestream-query",
        session=boto3_session,
        botocore_config=Config(read_timeout=60, retries={"max_attempts": 10}),
    )
    paginator = client.get_paginator("query")
    rows: List[List[Any]] = []
    schema: List[Dict[str, str]] = []
    page_iterator = paginator.paginate(QueryString=sql, PaginationConfig=pagination_config or {})
    for page in page_iterator:
        if not schema:
            schema = _process_schema(page=page)
            _logger.debug("schema: %s", schema)
        for row in page["Rows"]:
            rows.append(_process_row(schema=schema, row=row))
        if len(rows) > 0:
            df_metadata = {}
            if chunked:
                if "NextToken" in page:
                    df_metadata["NextToken"] = page["NextToken"]
                df_metadata["QueryId"] = page["QueryId"]

            yield _rows_to_df(rows, schema, df_metadata)
        rows = []


def query(
    sql: str,
    chunked: bool = False,
    pagination_config: Optional[Dict[str, Any]] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Run a query and retrieve the result as a Pandas DataFrame.

    Parameters
    ----------
    sql: str
        SQL query.
    chunked: bool
        If True returns DataFrame iterator, and a single DataFrame otherwise. False by default.
    pagination_config: Dict[str, Any], optional
        Pagination configuration dictionary of a form {'MaxItems': 10, 'PageSize': 10, 'StartingToken': '...'}
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.

    Returns
    -------
    Union[pd.DataFrame, Iterator[pd.DataFrame]]
        Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html

    Examples
    --------
    Run a query and return the result as a Pandas DataFrame or an iterable.

    >>> import awswrangler as wr
    >>> df = wr.timestream.query('SELECT * FROM "sampleDB"."sampleTable" ORDER BY time DESC LIMIT 10')

    """
    result_iterator = _paginate_query(sql, chunked, cast("PaginatorConfigTypeDef", pagination_config), boto3_session)
    if chunked:
        return result_iterator

    # Prepending an empty DataFrame ensures returning an empty DataFrame if result_iterator is empty
    results = list(result_iterator)
    if len(results) > 0:
        # Modin's concat() can not concatenate empty data frames
        return pd.concat(results, ignore_index=True)
    return pd.DataFrame()


@apply_configs
def unload_to_files(
    sql: str,
    path: str,
    unload_format: Optional[Literal["CSV", "PARQUET"]] = None,
    compression: Optional[Literal["GZIP", "NONE"]] = None,
    partition_cols: Optional[List[str]] = None,
    encryption: Optional[Literal["SSE_KMS", "SSE_S3"]] = None,
    kms_key_id: Optional[str] = None,
    field_delimiter: Optional[str] = ",",
    escaped_by: Optional[str] = "\\",
    boto3_session: Optional[boto3.Session] = None,
) -> None:
    """
    Unload query results to Amazon S3.

    Parameters
    ----------
    sql : str
        SQL query
    path : str
        S3 path to write stage files (e.g. s3://bucket_name/any_name/)
    unload_format : str, optional
        Format of the unloaded S3 objects from the query.
        Valid values: "CSV", "PARQUET". Case sensitive. Defaults to "PARQUET"
    compression : str, optional
        Compression of the unloaded S3 objects from the query.
        Valid values: "GZIP", "NONE". Defaults to "GZIP"
    partition_cols : List[str], optional
        Specifies the partition keys for the unload operation
    encryption : str, optional
        Encryption of the unloaded S3 objects from the query.
        Valid values: "SSE_KMS", "SSE_S3". Defaults to "SSE_S3"
    kms_key_id : str, optional
        Specifies the key ID for an AWS Key Management Service (AWS KMS) key to be
        used to encrypt data files on Amazon S3
    field_delimiter : str, optional
        A single ASCII character that is used to separate fields in the output file,
        such as pipe character (|), a comma (,), or tab (/t). Only used with CSV format
    escaped_by : str, optional
        The character that should be treated as an escape character in the data file
        written to S3 bucket. Only used with CSV format
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session is used if None

    Returns
    -------
    None

    """
    timestream_client = _utils.client(service_name="timestream-query", session=boto3_session)

    partitioned_by_str: str = (
        f"""partitioned_by = ARRAY [{','.join([f"'{col}'" for col in partition_cols])}],\n"""
        if partition_cols is not None
        else ""
    )
    kms_key_id_str: str = f"kms_key = '{kms_key_id}',\n" if kms_key_id is not None else ""
    field_delimiter_str: str = f"field_delimiter = '{field_delimiter}',\n" if unload_format == "CSV" else ""
    escaped_by_str: str = f"escaped_by = '{escaped_by}',\n" if unload_format == "CSV" else ""

    sql = (
        f"UNLOAD ({sql})\n"
        f"TO '{path}'\n"
        f"WITH (\n"
        f"{partitioned_by_str}"
        f"format='{unload_format or 'PARQUET'}',\n"
        f"compression='{compression or 'GZIP'}',\n"
        f"{field_delimiter_str}"
        f"{escaped_by_str}"
        f"{kms_key_id_str}"
        f"encryption='{encryption or 'SSE_S3'}'\n"
        f")"
    )

    timestream_client.query(QueryString=sql)
