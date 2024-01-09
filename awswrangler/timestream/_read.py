"""Amazon Timestream Read Module."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any, Iterator, Literal, cast

import boto3
import pandas as pd
from botocore.config import Config

from awswrangler import _utils, exceptions, s3
from awswrangler._config import apply_configs

_logger: logging.Logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from mypy_boto3_timestream_query.type_defs import PaginatorConfigTypeDef, QueryResponseTypeDef, RowTypeDef


def _cast_value(value: str, dtype: str) -> Any:  # noqa: PLR0911
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


def _process_row(schema: list[dict[str, str]], row: "RowTypeDef") -> list[Any]:
    row_processed: list[Any] = []
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
    rows: list[list[Any]], schema: list[dict[str, str]], df_metadata: dict[str, str] | None = None
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


def _process_schema(page: "QueryResponseTypeDef") -> list[dict[str, str]]:
    schema: list[dict[str, str]] = []
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
    pagination_config: "PaginatorConfigTypeDef" | None,
    boto3_session: boto3.Session | None = None,
) -> Iterator[pd.DataFrame]:
    client = _utils.client(
        service_name="timestream-query",
        session=boto3_session,
        botocore_config=Config(read_timeout=60, retries={"max_attempts": 10}),
    )
    paginator = client.get_paginator("query")
    rows: list[list[Any]] = []
    schema: list[dict[str, str]] = []
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


def _get_column_names_from_metadata(unload_path: str, boto3_session: boto3.Session | None = None) -> list[str]:
    client_s3 = _utils.client(service_name="s3", session=boto3_session)
    metadata_path = s3.list_objects(path=unload_path, suffix="_metadata.json", boto3_session=boto3_session)[0]
    bucket, key = _utils.parse_path(metadata_path)
    metadata_content = json.loads(client_s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8"))
    columns = [column["Name"] for column in metadata_content["ColumnInfo"]]
    _logger.debug("Read %d columns from metadata file in: %s", len(columns), metadata_path)
    return columns


def query(
    sql: str,
    chunked: bool = False,
    pagination_config: dict[str, Any] | None = None,
    boto3_session: boto3.Session | None = None,
) -> pd.DataFrame | Iterator[pd.DataFrame]:
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


@_utils.validate_distributed_kwargs(
    unsupported_kwargs=["boto3_session", "s3_additional_kwargs"],
)
@apply_configs
def unload(
    sql: str,
    path: str,
    unload_format: Literal["CSV", "PARQUET"] | None = None,
    compression: Literal["GZIP", "NONE"] | None = None,
    partition_cols: list[str] | None = None,
    encryption: Literal["SSE_KMS", "SSE_S3"] | None = None,
    kms_key_id: str | None = None,
    field_delimiter: str | None = ",",
    escaped_by: str | None = "\\",
    chunked: bool | int = False,
    keep_files: bool = False,
    use_threads: bool | int = True,
    boto3_session: boto3.Session | None = None,
    s3_additional_kwargs: dict[str, str] | None = None,
    pyarrow_additional_kwargs: dict[str, Any] | None = None,
) -> pd.DataFrame | Iterator[pd.DataFrame]:
    """
    Unload query results to Amazon S3 and read the results as Pandas Data Frame.

    https://docs.aws.amazon.com/timestream/latest/developerguide/export-unload.html

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
    chunked : Union[int, bool]
        If passed will split the data in a Iterable of DataFrames (Memory friendly).
        If `True` awswrangler iterates on the data by files in the most efficient way without guarantee of chunksize.
        If an `INTEGER` is passed awswrangler will iterate on the data by number of rows equal the received INTEGER.
    keep_files : bool
        Should keep stage files?
    use_threads : bool, int
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
        If integer is provided, specified number is used.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session is used if None
    s3_additional_kwargs : Dict[str, str], optional
        Forward to botocore requests.
    pyarrow_additional_kwargs : Dict[str, Any], optional
        Forwarded to `to_pandas` method converting from PyArrow tables to Pandas DataFrame.
        Valid values include "split_blocks", "self_destruct", "ignore_metadata".
        e.g. pyarrow_additional_kwargs={'split_blocks': True}.

    Returns
    -------
    Union[pandas.DataFrame, Iterator[pandas.DataFrame]]
        Result as Pandas DataFrame(s).

    Examples
    --------
    Unload and read as Parquet (default).

    >>> import awswrangler as wr
    >>> df = wr.timestream.unload(
    ...     sql="SELECT time, measure, dimension FROM database.mytable",
    ...     path="s3://bucket/extracted_parquet_files/",
    ... )

    Unload and read partitioned Parquet. Note: partition columns must be at the end of the table.

    >>> import awswrangler as wr
    >>> df = wr.timestream.unload(
    ...     sql="SELECT time, measure, dim1, dim2 FROM database.mytable",
    ...     path="s3://bucket/extracted_parquet_files/",
    ...     partition_cols=["dim2"],
    ... )

    Unload and read as CSV.

    >>> import awswrangler as wr
    >>> df = wr.timestream.unload(
    ...     sql="SELECT time, measure, dimension FROM database.mytable",
    ...     path="s3://bucket/extracted_parquet_files/",
    ...     unload_format="CSV",
    ... )

    """
    path = path if path.endswith("/") else f"{path}/"

    if unload_format not in [None, "CSV", "PARQUET"]:
        raise exceptions.InvalidArgumentValue("<unload_format> argument must be 'CSV' or 'PARQUET'")

    unload_to_files(
        sql=sql,
        path=path,
        unload_format=unload_format,
        compression=compression,
        partition_cols=partition_cols,
        encryption=encryption,
        kms_key_id=kms_key_id,
        field_delimiter=field_delimiter,
        escaped_by=escaped_by,
        boto3_session=boto3_session,
    )
    results_path = f"{path}results/"
    try:
        if unload_format == "CSV":
            column_names: list[str] = _get_column_names_from_metadata(path, boto3_session)
            return s3.read_csv(
                path=results_path,
                header=None,
                names=[column for column in column_names if column not in set(partition_cols)]
                if partition_cols is not None
                else column_names,
                dataset=True,
                use_threads=use_threads,
                boto3_session=boto3_session,
                s3_additional_kwargs=s3_additional_kwargs,
            )
        else:
            return s3.read_parquet(
                path=results_path,
                chunked=chunked,
                dataset=True,
                use_threads=use_threads,
                boto3_session=boto3_session,
                s3_additional_kwargs=s3_additional_kwargs,
                pyarrow_additional_kwargs=pyarrow_additional_kwargs,
            )
    finally:
        if keep_files is False:
            _logger.debug("Deleting objects in S3 path: %s", path)
            s3.delete_objects(
                path=path,
                use_threads=use_threads,
                boto3_session=boto3_session,
                s3_additional_kwargs=s3_additional_kwargs,
            )


@_utils.validate_distributed_kwargs(
    unsupported_kwargs=["boto3_session"],
)
@apply_configs
def unload_to_files(
    sql: str,
    path: str,
    unload_format: Literal["CSV", "PARQUET"] | None = None,
    compression: Literal["GZIP", "NONE"] | None = None,
    partition_cols: list[str] | None = None,
    encryption: Literal["SSE_KMS", "SSE_S3"] | None = None,
    kms_key_id: str | None = None,
    field_delimiter: str | None = ",",
    escaped_by: str | None = "\\",
    boto3_session: boto3.Session | None = None,
) -> None:
    """
    Unload query results to Amazon S3.

    https://docs.aws.amazon.com/timestream/latest/developerguide/export-unload.html

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

    Examples
    --------
    Unload and read as Parquet (default).

    >>> import awswrangler as wr
    >>> wr.timestream.unload_to_files(
    ...     sql="SELECT time, measure, dimension FROM database.mytable",
    ...     path="s3://bucket/extracted_parquet_files/",
    ... )

    Unload and read partitioned Parquet. Note: partition columns must be at the end of the table.

    >>> import awswrangler as wr
    >>> wr.timestream.unload_to_files(
    ...     sql="SELECT time, measure, dim1, dim2 FROM database.mytable",
    ...     path="s3://bucket/extracted_parquet_files/",
    ...     partition_cols=["dim2"],
    ... )

    Unload and read as CSV.

    >>> import awswrangler as wr
    >>> wr.timestream.unload_to_files(
    ...     sql="SELECT time, measure, dimension FROM database.mytable",
    ...     path="s3://bucket/extracted_parquet_files/",
    ...     unload_format="CSV",
    ... )

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
