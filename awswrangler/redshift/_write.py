"""Amazon Redshift Write Module (PRIVATE)."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Literal, get_args

import boto3

import awswrangler.pandas as pd
from awswrangler import _databases as _db_utils
from awswrangler import _utils, exceptions, s3
from awswrangler._config import apply_configs

from ._connect import _validate_connection
from ._utils import (
    _add_new_table_columns,
    _create_table,
    _does_table_exist,
    _get_rsh_columns_types,
    _make_s3_auth_string,
    _upsert,
)

if TYPE_CHECKING:
    try:
        import redshift_connector
    except ImportError:
        pass
else:
    redshift_connector = _utils.import_optional_dependency("redshift_connector")

_logger: logging.Logger = logging.getLogger(__name__)

_ToSqlModeLiteral = Literal["append", "overwrite", "upsert"]
_ToSqlOverwriteModeLiteral = Literal["drop", "cascade", "truncate", "delete"]
_ToSqlDistStyleLiteral = Literal["AUTO", "EVEN", "ALL", "KEY"]
_ToSqlSortStyleLiteral = Literal["COMPOUND", "INTERLEAVED"]
_CopyFromFilesDataFormatLiteral = Literal["parquet", "orc", "csv"]


def _copy(
    cursor: "redshift_connector.Cursor",
    path: str,
    table: str,
    serialize_to_json: bool,
    data_format: _CopyFromFilesDataFormatLiteral = "parquet",
    iam_role: str | None = None,
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
    aws_session_token: str | None = None,
    boto3_session: boto3.Session | None = None,
    schema: str | None = None,
    manifest: bool | None = False,
    sql_copy_extra_params: list[str] | None = None,
    column_names: list[str] | None = None,
) -> None:
    if schema is None:
        table_name: str = f'"{table}"'
    else:
        table_name = f'"{schema}"."{table}"'

    if data_format not in ["parquet", "orc"] and serialize_to_json:
        raise exceptions.InvalidArgumentCombination(
            "You can only use SERIALIZETOJSON with data_format='parquet' or 'orc'."
        )

    auth_str: str = _make_s3_auth_string(
        iam_role=iam_role,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token,
        boto3_session=boto3_session,
    )
    ser_json_str: str = " SERIALIZETOJSON" if serialize_to_json else ""
    column_names_str: str = f"({','.join(column_names)})" if column_names else ""
    sql = (
        f"COPY {table_name} {column_names_str}\nFROM '{path}' {auth_str}\nFORMAT AS {data_format.upper()}{ser_json_str}"
    )

    if manifest:
        sql += "\nMANIFEST"
    if sql_copy_extra_params:
        for param in sql_copy_extra_params:
            sql += f"\n{param}"
    cursor.execute(sql)


@_utils.check_optional_dependency(redshift_connector, "redshift_connector")
@apply_configs
def to_sql(
    df: pd.DataFrame,
    con: "redshift_connector.Connection",
    table: str,
    schema: str,
    mode: _ToSqlModeLiteral = "append",
    overwrite_method: _ToSqlOverwriteModeLiteral = "drop",
    index: bool = False,
    dtype: dict[str, str] | None = None,
    diststyle: _ToSqlDistStyleLiteral = "AUTO",
    distkey: str | None = None,
    sortstyle: _ToSqlSortStyleLiteral = "COMPOUND",
    sortkey: list[str] | None = None,
    primary_keys: list[str] | None = None,
    varchar_lengths_default: int = 256,
    varchar_lengths: dict[str, int] | None = None,
    use_column_names: bool = False,
    lock: bool = False,
    chunksize: int = 200,
    commit_transaction: bool = True,
    precombine_key: str | None = None,
    add_new_columns: bool = False,
) -> None:
    """Write records stored in a DataFrame into Redshift.

    Note
    ----
    For large DataFrames (1K+ rows) consider the function **wr.redshift.copy()**.


    Parameters
    ----------
    df
        Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    con
        Use redshift_connector.connect() to use "
        "credentials directly or wr.redshift.connect() to fetch it from the Glue Catalog.
    table
        Table name
    schema
        Schema name
    mode
        Append, overwrite or upsert.
    overwrite_method
        Drop, cascade, truncate, or delete. Only applicable in overwrite mode.

        - "drop" - ``DROP ... RESTRICT`` - drops the table. Fails if there are any views that depend on it.
        - "cascade" - ``DROP ... CASCADE`` - drops the table, and all views that depend on it.
        - "truncate" - ``TRUNCATE ...`` - truncates the table, but immediately commits current transaction &
          starts a new one, hence the overwrite happens in two transactions and is not atomic.
        - "delete" - ``DELETE FROM ...`` - deletes all rows from the table. Slow relative to the other methods.

    index
        True to store the DataFrame index as a column in the table,
        otherwise False to ignore it.
    dtype
        Dictionary of columns names and Redshift types to be casted.
        Useful when you have columns with undetermined or mixed data types.
        (e.g. {'col name': 'VARCHAR(10)', 'col2 name': 'FLOAT'})
    diststyle
        Redshift distribution styles. Must be in ["AUTO", "EVEN", "ALL", "KEY"].
        https://docs.aws.amazon.com/redshift/latest/dg/t_Distributing_data.html
    distkey
        Specifies a column name or positional number for the distribution key.
    sortstyle
        Sorting can be "COMPOUND" or "INTERLEAVED".
        https://docs.aws.amazon.com/redshift/latest/dg/t_Sorting_data.html
    sortkey
        List of columns to be sorted.
    primary_keys
        Primary keys.
    varchar_lengths_default
        The size that will be set for all VARCHAR columns not specified with varchar_lengths.
    varchar_lengths
        Dict of VARCHAR length by columns. (e.g. {"col1": 10, "col5": 200}).
    use_column_names
        If set to True, will use the column names of the DataFrame for generating the INSERT SQL Query.
        E.g. If the DataFrame has two columns `col1` and `col3` and `use_column_names` is True, data will only be
        inserted into the database columns `col1` and `col3`.
    lock
        True to execute LOCK command inside the transaction to force serializable isolation.
    chunksize
        Number of rows which are inserted with each SQL query. Defaults to inserting 200 rows per query.
    commit_transaction
        Whether to commit the transaction. True by default.
    precombine_key
        When there is a primary_key match during upsert, this column will change the upsert method,
        comparing the values of the specified column from source and target, and keeping the
        larger of the two. Will only work when mode = upsert.
    add_new_columns
        If True, it automatically adds the new DataFrame columns into the target table.

    Examples
    --------
    Writing to Redshift using a Glue Catalog Connections

    >>> import awswrangler as wr
    >>> with wr.redshift.connect("MY_GLUE_CONNECTION") as con"
    ...     wr.redshift.to_sql(
    ...         df=df,
    ...         table="my_table",
    ...         schema="public",
    ...         con=con,
    ...     )

    """
    if df.empty is True:
        raise exceptions.EmptyDataFrame("DataFrame cannot be empty.")
    _validate_connection(con=con)
    autocommit_temp: bool = con.autocommit
    con.autocommit = False
    try:
        with con.cursor() as cursor:
            if add_new_columns and _does_table_exist(cursor=cursor, schema=schema, table=table):
                redshift_columns_types = _get_rsh_columns_types(
                    df=df,
                    path=None,
                    index=index,
                    dtype=dtype,
                    varchar_lengths_default=varchar_lengths_default,
                    varchar_lengths=varchar_lengths,
                )
                _add_new_table_columns(
                    cursor=cursor, schema=schema, table=table, redshift_columns_types=redshift_columns_types
                )

            created_table, created_schema = _create_table(
                df=df,
                path=None,
                con=con,
                cursor=cursor,
                table=table,
                schema=schema,
                mode=mode,
                overwrite_method=overwrite_method,
                index=index,
                dtype=dtype,
                diststyle=diststyle,
                sortstyle=sortstyle,
                distkey=distkey,
                sortkey=sortkey,
                primary_keys=primary_keys,
                varchar_lengths_default=varchar_lengths_default,
                varchar_lengths=varchar_lengths,
                lock=lock,
            )
            if index:
                df.reset_index(level=df.index.names, inplace=True)
            column_names = [f'"{column}"' for column in df.columns]
            column_placeholders: str = ", ".join(["%s"] * len(column_names))
            schema_str = f'"{created_schema}".' if created_schema else ""
            insertion_columns = ""
            if use_column_names:
                insertion_columns = f"({', '.join(column_names)})"
            placeholder_parameter_pair_generator = _db_utils.generate_placeholder_parameter_pairs(
                df=df, column_placeholders=column_placeholders, chunksize=chunksize
            )
            for placeholders, parameters in placeholder_parameter_pair_generator:
                sql: str = f'INSERT INTO {schema_str}"{created_table}" {insertion_columns} VALUES {placeholders}'
                _logger.debug("Executing insert query:\n%s", sql)
                cursor.executemany(sql, (parameters,))
            if table != created_table:  # upsert
                _upsert(
                    cursor=cursor,
                    schema=schema,
                    table=table,
                    temp_table=created_table,
                    primary_keys=primary_keys,
                    precombine_key=precombine_key,
                    column_names=column_names,
                )
            if commit_transaction:
                con.commit()
    except Exception as ex:
        con.rollback()
        _logger.error(ex)
        raise
    finally:
        con.autocommit = autocommit_temp


@_utils.check_optional_dependency(redshift_connector, "redshift_connector")
def copy_from_files(  # noqa: PLR0913
    path: str,
    con: "redshift_connector.Connection",
    table: str,
    schema: str,
    iam_role: str | None = None,
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
    aws_session_token: str | None = None,
    data_format: _CopyFromFilesDataFormatLiteral = "parquet",
    redshift_column_types: dict[str, str] | None = None,
    parquet_infer_sampling: float = 1.0,
    mode: _ToSqlModeLiteral = "append",
    overwrite_method: _ToSqlOverwriteModeLiteral = "drop",
    diststyle: _ToSqlDistStyleLiteral = "AUTO",
    distkey: str | None = None,
    sortstyle: _ToSqlSortStyleLiteral = "COMPOUND",
    sortkey: list[str] | None = None,
    primary_keys: list[str] | None = None,
    varchar_lengths_default: int = 256,
    varchar_lengths: dict[str, int] | None = None,
    serialize_to_json: bool = False,
    path_suffix: str | None = None,
    path_ignore_suffix: str | list[str] | None = None,
    use_threads: bool | int = True,
    lock: bool = False,
    commit_transaction: bool = True,
    manifest: bool | None = False,
    sql_copy_extra_params: list[str] | None = None,
    boto3_session: boto3.Session | None = None,
    s3_additional_kwargs: dict[str, str] | None = None,
    precombine_key: str | None = None,
    column_names: list[str] | None = None,
    add_new_columns: bool = False,
) -> None:
    """Load files from S3 to a Table on Amazon Redshift (Through COPY command).

    https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html

    Note
    ----
    If the table does not exist yet,
    it will be automatically created for you
    using the Parquet/ORC/CSV metadata to
    infer the columns data types.
    If the data is in the CSV format,
    the Redshift column types need to be
    specified manually using ``redshift_column_types``.

    Note
    ----
    In case of `use_threads=True` the number of threads
    that will be spawned will be gotten from os.cpu_count().

    Parameters
    ----------
    path
        S3 prefix (e.g. s3://bucket/prefix/)
    con
        Use redshift_connector.connect() to use "
        "credentials directly or wr.redshift.connect() to fetch it from the Glue Catalog.
    table
        Table name
    schema
        Schema name
    iam_role
        AWS IAM role with the related permissions.
    aws_access_key_id
        The access key for your AWS account.
    aws_secret_access_key
        The secret key for your AWS account.
    aws_session_token
        The session key for your AWS account. This is only needed when you are using temporary credentials.
    data_format
        Data format to be loaded.
        Supported values are Parquet, ORC, and CSV.
        Default is Parquet.
    redshift_column_types
        Dictionary with keys as column names and values as Redshift column types.
        Only used when ``data_format`` is CSV.

        e.g. ```{'col1': 'BIGINT', 'col2': 'VARCHAR(256)'}```
    parquet_infer_sampling
        Random sample ratio of files that will have the metadata inspected.
        Must be `0.0 < sampling <= 1.0`.
        The higher, the more accurate.
        The lower, the faster.
    mode
        Append, overwrite or upsert.
    overwrite_method
        Drop, cascade, truncate, or delete. Only applicable in overwrite mode.

        "drop" - ``DROP ... RESTRICT`` - drops the table. Fails if there are any views that depend on it.
        "cascade" - ``DROP ... CASCADE`` - drops the table, and all views that depend on it.
        "truncate" - ``TRUNCATE ...`` - truncates the table, but immediately commits current
        transaction & starts a new one, hence the overwrite happens in two transactions and is not atomic.
        "delete" - ``DELETE FROM ...`` - deletes all rows from the table. Slow relative to the other methods.
    diststyle
        Redshift distribution styles. Must be in ["AUTO", "EVEN", "ALL", "KEY"].
        https://docs.aws.amazon.com/redshift/latest/dg/t_Distributing_data.html
    distkey
        Specifies a column name or positional number for the distribution key.
    sortstyle
        Sorting can be "COMPOUND" or "INTERLEAVED".
        https://docs.aws.amazon.com/redshift/latest/dg/t_Sorting_data.html
    sortkey
        List of columns to be sorted.
    primary_keys
        Primary keys.
    varchar_lengths_default
        The size that will be set for all VARCHAR columns not specified with varchar_lengths.
    varchar_lengths
        Dict of VARCHAR length by columns. (e.g. {"col1": 10, "col5": 200}).
    serialize_to_json
        Should awswrangler add SERIALIZETOJSON parameter into the COPY command?
        SERIALIZETOJSON is necessary to load nested data
        https://docs.aws.amazon.com/redshift/latest/dg/ingest-super.html#copy_json
    path_suffix
        Suffix or List of suffixes to be scanned on s3 for the schema extraction
        (e.g. [".gz.parquet", ".snappy.parquet"]).
        Only has effect during the table creation.
        If None, will try to read all files. (default)
    path_ignore_suffix
        Suffix or List of suffixes for S3 keys to be ignored during the schema extraction.
        (e.g. [".csv", "_SUCCESS"]).
        Only has effect during the table creation.
        If None, will try to read all files. (default)
    use_threads
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
        If integer is provided, specified number is used.
    lock
        True to execute LOCK command inside the transaction to force serializable isolation.
    commit_transaction
        Whether to commit the transaction. True by default.
    manifest
        If set to true path argument accepts a S3 uri to a manifest file.
    sql_copy_extra_params
        Additional copy parameters to pass to the command. For example: ["STATUPDATE ON"]
    boto3_session
        The default boto3 session will be used if **boto3_session** is ``None``.
    s3_additional_kwargs
        Forwarded to botocore requests.
        e.g. s3_additional_kwargs={'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'}
    precombine_key
        When there is a primary_key match during upsert, this column will change the upsert method,
        comparing the values of the specified column from source and target, and keeping the
        larger of the two. Will only work when mode = upsert.
    column_names
        List of column names to map source data fields to the target columns.
    add_new_columns
        If True, it automatically adds the new DataFrame columns into the target table.

    Examples
    --------
    >>> import awswrangler as wr
    >>> with wr.redshift.connect("MY_GLUE_CONNECTION") as con:
    ...     wr.redshift.copy_from_files(
    ...         path="s3://bucket/my_parquet_files/",
    ...         con=con,
    ...         table="my_table",
    ...         schema="public",
    ...         iam_role="arn:aws:iam::XXX:role/XXX"
    ...     )

    """
    _logger.debug("Copying objects from S3 path: %s", path)

    data_format = data_format.lower()  # type: ignore[assignment]
    if data_format not in get_args(_CopyFromFilesDataFormatLiteral):
        raise exceptions.InvalidArgumentValue(f"The specified data_format {data_format} is not supported.")

    autocommit_temp: bool = con.autocommit
    con.autocommit = False
    try:
        with con.cursor() as cursor:
            if add_new_columns and _does_table_exist(cursor=cursor, schema=schema, table=table):
                redshift_columns_types = _get_rsh_columns_types(
                    df=None,
                    path=path,
                    index=False,
                    dtype=None,
                    varchar_lengths_default=varchar_lengths_default,
                    varchar_lengths=varchar_lengths,
                    parquet_infer_sampling=parquet_infer_sampling,
                    path_suffix=path_suffix,
                    path_ignore_suffix=path_ignore_suffix,
                    use_threads=use_threads,
                    boto3_session=boto3_session,
                    s3_additional_kwargs=s3_additional_kwargs,
                    data_format=data_format,
                    redshift_column_types=redshift_column_types,
                    manifest=manifest,
                )
                _add_new_table_columns(
                    cursor=cursor, schema=schema, table=table, redshift_columns_types=redshift_columns_types
                )
            created_table, created_schema = _create_table(
                df=None,
                path=path,
                data_format=data_format,
                parquet_infer_sampling=parquet_infer_sampling,
                path_suffix=path_suffix,
                path_ignore_suffix=path_ignore_suffix,
                con=con,
                cursor=cursor,
                table=table,
                schema=schema,
                mode=mode,
                overwrite_method=overwrite_method,
                redshift_column_types=redshift_column_types,
                diststyle=diststyle,
                sortstyle=sortstyle,
                distkey=distkey,
                sortkey=sortkey,
                primary_keys=primary_keys,
                varchar_lengths_default=varchar_lengths_default,
                varchar_lengths=varchar_lengths,
                index=False,
                dtype=None,
                manifest=manifest,
                use_threads=use_threads,
                boto3_session=boto3_session,
                s3_additional_kwargs=s3_additional_kwargs,
                lock=lock,
            )
            _copy(
                cursor=cursor,
                path=path,
                table=created_table,
                schema=created_schema,
                iam_role=iam_role,
                data_format=data_format,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
                boto3_session=boto3_session,
                serialize_to_json=serialize_to_json,
                sql_copy_extra_params=sql_copy_extra_params,
                manifest=manifest,
                column_names=column_names,
            )
            if table != created_table:  # upsert
                _upsert(
                    cursor=cursor,
                    schema=schema,
                    table=table,
                    temp_table=created_table,
                    primary_keys=primary_keys,
                    precombine_key=precombine_key,
                    column_names=column_names,
                )
            if commit_transaction:
                con.commit()
    except Exception as ex:
        con.rollback()
        _logger.error(ex)
        raise
    finally:
        con.autocommit = autocommit_temp


@_utils.validate_distributed_kwargs(
    unsupported_kwargs=["boto3_session", "s3_additional_kwargs"],
)
@_utils.check_optional_dependency(redshift_connector, "redshift_connector")
def copy(  # noqa: PLR0913
    df: pd.DataFrame,
    path: str,
    con: "redshift_connector.Connection",
    table: str,
    schema: str,
    iam_role: str | None = None,
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
    aws_session_token: str | None = None,
    index: bool = False,
    dtype: dict[str, str] | None = None,
    mode: _ToSqlModeLiteral = "append",
    overwrite_method: _ToSqlOverwriteModeLiteral = "drop",
    diststyle: _ToSqlDistStyleLiteral = "AUTO",
    distkey: str | None = None,
    sortstyle: _ToSqlSortStyleLiteral = "COMPOUND",
    sortkey: list[str] | None = None,
    primary_keys: list[str] | None = None,
    varchar_lengths_default: int = 256,
    varchar_lengths: dict[str, int] | None = None,
    serialize_to_json: bool = False,
    keep_files: bool = False,
    use_threads: bool | int = True,
    lock: bool = False,
    commit_transaction: bool = True,
    sql_copy_extra_params: list[str] | None = None,
    boto3_session: boto3.Session | None = None,
    s3_additional_kwargs: dict[str, str] | None = None,
    max_rows_by_file: int | None = 10_000_000,
    precombine_key: str | None = None,
    use_column_names: bool = False,
    add_new_columns: bool = False,
) -> None:
    """Load Pandas DataFrame as a Table on Amazon Redshift using parquet files on S3 as stage.

    This is a **HIGH** latency and **HIGH** throughput alternative to `wr.redshift.to_sql()` to load large
    DataFrames into Amazon Redshift through the ** SQL COPY command**.

    This strategy has more overhead and requires more IAM privileges
    than the regular `wr.redshift.to_sql()` function, so it is only recommended
    to inserting +1K rows at once.

    https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html

    Note
    ----
    If the table does not exist yet,
    it will be automatically created for you
    using the Parquet metadata to
    infer the columns data types.

    Note
    ----
    In case of `use_threads=True` the number of threads
    that will be spawned will be gotten from os.cpu_count().

    Parameters
    ----------
    df
        Pandas DataFrame.
    path
        S3 path to write stage files (e.g. s3://bucket_name/any_name/).
        Note: This path must be empty.
    con
        Use redshift_connector.connect() to use "
        "credentials directly or wr.redshift.connect() to fetch it from the Glue Catalog.
    table
        Table name
    schema
        Schema name
    iam_role
        AWS IAM role with the related permissions.
    aws_access_key_id
        The access key for your AWS account.
    aws_secret_access_key
        The secret key for your AWS account.
    aws_session_token
        The session key for your AWS account. This is only needed when you are using temporary credentials.
    index
        True to store the DataFrame index in file, otherwise False to ignore it.
    dtype
        Dictionary of columns names and Athena/Glue types to be casted.
        Useful when you have columns with undetermined or mixed data types.
        Only takes effect if dataset=True.
        (e.g. {'col name': 'bigint', 'col2 name': 'int'})
    mode
        Append, overwrite or upsert.
    overwrite_method
        Drop, cascade, truncate, or delete. Only applicable in overwrite mode.

        "drop" - ``DROP ... RESTRICT`` - drops the table. Fails if there are any views that depend on it.
        "cascade" - ``DROP ... CASCADE`` - drops the table, and all views that depend on it.
        "truncate" - ``TRUNCATE ...`` - truncates the table, but immediately commits current
        transaction & starts a new one, hence the overwrite happens in two transactions and is not atomic.
        "delete" - ``DELETE FROM ...`` - deletes all rows from the table. Slow relative to the other methods.
    diststyle
        Redshift distribution styles. Must be in ["AUTO", "EVEN", "ALL", "KEY"].
        https://docs.aws.amazon.com/redshift/latest/dg/t_Distributing_data.html
    distkey
        Specifies a column name or positional number for the distribution key.
    sortstyle
        Sorting can be "COMPOUND" or "INTERLEAVED".
        https://docs.aws.amazon.com/redshift/latest/dg/t_Sorting_data.html
    sortkey
        List of columns to be sorted.
    primary_keys
        Primary keys.
    varchar_lengths_default
        The size that will be set for all VARCHAR columns not specified with varchar_lengths.
    varchar_lengths
        Dict of VARCHAR length by columns. (e.g. {"col1": 10, "col5": 200}).
    keep_files
        Should keep stage files?
    use_threads
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
        If integer is provided, specified number is used.
    lock
        True to execute LOCK command inside the transaction to force serializable isolation.
    commit_transaction
        Whether to commit the transaction. True by default.
    sql_copy_extra_params
        Additional copy parameters to pass to the command. For example: ["STATUPDATE ON"]
    boto3_session
        The default boto3 session will be used if **boto3_session** is ``None``.
    s3_additional_kwargs
        Forwarded to botocore requests.
        e.g. s3_additional_kwargs={'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'}
    max_rows_by_file
        Max number of rows in each file.
        (e.g. 33554432, 268435456)
    precombine_key
        When there is a primary_key match during upsert, this column will change the upsert method,
        comparing the values of the specified column from source and target, and keeping the
        larger of the two. Will only work when mode = upsert.
    use_column_names
        If set to True, will use the column names of the DataFrame for generating the INSERT SQL Query.
        E.g. If the DataFrame has two columns `col1` and `col3` and `use_column_names` is True, data will only be
        inserted into the database columns `col1` and `col3`.
    add_new_columns
        If True, it automatically adds the new DataFrame columns into the target table.

    Examples
    --------
    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> with wr.redshift.connect("MY_GLUE_CONNECTION") as con:
    ...     wr.redshift.copy(
    ...         df=pd.DataFrame({'col': [1, 2, 3]}),
    ...         path="s3://bucket/my_parquet_files/",
    ...         con=con,
    ...         table="my_table",
    ...         schema="public",
    ...         iam_role="arn:aws:iam::XXX:role/XXX",
    ...     )

    """
    path = path[:-1] if path.endswith("*") else path
    path = path if path.endswith("/") else f"{path}/"
    column_names = [f'"{column}"' for column in df.columns] if use_column_names else []
    if s3.list_objects(path=path, boto3_session=boto3_session, s3_additional_kwargs=s3_additional_kwargs):
        raise exceptions.InvalidArgument(
            f"The received S3 path ({path}) is not empty. "
            "Please, provide a different path or use wr.s3.delete_objects() to clean up the current one."
        )
    try:
        s3.to_parquet(
            df=df,
            path=path,
            index=index,
            dataset=True,
            mode="append",
            dtype=dtype,
            use_threads=use_threads,
            boto3_session=boto3_session,
            s3_additional_kwargs=s3_additional_kwargs,
            max_rows_by_file=max_rows_by_file,
        )
        copy_from_files(
            path=path,
            con=con,
            table=table,
            schema=schema,
            iam_role=iam_role,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            mode=mode,
            overwrite_method=overwrite_method,
            diststyle=diststyle,
            distkey=distkey,
            sortstyle=sortstyle,
            sortkey=sortkey,
            primary_keys=primary_keys,
            varchar_lengths_default=varchar_lengths_default,
            varchar_lengths=varchar_lengths,
            serialize_to_json=serialize_to_json,
            use_threads=use_threads,
            lock=lock,
            commit_transaction=commit_transaction,
            boto3_session=boto3_session,
            s3_additional_kwargs=s3_additional_kwargs,
            sql_copy_extra_params=sql_copy_extra_params,
            precombine_key=precombine_key,
            column_names=column_names,
            add_new_columns=add_new_columns,
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
