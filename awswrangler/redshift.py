"""Amazon Redshift Module."""
# pylint: disable=too-many-lines

import json
import logging
import uuid
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

import boto3
import botocore
import pandas as pd
import pyarrow as pa
import redshift_connector

from awswrangler import _data_types
from awswrangler import _databases as _db_utils
from awswrangler import _utils, exceptions, s3
from awswrangler._config import apply_configs

_logger: logging.Logger = logging.getLogger(__name__)

_RS_DISTSTYLES: List[str] = ["AUTO", "EVEN", "ALL", "KEY"]
_RS_SORTSTYLES: List[str] = ["COMPOUND", "INTERLEAVED"]


def _validate_connection(con: redshift_connector.Connection) -> None:
    if not isinstance(con, redshift_connector.Connection):
        raise exceptions.InvalidConnection(
            "Invalid 'conn' argument, please pass a "
            "redshift_connector.Connection object. Use redshift_connector.connect() to use "
            "credentials directly or wr.redshift.connect() to fetch it from the Glue Catalog."
        )


def _begin_transaction(cursor: redshift_connector.Cursor) -> None:
    sql = "BEGIN TRANSACTION"
    _logger.debug("Begin transaction query:\n%s", sql)
    cursor.execute(sql)


def _drop_table(cursor: redshift_connector.Cursor, schema: Optional[str], table: str, cascade: bool = False) -> None:
    schema_str = f'"{schema}".' if schema else ""
    cascade_str = " CASCADE" if cascade else ""
    sql = f'DROP TABLE IF EXISTS {schema_str}"{table}"' f"{cascade_str}"
    _logger.debug("Drop table query:\n%s", sql)
    cursor.execute(sql)


def _truncate_table(cursor: redshift_connector.Cursor, schema: Optional[str], table: str) -> None:
    schema_str = f'"{schema}".' if schema else ""
    sql = f'TRUNCATE TABLE {schema_str}"{table}"'
    _logger.debug("Truncate table query:\n%s", sql)
    cursor.execute(sql)


def _delete_all(cursor: redshift_connector.Cursor, schema: Optional[str], table: str) -> None:
    schema_str = f'"{schema}".' if schema else ""
    sql = f'DELETE FROM {schema_str}"{table}"'
    _logger.debug("Delete query:\n%s", sql)
    cursor.execute(sql)


def _get_primary_keys(cursor: redshift_connector.Cursor, schema: str, table: str) -> List[str]:
    cursor.execute(f"SELECT indexdef FROM pg_indexes WHERE schemaname = '{schema}' AND tablename = '{table}'")
    result: str = cursor.fetchall()[0][0]
    rfields: List[str] = result.split("(")[1].strip(")").split(",")
    fields: List[str] = [field.strip().strip('"') for field in rfields]
    return fields


def _does_table_exist(cursor: redshift_connector.Cursor, schema: Optional[str], table: str) -> bool:
    schema_str = f"TABLE_SCHEMA = '{schema}' AND" if schema else ""
    cursor.execute(
        f"SELECT true WHERE EXISTS ("
        f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE "
        f"{schema_str} TABLE_NAME = '{table}'"
        f");"
    )
    return len(cursor.fetchall()) > 0


def _get_paths_from_manifest(path: str, boto3_session: Optional[boto3.Session] = None) -> List[str]:
    resource_s3: boto3.resource = _utils.resource(service_name="s3", session=boto3_session)
    bucket, key = _utils.parse_path(path)
    content_object = resource_s3.Object(bucket, key)
    manifest_content = json.loads(content_object.get()["Body"].read().decode("utf-8"))
    return [path["url"] for path in manifest_content["entries"]]


def _make_s3_auth_string(
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
    iam_role: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> str:
    if aws_access_key_id is not None and aws_secret_access_key is not None:
        auth_str: str = f"ACCESS_KEY_ID '{aws_access_key_id}'\nSECRET_ACCESS_KEY '{aws_secret_access_key}'\n"
        if aws_session_token is not None:
            auth_str += f"SESSION_TOKEN '{aws_session_token}'\n"
    elif iam_role is not None:
        auth_str = f"IAM_ROLE '{iam_role}'\n"
    else:
        _logger.debug("Attempting to get S3 authorization credentials from boto3 session.")
        credentials: botocore.credentials.ReadOnlyCredentials
        credentials = _utils.get_credentials_from_session(boto3_session=boto3_session)
        if credentials.access_key is None or credentials.secret_key is None:
            raise exceptions.InvalidArgument(
                "One of IAM Role or AWS ACCESS_KEY_ID and SECRET_ACCESS_KEY must be "
                "given. Unable to find ACCESS_KEY_ID and SECRET_ACCESS_KEY in boto3 "
                "session."
            )

        auth_str = f"ACCESS_KEY_ID '{credentials.access_key}'\nSECRET_ACCESS_KEY '{credentials.secret_key}'\n"
        if credentials.token is not None:
            auth_str += f"SESSION_TOKEN '{credentials.token}'\n"

    return auth_str


def _copy(
    cursor: redshift_connector.Cursor,
    path: str,
    table: str,
    serialize_to_json: bool,
    iam_role: Optional[str] = None,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
    boto3_session: Optional[str] = None,
    schema: Optional[str] = None,
    manifest: Optional[bool] = False,
    sql_copy_extra_params: Optional[List[str]] = None,
    column_names: Optional[List[str]] = None,
) -> None:
    if schema is None:
        table_name: str = f'"{table}"'
    else:
        table_name = f'"{schema}"."{table}"'

    auth_str: str = _make_s3_auth_string(
        iam_role=iam_role,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token,
        boto3_session=boto3_session,
    )
    ser_json_str: str = " SERIALIZETOJSON" if serialize_to_json else ""
    column_names_str: str = f"({','.join(column_names)})" if column_names else ""
    sql = f"COPY {table_name} {column_names_str}\nFROM '{path}' {auth_str}\nFORMAT AS PARQUET{ser_json_str}"

    if manifest:
        sql += "\nMANIFEST"
    if sql_copy_extra_params:
        for param in sql_copy_extra_params:
            sql += f"\n{param}"
    _logger.debug("copy query:\n%s", sql)
    cursor.execute(sql)


def _lock(
    cursor: redshift_connector.Cursor,
    table_names: List[str],
    schema: Optional[str] = None,
) -> None:
    fmt = '"{schema}"."{table}"' if schema else '"{table}"'
    tables = ", ".join([fmt.format(schema=schema, table=table) for table in table_names])
    sql: str = f"LOCK {tables};\n"
    _logger.debug("lock query:\n%s", sql)
    cursor.execute(sql)


def _upsert(
    cursor: redshift_connector.Cursor,
    table: str,
    temp_table: str,
    schema: str,
    primary_keys: Optional[List[str]] = None,
    precombine_key: Optional[str] = None,
    column_names: Optional[List[str]] = None,
) -> None:
    if not primary_keys:
        primary_keys = _get_primary_keys(cursor=cursor, schema=schema, table=table)
    _logger.debug("primary_keys: %s", primary_keys)
    if not primary_keys:
        raise exceptions.InvalidRedshiftPrimaryKeys()
    equals_clause: str = f'"{table}".%s = "{temp_table}".%s'
    join_clause: str = " AND ".join([equals_clause % (pk, pk) for pk in primary_keys])
    if precombine_key:
        delete_from_target_filter: str = f"AND {table}.{precombine_key} <= {temp_table}.{precombine_key}"
        delete_from_temp_filter: str = f"AND {table}.{precombine_key} > {temp_table}.{precombine_key}"
        target_del_sql: str = (
            f'DELETE FROM "{schema}"."{table}" USING "{temp_table}" WHERE {join_clause} {delete_from_target_filter}'
        )
        _logger.debug(target_del_sql)
        cursor.execute(target_del_sql)
        source_del_sql: str = (
            f'DELETE FROM "{temp_table}" USING "{schema}"."{table}" WHERE {join_clause} {delete_from_temp_filter}'
        )
        _logger.debug(source_del_sql)
        cursor.execute(source_del_sql)
    else:
        sql: str = f'DELETE FROM "{schema}"."{table}" USING "{temp_table}" WHERE {join_clause}'
        _logger.debug(sql)
        cursor.execute(sql)
    if column_names:
        column_names_str = ",".join(column_names)
        insert_sql = (
            f'INSERT INTO "{schema}"."{table}"({column_names_str}) SELECT {column_names_str} FROM "{temp_table}"'
        )
    else:
        insert_sql = f'INSERT INTO "{schema}"."{table}" SELECT * FROM "{temp_table}"'
    _logger.debug(insert_sql)
    cursor.execute(insert_sql)
    _drop_table(cursor=cursor, schema=schema, table=temp_table)


def _validate_parameters(
    redshift_types: Dict[str, str],
    diststyle: str,
    distkey: Optional[str],
    sortstyle: str,
    sortkey: Optional[List[str]],
    primary_keys: Optional[List[str]],
) -> None:
    if diststyle not in _RS_DISTSTYLES:
        raise exceptions.InvalidRedshiftDiststyle(f"diststyle must be in {_RS_DISTSTYLES}")
    cols = list(redshift_types.keys())
    _logger.debug("Redshift columns: %s", cols)
    if (diststyle == "KEY") and (not distkey):
        raise exceptions.InvalidRedshiftDistkey("You must pass a distkey if you intend to use KEY diststyle")
    if distkey and distkey not in cols:
        raise exceptions.InvalidRedshiftDistkey(f"distkey ({distkey}) must be in the columns list: {cols})")
    if sortstyle and sortstyle not in _RS_SORTSTYLES:
        raise exceptions.InvalidRedshiftSortstyle(f"sortstyle must be in {_RS_SORTSTYLES}")
    if sortkey:
        if not isinstance(sortkey, list):
            raise exceptions.InvalidRedshiftSortkey(
                f"sortkey must be a List of items in the columns list: {cols}. " f"Currently value: {sortkey}"
            )
        for key in sortkey:
            if key not in cols:
                raise exceptions.InvalidRedshiftSortkey(
                    f"sortkey must be a List of items in the columns list: {cols}. " f"Currently value: {key}"
                )
    if primary_keys:
        if not isinstance(primary_keys, list):
            raise exceptions.InvalidArgumentType(
                f"""
                    primary keys should be of type list[str].
                    Current value: {primary_keys} is of type {type(primary_keys)}
                """
            )


def _redshift_types_from_path(
    path: Optional[Union[str, List[str]]],
    varchar_lengths_default: int,
    varchar_lengths: Optional[Dict[str, int]],
    parquet_infer_sampling: float,
    path_suffix: Optional[str],
    path_ignore_suffix: Optional[str],
    use_threads: Union[bool, int],
    boto3_session: Optional[boto3.Session],
    s3_additional_kwargs: Optional[Dict[str, str]],
) -> Dict[str, str]:
    """Extract Redshift data types from a Pandas DataFrame."""
    _varchar_lengths: Dict[str, int] = {} if varchar_lengths is None else varchar_lengths
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    _logger.debug("Scanning parquet schemas on s3...")
    athena_types, _ = s3.read_parquet_metadata(
        path=path,
        sampling=parquet_infer_sampling,
        path_suffix=path_suffix,
        path_ignore_suffix=path_ignore_suffix,
        dataset=False,
        use_threads=use_threads,
        boto3_session=session,
        s3_additional_kwargs=s3_additional_kwargs,
    )
    _logger.debug("athena_types: %s", athena_types)
    redshift_types: Dict[str, str] = {}
    for col_name, col_type in athena_types.items():
        length: int = _varchar_lengths[col_name] if col_name in _varchar_lengths else varchar_lengths_default
        redshift_types[col_name] = _data_types.athena2redshift(dtype=col_type, varchar_length=length)
    return redshift_types


def _create_table(  # pylint: disable=too-many-locals,too-many-arguments,too-many-branches,too-many-statements
    df: Optional[pd.DataFrame],
    path: Optional[Union[str, List[str]]],
    con: redshift_connector.Connection,
    cursor: redshift_connector.Cursor,
    table: str,
    schema: str,
    mode: str,
    overwrite_method: str,
    index: bool,
    dtype: Optional[Dict[str, str]],
    diststyle: str,
    sortstyle: str,
    distkey: Optional[str],
    sortkey: Optional[List[str]],
    primary_keys: Optional[List[str]],
    varchar_lengths_default: int,
    varchar_lengths: Optional[Dict[str, int]],
    parquet_infer_sampling: float = 1.0,
    path_suffix: Optional[str] = None,
    path_ignore_suffix: Optional[str] = None,
    manifest: Optional[bool] = False,
    use_threads: Union[bool, int] = True,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
    lock: bool = False,
) -> Tuple[str, Optional[str]]:
    if mode == "overwrite":
        if overwrite_method == "truncate":
            try:
                # Truncate commits current transaction, if successful.
                # Fast, but not atomic.
                _truncate_table(cursor=cursor, schema=schema, table=table)
            except redshift_connector.error.ProgrammingError as e:
                # Caught "relation does not exist".
                if e.args[0]["C"] != "42P01":  # pylint: disable=invalid-sequence-index
                    raise e
                _logger.debug(str(e))
                con.rollback()
            _begin_transaction(cursor=cursor)
            if lock:
                _lock(cursor, [table], schema=schema)
        elif overwrite_method == "delete":
            if _does_table_exist(cursor=cursor, schema=schema, table=table):
                if lock:
                    _lock(cursor, [table], schema=schema)
                # Atomic, but slow.
                _delete_all(cursor=cursor, schema=schema, table=table)
        else:
            # Fast, atomic, but either fails if there are any dependent views or, in cascade mode, deletes them.
            _drop_table(cursor=cursor, schema=schema, table=table, cascade=bool(overwrite_method == "cascade"))
            # No point in locking here, the oid will change.
    elif _does_table_exist(cursor=cursor, schema=schema, table=table) is True:
        if lock:
            _lock(cursor, [table], schema=schema)
        if mode == "upsert":
            guid: str = uuid.uuid4().hex
            temp_table: str = f"temp_redshift_{guid}"
            sql: str = f'CREATE TEMPORARY TABLE {temp_table} (LIKE "{schema}"."{table}")'
            _logger.debug(sql)
            cursor.execute(sql)
            return temp_table, None
        return table, schema
    diststyle = diststyle.upper() if diststyle else "AUTO"
    sortstyle = sortstyle.upper() if sortstyle else "COMPOUND"
    if df is not None:
        redshift_types: Dict[str, str] = _data_types.database_types_from_pandas(
            df=df,
            index=index,
            dtype=dtype,
            varchar_lengths_default=varchar_lengths_default,
            varchar_lengths=varchar_lengths,
            converter_func=_data_types.pyarrow2redshift,
        )
    elif path is not None:
        if manifest:
            if not isinstance(path, str):
                raise TypeError(
                    f"""type: {type(path)} is not a valid type for 'path' when 'manifest' is set to True;
                    must be a string"""
                )
            path = _get_paths_from_manifest(
                path=path,
                boto3_session=boto3_session,
            )
        redshift_types = _redshift_types_from_path(
            path=path,
            varchar_lengths_default=varchar_lengths_default,
            varchar_lengths=varchar_lengths,
            parquet_infer_sampling=parquet_infer_sampling,
            path_suffix=path_suffix,
            path_ignore_suffix=path_ignore_suffix,
            use_threads=use_threads,
            boto3_session=boto3_session,
            s3_additional_kwargs=s3_additional_kwargs,
        )
    else:
        raise ValueError("df and path are None.You MUST pass at least one.")
    _validate_parameters(
        redshift_types=redshift_types,
        diststyle=diststyle,
        distkey=distkey,
        sortstyle=sortstyle,
        sortkey=sortkey,
        primary_keys=primary_keys,
    )
    cols_str: str = "".join([f'"{k}" {v},\n' for k, v in redshift_types.items()])[:-2]
    primary_keys_str: str = f",\nPRIMARY KEY ({', '.join(primary_keys)})" if primary_keys else ""
    distkey_str: str = f"\nDISTKEY({distkey})" if distkey and diststyle == "KEY" else ""
    sortkey_str: str = f"\n{sortstyle} SORTKEY({','.join(sortkey)})" if sortkey else ""
    sql = (
        f'CREATE TABLE IF NOT EXISTS "{schema}"."{table}" (\n'
        f"{cols_str}"
        f"{primary_keys_str}"
        f")\nDISTSTYLE {diststyle}"
        f"{distkey_str}"
        f"{sortkey_str}"
    )
    _logger.debug("Create table query:\n%s", sql)
    cursor.execute(sql)
    if lock:
        _lock(cursor, [table], schema=schema)
    return table, schema


def _read_parquet_iterator(
    path: str,
    keep_files: bool,
    use_threads: Union[bool, int],
    categories: Optional[List[str]],
    chunked: Union[bool, int],
    boto3_session: Optional[boto3.Session],
    s3_additional_kwargs: Optional[Dict[str, str]],
) -> Iterator[pd.DataFrame]:
    dfs: Iterator[pd.DataFrame] = s3.read_parquet(
        path=path,
        categories=categories,
        chunked=chunked,
        dataset=False,
        use_threads=use_threads,
        boto3_session=boto3_session,
        s3_additional_kwargs=s3_additional_kwargs,
    )
    yield from dfs
    if keep_files is False:
        s3.delete_objects(
            path=path, use_threads=use_threads, boto3_session=boto3_session, s3_additional_kwargs=s3_additional_kwargs
        )


def connect(
    connection: Optional[str] = None,
    secret_id: Optional[str] = None,
    catalog_id: Optional[str] = None,
    dbname: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
    ssl: bool = True,
    timeout: Optional[int] = None,
    max_prepared_statements: int = 1000,
    tcp_keepalive: bool = True,
    **kwargs: Any,
) -> redshift_connector.Connection:
    """Return a redshift_connector connection from a Glue Catalog or Secret Manager.

    Note
    ----
    You MUST pass a `connection` OR `secret_id`.
    Here is an example of the secret structure in Secrets Manager:
    {
    "host":"my-host.us-east-1.redshift.amazonaws.com",
    "username":"test",
    "password":"test",
    "engine":"redshift",
    "port":"5439",
    "dbname": "mydb"
    }


    https://github.com/aws/amazon-redshift-python-driver

    Parameters
    ----------
    connection : str, optional
        Glue Catalog Connection name.
    secret_id : Optional[str]:
        Specifies the secret containing the connection details that you want to retrieve.
        You can specify either the Amazon Resource Name (ARN) or the friendly name of the secret.
    catalog_id : str, optional
        The ID of the Data Catalog.
        If none is provided, the AWS account ID is used by default.
    dbname : str, optional
        Optional database name to overwrite the stored one.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    ssl : bool
        This governs SSL encryption for TCP/IP sockets.
        This parameter is forward to redshift_connector.
        https://github.com/aws/amazon-redshift-python-driver
    timeout : int, optional
        This is the time in seconds before the connection to the server will time out.
        The default is None which means no timeout.
        This parameter is forward to redshift_connector.
        https://github.com/aws/amazon-redshift-python-driver
    max_prepared_statements : int
        This parameter is forward to redshift_connector.
        https://github.com/aws/amazon-redshift-python-driver
    tcp_keepalive : bool
        If True then use TCP keepalive. The default is True.
        This parameter is forward to redshift_connector.
        https://github.com/aws/amazon-redshift-python-driver
    **kwargs : Any
        Forwarded to redshift_connector.connect.
        e.g. is_serverless=True, serverless_acct_id='...', serverless_work_group='...'

    Returns
    -------
    redshift_connector.Connection
        redshift_connector connection.

    Examples
    --------
    Fetching Redshift connection from Glue Catalog

    >>> import awswrangler as wr
    >>> con = wr.redshift.connect("MY_GLUE_CONNECTION")
    >>> with con.cursor() as cursor:
    >>>     cursor.execute("SELECT 1")
    >>>     print(cursor.fetchall())
    >>> con.close()

    Fetching Redshift connection from Secrets Manager

    >>> import awswrangler as wr
    >>> con = wr.redshift.connect(secret_id="MY_SECRET")
    >>> with con.cursor() as cursor:
    >>>     cursor.execute("SELECT 1")
    >>>     print(cursor.fetchall())
    >>> con.close()

    """
    attrs: _db_utils.ConnectionAttributes = _db_utils.get_connection_attributes(
        connection=connection, secret_id=secret_id, catalog_id=catalog_id, dbname=dbname, boto3_session=boto3_session
    )
    if attrs.kind != "redshift":
        raise exceptions.InvalidDatabaseType(
            f"Invalid connection type ({attrs.kind}. It must be a redshift connection.)"
        )
    return redshift_connector.connect(
        user=attrs.user,
        database=attrs.database,
        password=attrs.password,
        port=int(attrs.port),
        host=attrs.host,
        ssl=ssl,
        timeout=timeout,
        max_prepared_statements=max_prepared_statements,
        tcp_keepalive=tcp_keepalive,
        **kwargs,
    )


def connect_temp(
    cluster_identifier: str,
    user: str,
    database: Optional[str] = None,
    duration: int = 900,
    auto_create: bool = True,
    db_groups: Optional[List[str]] = None,
    boto3_session: Optional[boto3.Session] = None,
    ssl: bool = True,
    timeout: Optional[int] = None,
    max_prepared_statements: int = 1000,
    tcp_keepalive: bool = True,
    **kwargs: Any,
) -> redshift_connector.Connection:
    """Return a redshift_connector temporary connection (No password required).

    https://github.com/aws/amazon-redshift-python-driver

    Parameters
    ----------
    cluster_identifier : str
        The unique identifier of a cluster.
        This parameter is case sensitive.
    user : str, optional
        The name of a database user.
    database : str, optional
        Database name. If None, the default Database is used.
    duration : int, optional
        The number of seconds until the returned temporary password expires.
        Constraint: minimum 900, maximum 3600.
        Default: 900
    auto_create : bool
        Create a database user with the name specified for the user named in user if one does not exist.
    db_groups : List[str], optional
        A list of the names of existing database groups that the user named in user will join for the current session,
        in addition to any group memberships for an existing user. If not specified, a new user is added only to PUBLIC.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    ssl : bool
        This governs SSL encryption for TCP/IP sockets.
        This parameter is forward to redshift_connector.
        https://github.com/aws/amazon-redshift-python-driver
    timeout : int, optional
        This is the time in seconds before the connection to the server will time out.
        The default is None which means no timeout.
        This parameter is forward to redshift_connector.
        https://github.com/aws/amazon-redshift-python-driver
    max_prepared_statements : int
        This parameter is forward to redshift_connector.
        https://github.com/aws/amazon-redshift-python-driver
    tcp_keepalive : bool
        If True then use TCP keepalive. The default is True.
        This parameter is forward to redshift_connector.
        https://github.com/aws/amazon-redshift-python-driver
    **kwargs : Any
        Forwarded to redshift_connector.connect.
        e.g. is_serverless=True, serverless_acct_id='...', serverless_work_group='...'

    Returns
    -------
    redshift_connector.Connection
        redshift_connector connection.

    Examples
    --------
    >>> import awswrangler as wr
    >>> con = wr.redshift.connect_temp(cluster_identifier="my-cluster", user="test")
    >>> with con.cursor() as cursor:
    >>>     cursor.execute("SELECT 1")
    >>>     print(cursor.fetchall())
    >>> con.close()

    """
    client_redshift: boto3.client = _utils.client(service_name="redshift", session=boto3_session)
    args: Dict[str, Any] = {
        "DbUser": user,
        "ClusterIdentifier": cluster_identifier,
        "DurationSeconds": duration,
        "AutoCreate": auto_create,
    }
    if db_groups is not None:
        args["DbGroups"] = db_groups
    else:
        db_groups = []
    res: Dict[str, Any] = client_redshift.get_cluster_credentials(**args)
    cluster: Dict[str, Any] = client_redshift.describe_clusters(ClusterIdentifier=cluster_identifier)["Clusters"][0]
    return redshift_connector.connect(
        user=res["DbUser"],
        database=database if database else cluster["DBName"],
        password=res["DbPassword"],
        port=cluster["Endpoint"]["Port"],
        host=cluster["Endpoint"]["Address"],
        ssl=ssl,
        timeout=timeout,
        max_prepared_statements=max_prepared_statements,
        tcp_keepalive=tcp_keepalive,
        db_groups=db_groups,
        **kwargs,
    )


def read_sql_query(
    sql: str,
    con: redshift_connector.Connection,
    index_col: Optional[Union[str, List[str]]] = None,
    params: Optional[Union[List[Any], Tuple[Any, ...], Dict[Any, Any]]] = None,
    chunksize: Optional[int] = None,
    dtype: Optional[Dict[str, pa.DataType]] = None,
    safe: bool = True,
    timestamp_as_object: bool = False,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Return a DataFrame corresponding to the result set of the query string.

    Note
    ----
    For large extractions (1K+ rows) consider the function **wr.redshift.unload()**.

    Parameters
    ----------
    sql : str
        SQL query.
    con : redshift_connector.Connection
        Use redshift_connector.connect() to use "
        "credentials directly or wr.redshift.connect() to fetch it from the Glue Catalog.
    index_col : Union[str, List[str]], optional
        Column(s) to set as index(MultiIndex).
    params :  Union[List, Tuple, Dict], optional
        List of parameters to pass to execute method.
        The syntax used to pass parameters is database driver dependent.
        Check your database driver documentation for which of the five syntax styles,
        described in PEP 249’s paramstyle, is supported.
    chunksize : int, optional
        If specified, return an iterator where chunksize is the number of rows to include in each chunk.
    dtype : Dict[str, pyarrow.DataType], optional
        Specifying the datatype for columns.
        The keys should be the column names and the values should be the PyArrow types.
    safe : bool
        Check for overflows or other unsafe data type conversions.
    timestamp_as_object : bool
        Cast non-nanosecond timestamps (np.datetime64) to objects.

    Returns
    -------
    Union[pandas.DataFrame, Iterator[pandas.DataFrame]]
        Result as Pandas DataFrame(s).

    Examples
    --------
    Reading from Redshift using a Glue Catalog Connections

    >>> import awswrangler as wr
    >>> con = wr.redshift.connect("MY_GLUE_CONNECTION")
    >>> df = wr.redshift.read_sql_query(
    ...     sql="SELECT * FROM public.my_table",
    ...     con=con
    ... )
    >>> con.close()

    """
    _validate_connection(con=con)
    return _db_utils.read_sql_query(
        sql=sql,
        con=con,
        index_col=index_col,
        params=params,
        chunksize=chunksize,
        dtype=dtype,
        safe=safe,
        timestamp_as_object=timestamp_as_object,
    )


def read_sql_table(
    table: str,
    con: redshift_connector.Connection,
    schema: Optional[str] = None,
    index_col: Optional[Union[str, List[str]]] = None,
    params: Optional[Union[List[Any], Tuple[Any, ...], Dict[Any, Any]]] = None,
    chunksize: Optional[int] = None,
    dtype: Optional[Dict[str, pa.DataType]] = None,
    safe: bool = True,
    timestamp_as_object: bool = False,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Return a DataFrame corresponding the table.

    Note
    ----
    For large extractions (1K+ rows) consider the function **wr.redshift.unload()**.

    Parameters
    ----------
    table : str
        Table name.
    con : redshift_connector.Connection
        Use redshift_connector.connect() to use "
        "credentials directly or wr.redshift.connect() to fetch it from the Glue Catalog.
    schema : str, optional
        Name of SQL schema in database to query (if database flavor supports this).
        Uses default schema if None (default).
    index_col : Union[str, List[str]], optional
        Column(s) to set as index(MultiIndex).
    params :  Union[List, Tuple, Dict], optional
        List of parameters to pass to execute method.
        The syntax used to pass parameters is database driver dependent.
        Check your database driver documentation for which of the five syntax styles,
        described in PEP 249's paramstyle, is supported.
    chunksize : int, optional
        If specified, return an iterator where chunksize is the number of rows to include in each chunk.
    dtype : Dict[str, pyarrow.DataType], optional
        Specifying the datatype for columns.
        The keys should be the column names and the values should be the PyArrow types.
    safe : bool
        Check for overflows or other unsafe data type conversions.
    timestamp_as_object : bool
        Cast non-nanosecond timestamps (np.datetime64) to objects.

    Returns
    -------
    Union[pandas.DataFrame, Iterator[pandas.DataFrame]]
        Result as Pandas DataFrame(s).

    Examples
    --------
    Reading from Redshift using a Glue Catalog Connections

    >>> import awswrangler as wr
    >>> con = wr.redshift.connect("MY_GLUE_CONNECTION")
    >>> df = wr.redshift.read_sql_table(
    ...     table="my_table",
    ...     schema="public",
    ...     con=con
    ... )
    >>> con.close()

    """
    sql: str = f'SELECT * FROM "{table}"' if schema is None else f'SELECT * FROM "{schema}"."{table}"'
    return read_sql_query(
        sql=sql,
        con=con,
        index_col=index_col,
        params=params,
        chunksize=chunksize,
        dtype=dtype,
        safe=safe,
        timestamp_as_object=timestamp_as_object,
    )


@apply_configs
def to_sql(  # pylint: disable=too-many-locals
    df: pd.DataFrame,
    con: redshift_connector.Connection,
    table: str,
    schema: str,
    mode: str = "append",
    overwrite_method: str = "drop",
    index: bool = False,
    dtype: Optional[Dict[str, str]] = None,
    diststyle: str = "AUTO",
    distkey: Optional[str] = None,
    sortstyle: str = "COMPOUND",
    sortkey: Optional[List[str]] = None,
    primary_keys: Optional[List[str]] = None,
    varchar_lengths_default: int = 256,
    varchar_lengths: Optional[Dict[str, int]] = None,
    use_column_names: bool = False,
    lock: bool = False,
    chunksize: int = 200,
    commit_transaction: bool = True,
    precombine_key: Optional[str] = None,
) -> None:
    """Write records stored in a DataFrame into Redshift.

    Note
    ----
    For large DataFrames (1K+ rows) consider the function **wr.redshift.copy()**.


    Parameters
    ----------
    df : pandas.DataFrame
        Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    con : redshift_connector.Connection
        Use redshift_connector.connect() to use "
        "credentials directly or wr.redshift.connect() to fetch it from the Glue Catalog.
    table : str
        Table name
    schema : str
        Schema name
    mode : str
        Append, overwrite or upsert.
    overwrite_method : str
        Drop, cascade, truncate, or delete. Only applicable in overwrite mode.

        "drop" - ``DROP ... RESTRICT`` - drops the table. Fails if there are any views that depend on it.
        "cascade" - ``DROP ... CASCADE`` - drops the table, and all views that depend on it.
        "truncate" - ``TRUNCATE ...`` - truncates the table, but immediately commits current
        transaction & starts a new one, hence the overwrite happens in two transactions and is not atomic.
        "delete" - ``DELETE FROM ...`` - deletes all rows from the table. Slow relative to the other methods.
    index : bool
        True to store the DataFrame index as a column in the table,
        otherwise False to ignore it.
    dtype : Dict[str, str], optional
        Dictionary of columns names and Redshift types to be casted.
        Useful when you have columns with undetermined or mixed data types.
        (e.g. {'col name': 'VARCHAR(10)', 'col2 name': 'FLOAT'})
        diststyle : str
        Redshift distribution styles. Must be in ["AUTO", "EVEN", "ALL", "KEY"].
        https://docs.aws.amazon.com/redshift/latest/dg/t_Distributing_data.html
    distkey : str, optional
        Specifies a column name or positional number for the distribution key.
    sortstyle : str
        Sorting can be "COMPOUND" or "INTERLEAVED".
        https://docs.aws.amazon.com/redshift/latest/dg/t_Sorting_data.html
    sortkey : List[str], optional
        List of columns to be sorted.
    primary_keys : List[str], optional
        Primary keys.
    varchar_lengths_default : int
        The size that will be set for all VARCHAR columns not specified with varchar_lengths.
    varchar_lengths : Dict[str, int], optional
        Dict of VARCHAR length by columns. (e.g. {"col1": 10, "col5": 200}).
    use_column_names: bool
        If set to True, will use the column names of the DataFrame for generating the INSERT SQL Query.
        E.g. If the DataFrame has two columns `col1` and `col3` and `use_column_names` is True, data will only be
        inserted into the database columns `col1` and `col3`.
    lock : bool
        True to execute LOCK command inside the transaction to force serializable isolation.
    chunksize : int
        Number of rows which are inserted with each SQL query. Defaults to inserting 200 rows per query.
    commit_transaction : bool
        Whether to commit the transaction. True by default.
    precombine_key : str, optional
        When there is a primary_key match during upsert, this column will change the upsert method,
        comparing the values of the specified column from source and target, and keeping the
        larger of the two. Will only work when mode = upsert.

    Returns
    -------
    None
        None.

    Examples
    --------
    Writing to Redshift using a Glue Catalog Connections

    >>> import awswrangler as wr
    >>> con = wr.redshift.connect("MY_GLUE_CONNECTION")
    >>> wr.redshift.to_sql(
    ...     df=df,
    ...     table="my_table",
    ...     schema="public",
    ...     con=con
    ... )
    >>> con.close()

    """
    if df.empty is True:
        raise exceptions.EmptyDataFrame("DataFrame cannot be empty.")
    _validate_connection(con=con)
    autocommit_temp: bool = con.autocommit
    con.autocommit = False
    try:
        with con.cursor() as cursor:
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
                _logger.debug("sql: %s", sql)
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


def unload_to_files(
    sql: str,
    path: str,
    con: redshift_connector.Connection,
    iam_role: Optional[str] = None,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
    region: Optional[str] = None,
    unload_format: Optional[str] = None,
    max_file_size: Optional[float] = None,
    kms_key_id: Optional[str] = None,
    manifest: bool = False,
    partition_cols: Optional[List[str]] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> None:
    """Unload Parquet files on s3 from a Redshift query result (Through the UNLOAD command).

    https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html

    Note
    ----
    In case of `use_threads=True` the number of threads
    that will be spawned will be gotten from os.cpu_count().

    Parameters
    ----------
    sql: str
        SQL query.
    path : Union[str, List[str]]
        S3 path to write stage files (e.g. s3://bucket_name/any_name/)
    con : redshift_connector.Connection
        Use redshift_connector.connect() to use "
        "credentials directly or wr.redshift.connect() to fetch it from the Glue Catalog.
    iam_role : str, optional
        AWS IAM role with the related permissions.
    aws_access_key_id : str, optional
        The access key for your AWS account.
    aws_secret_access_key : str, optional
        The secret key for your AWS account.
    aws_session_token : str, optional
        The session key for your AWS account. This is only needed when you are using temporary credentials.
    region : str, optional
        Specifies the AWS Region where the target Amazon S3 bucket is located.
        REGION is required for UNLOAD to an Amazon S3 bucket that isn't in the
        same AWS Region as the Amazon Redshift cluster. By default, UNLOAD
        assumes that the target Amazon S3 bucket is located in the same AWS
        Region as the Amazon Redshift cluster.
    unload_format: str, optional
        Format of the unloaded S3 objects from the query.
        Valid values: "CSV", "PARQUET". Case sensitive. Defaults to PARQUET.
    max_file_size : float, optional
        Specifies the maximum size (MB) of files that UNLOAD creates in Amazon S3.
        Specify a decimal value between 5.0 MB and 6200.0 MB. If None, the default
        maximum file size is 6200.0 MB.
    kms_key_id : str, optional
        Specifies the key ID for an AWS Key Management Service (AWS KMS) key to be
        used to encrypt data files on Amazon S3.
    manifest : bool
        Unload a manifest file on S3.
    partition_cols: List[str], optional
        Specifies the partition keys for the unload operation.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    None

    Examples
    --------
    >>> import awswrangler as wr
    >>> con = wr.redshift.connect("MY_GLUE_CONNECTION")
    >>> wr.redshift.unload_to_files(
    ...     sql="SELECT * FROM public.mytable",
    ...     path="s3://bucket/extracted_parquet_files/",
    ...     con=con,
    ...     iam_role="arn:aws:iam::XXX:role/XXX"
    ... )
    >>> con.close()


    """
    if unload_format not in [None, "CSV", "PARQUET"]:
        raise exceptions.InvalidArgumentValue("<unload_format> argument must be 'CSV' or 'PARQUET'")
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    with con.cursor() as cursor:
        format_str: str = unload_format or "PARQUET"
        partition_str: str = f"\nPARTITION BY ({','.join(partition_cols)})" if partition_cols else ""
        manifest_str: str = "\nmanifest" if manifest is True else ""
        region_str: str = f"\nREGION AS '{region}'" if region is not None else ""
        max_file_size_str: str = f"\nMAXFILESIZE AS {max_file_size} MB" if max_file_size is not None else ""
        kms_key_id_str: str = f"\nKMS_KEY_ID '{kms_key_id}'" if kms_key_id is not None else ""

        auth_str: str = _make_s3_auth_string(
            iam_role=iam_role,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            boto3_session=session,
        )

        sql = (
            f"UNLOAD ('{sql}')\n"
            f"TO '{path}'\n"
            f"{auth_str}"
            "ALLOWOVERWRITE\n"
            "PARALLEL ON\n"
            f"FORMAT {format_str}\n"
            "ENCRYPTED"
            f"{kms_key_id_str}"
            f"{partition_str}"
            f"{region_str}"
            f"{max_file_size_str}"
            f"{manifest_str};"
        )
        _logger.debug("sql: \n%s", sql)
        cursor.execute(sql)


def unload(
    sql: str,
    path: str,
    con: redshift_connector.Connection,
    iam_role: Optional[str] = None,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
    region: Optional[str] = None,
    max_file_size: Optional[float] = None,
    kms_key_id: Optional[str] = None,
    categories: Optional[List[str]] = None,
    chunked: Union[bool, int] = False,
    keep_files: bool = False,
    use_threads: Union[bool, int] = True,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Load Pandas DataFrame from a Amazon Redshift query result using Parquet files on s3 as stage.

    This is a **HIGH** latency and **HIGH** throughput alternative to
    `wr.redshift.read_sql_query()`/`wr.redshift.read_sql_table()` to extract large
    Amazon Redshift data into a Pandas DataFrames through the **UNLOAD command**.

    This strategy has more overhead and requires more IAM privileges
    than the regular `wr.redshift.read_sql_query()`/`wr.redshift.read_sql_table()` function,
    so it is only recommended to fetch 1k+ rows at once.

    https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html

    Note
    ----
    ``Batching`` (`chunked` argument) (Memory Friendly):

    Will enable the function to return an Iterable of DataFrames instead of a regular DataFrame.

    There are two batching strategies on awswrangler:

    - If **chunked=True**, a new DataFrame will be returned for each file in your path/dataset.

    - If **chunked=INTEGER**, awswrangler will iterate on the data by number of rows (equal to the received INTEGER).

    `P.S.` `chunked=True` is faster and uses less memory while `chunked=INTEGER` is more precise
    in the number of rows for each Dataframe.


    Note
    ----
    In case of `use_threads=True` the number of threads
    that will be spawned will be gotten from os.cpu_count().

    Parameters
    ----------
    sql : str
        SQL query.
    path : Union[str, List[str]]
        S3 path to write stage files (e.g. s3://bucket_name/any_name/)
    con : redshift_connector.Connection
        Use redshift_connector.connect() to use "
        "credentials directly or wr.redshift.connect() to fetch it from the Glue Catalog.
    iam_role : str, optional
        AWS IAM role with the related permissions.
    aws_access_key_id : str, optional
        The access key for your AWS account.
    aws_secret_access_key : str, optional
        The secret key for your AWS account.
    aws_session_token : str, optional
        The session key for your AWS account. This is only needed when you are using temporary credentials.
    region : str, optional
        Specifies the AWS Region where the target Amazon S3 bucket is located.
        REGION is required for UNLOAD to an Amazon S3 bucket that isn't in the
        same AWS Region as the Amazon Redshift cluster. By default, UNLOAD
        assumes that the target Amazon S3 bucket is located in the same AWS
        Region as the Amazon Redshift cluster.
    max_file_size : float, optional
        Specifies the maximum size (MB) of files that UNLOAD creates in Amazon S3.
        Specify a decimal value between 5.0 MB and 6200.0 MB. If None, the default
        maximum file size is 6200.0 MB.
    kms_key_id : str, optional
        Specifies the key ID for an AWS Key Management Service (AWS KMS) key to be
        used to encrypt data files on Amazon S3.
    categories: List[str], optional
        List of columns names that should be returned as pandas.Categorical.
        Recommended for memory restricted environments.
    keep_files : bool
        Should keep stage files?
    chunked : Union[int, bool]
        If passed will split the data in a Iterable of DataFrames (Memory friendly).
        If `True` awswrangler iterates on the data by files in the most efficient way without guarantee of chunksize.
        If an `INTEGER` is passed awswrangler will iterate on the data by number of rows igual the received INTEGER.
    use_threads : bool, int
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
        If integer is provided, specified number is used.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    s3_additional_kwargs : Dict[str, str], optional
        Forward to botocore requests, only "SSECustomerAlgorithm" and "SSECustomerKey" arguments will be considered.

    Returns
    -------
    Union[pandas.DataFrame, Iterator[pandas.DataFrame]]
        Result as Pandas DataFrame(s).

    Examples
    --------
    >>> import awswrangler as wr
    >>> con = wr.redshift.connect("MY_GLUE_CONNECTION")
    >>> df = wr.redshift.unload(
    ...     sql="SELECT * FROM public.mytable",
    ...     path="s3://bucket/extracted_parquet_files/",
    ...     con=con,
    ...     iam_role="arn:aws:iam::XXX:role/XXX"
    ... )
    >>> con.close()

    """
    path = path if path.endswith("/") else f"{path}/"
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    unload_to_files(
        sql=sql,
        path=path,
        con=con,
        iam_role=iam_role,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token,
        region=region,
        max_file_size=max_file_size,
        kms_key_id=kms_key_id,
        manifest=False,
        boto3_session=session,
    )
    if chunked is False:
        df: pd.DataFrame = s3.read_parquet(
            path=path,
            categories=categories,
            chunked=chunked,
            dataset=False,
            use_threads=use_threads,
            boto3_session=session,
            s3_additional_kwargs=s3_additional_kwargs,
        )
        if keep_files is False:
            s3.delete_objects(
                path=path, use_threads=use_threads, boto3_session=session, s3_additional_kwargs=s3_additional_kwargs
            )
        return df
    return _read_parquet_iterator(
        path=path,
        categories=categories,
        chunked=chunked,
        use_threads=use_threads,
        boto3_session=session,
        s3_additional_kwargs=s3_additional_kwargs,
        keep_files=keep_files,
    )


def copy_from_files(  # pylint: disable=too-many-locals,too-many-arguments
    path: str,
    con: redshift_connector.Connection,
    table: str,
    schema: str,
    iam_role: Optional[str] = None,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
    parquet_infer_sampling: float = 1.0,
    mode: str = "append",
    overwrite_method: str = "drop",
    diststyle: str = "AUTO",
    distkey: Optional[str] = None,
    sortstyle: str = "COMPOUND",
    sortkey: Optional[List[str]] = None,
    primary_keys: Optional[List[str]] = None,
    varchar_lengths_default: int = 256,
    varchar_lengths: Optional[Dict[str, int]] = None,
    serialize_to_json: bool = False,
    path_suffix: Optional[str] = None,
    path_ignore_suffix: Optional[str] = None,
    use_threads: Union[bool, int] = True,
    lock: bool = False,
    commit_transaction: bool = True,
    manifest: Optional[bool] = False,
    sql_copy_extra_params: Optional[List[str]] = None,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
    precombine_key: Optional[str] = None,
    column_names: Optional[List[str]] = None,
) -> None:
    """Load Parquet files from S3 to a Table on Amazon Redshift (Through COPY command).

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
    path : str
        S3 prefix (e.g. s3://bucket/prefix/)
    con : redshift_connector.Connection
        Use redshift_connector.connect() to use "
        "credentials directly or wr.redshift.connect() to fetch it from the Glue Catalog.
    table : str
        Table name
    schema : str
        Schema name
    iam_role : str, optional
        AWS IAM role with the related permissions.
    aws_access_key_id : str, optional
        The access key for your AWS account.
    aws_secret_access_key : str, optional
        The secret key for your AWS account.
    aws_session_token : str, optional
        The session key for your AWS account. This is only needed when you are using temporary credentials.
    parquet_infer_sampling : float
        Random sample ratio of files that will have the metadata inspected.
        Must be `0.0 < sampling <= 1.0`.
        The higher, the more accurate.
        The lower, the faster.
    mode : str
        Append, overwrite or upsert.
    overwrite_method : str
        Drop, cascade, truncate, or delete. Only applicable in overwrite mode.

        "drop" - ``DROP ... RESTRICT`` - drops the table. Fails if there are any views that depend on it.
        "cascade" - ``DROP ... CASCADE`` - drops the table, and all views that depend on it.
        "truncate" - ``TRUNCATE ...`` - truncates the table, but immediately commits current
        transaction & starts a new one, hence the overwrite happens in two transactions and is not atomic.
        "delete" - ``DELETE FROM ...`` - deletes all rows from the table. Slow relative to the other methods.
    diststyle : str
        Redshift distribution styles. Must be in ["AUTO", "EVEN", "ALL", "KEY"].
        https://docs.aws.amazon.com/redshift/latest/dg/t_Distributing_data.html
    distkey : str, optional
        Specifies a column name or positional number for the distribution key.
    sortstyle : str
        Sorting can be "COMPOUND" or "INTERLEAVED".
        https://docs.aws.amazon.com/redshift/latest/dg/t_Sorting_data.html
    sortkey : List[str], optional
        List of columns to be sorted.
    primary_keys : List[str], optional
        Primary keys.
    varchar_lengths_default : int
        The size that will be set for all VARCHAR columns not specified with varchar_lengths.
    varchar_lengths : Dict[str, int], optional
        Dict of VARCHAR length by columns. (e.g. {"col1": 10, "col5": 200}).
    serialize_to_json : bool
        Should awswrangler add SERIALIZETOJSON parameter into the COPY command?
        SERIALIZETOJSON is necessary to load nested data
        https://docs.aws.amazon.com/redshift/latest/dg/ingest-super.html#copy_json
    path_suffix : Union[str, List[str], None]
        Suffix or List of suffixes to be scanned on s3 for the schema extraction
        (e.g. [".gz.parquet", ".snappy.parquet"]).
        Only has effect during the table creation.
        If None, will try to read all files. (default)
    path_ignore_suffix : Union[str, List[str], None]
        Suffix or List of suffixes for S3 keys to be ignored during the schema extraction.
        (e.g. [".csv", "_SUCCESS"]).
        Only has effect during the table creation.
        If None, will try to read all files. (default)
    use_threads : bool, int
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
        If integer is provided, specified number is used.
    lock : bool
        True to execute LOCK command inside the transaction to force serializable isolation.
    commit_transaction : bool
        Whether to commit the transaction. True by default.
    manifest : bool
        If set to true path argument accepts a S3 uri to a manifest file.
    sql_copy_extra_params : Optional[List[str]]
        Additional copy parameters to pass to the command. For example: ["STATUPDATE ON"]
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    s3_additional_kwargs : Dict[str, str], optional
        Forwarded to botocore requests.
        e.g. s3_additional_kwargs={'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'}
    precombine_key : str, optional
        When there is a primary_key match during upsert, this column will change the upsert method,
        comparing the values of the specified column from source and target, and keeping the
        larger of the two. Will only work when mode = upsert.
    column_names: List[str], optional
        List of column names to map source data fields to the target columns.

    Returns
    -------
    None
        None.

    Examples
    --------
    >>> import awswrangler as wr
    >>> con = wr.redshift.connect("MY_GLUE_CONNECTION")
    >>> wr.redshift.copy_from_files(
    ...     path="s3://bucket/my_parquet_files/",
    ...     con=con,
    ...     table="my_table",
    ...     schema="public",
    ...     iam_role="arn:aws:iam::XXX:role/XXX"
    ... )
    >>> con.close()

    """
    autocommit_temp: bool = con.autocommit
    con.autocommit = False
    try:
        with con.cursor() as cursor:
            created_table, created_schema = _create_table(
                df=None,
                path=path,
                parquet_infer_sampling=parquet_infer_sampling,
                path_suffix=path_suffix,
                path_ignore_suffix=path_ignore_suffix,
                con=con,
                cursor=cursor,
                table=table,
                schema=schema,
                mode=mode,
                overwrite_method=overwrite_method,
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


def copy(  # pylint: disable=too-many-arguments,too-many-locals
    df: pd.DataFrame,
    path: str,
    con: redshift_connector.Connection,
    table: str,
    schema: str,
    iam_role: Optional[str] = None,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
    index: bool = False,
    dtype: Optional[Dict[str, str]] = None,
    mode: str = "append",
    overwrite_method: str = "drop",
    diststyle: str = "AUTO",
    distkey: Optional[str] = None,
    sortstyle: str = "COMPOUND",
    sortkey: Optional[List[str]] = None,
    primary_keys: Optional[List[str]] = None,
    varchar_lengths_default: int = 256,
    varchar_lengths: Optional[Dict[str, int]] = None,
    serialize_to_json: bool = False,
    keep_files: bool = False,
    use_threads: Union[bool, int] = True,
    lock: bool = False,
    sql_copy_extra_params: Optional[List[str]] = None,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
    max_rows_by_file: Optional[int] = 10_000_000,
    precombine_key: Optional[str] = None,
    use_column_names: bool = False,
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
    df : pandas.DataFrame
        Pandas DataFrame.
    path : str
        S3 path to write stage files (e.g. s3://bucket_name/any_name/).
        Note: This path must be empty.
    con : redshift_connector.Connection
        Use redshift_connector.connect() to use "
        "credentials directly or wr.redshift.connect() to fetch it from the Glue Catalog.
    table : str
        Table name
    schema : str
        Schema name
    iam_role : str, optional
        AWS IAM role with the related permissions.
    aws_access_key_id : str, optional
        The access key for your AWS account.
    aws_secret_access_key : str, optional
        The secret key for your AWS account.
    aws_session_token : str, optional
        The session key for your AWS account. This is only needed when you are using temporary credentials.
    index : bool
        True to store the DataFrame index in file, otherwise False to ignore it.
    dtype : Dict[str, str], optional
        Dictionary of columns names and Athena/Glue types to be casted.
        Useful when you have columns with undetermined or mixed data types.
        Only takes effect if dataset=True.
        (e.g. {'col name': 'bigint', 'col2 name': 'int'})
    mode : str
        Append, overwrite or upsert.
    overwrite_method : str
        Drop, cascade, truncate, or delete. Only applicable in overwrite mode.

        "drop" - ``DROP ... RESTRICT`` - drops the table. Fails if there are any views that depend on it.
        "cascade" - ``DROP ... CASCADE`` - drops the table, and all views that depend on it.
        "truncate" - ``TRUNCATE ...`` - truncates the table, but immediately commits current
        transaction & starts a new one, hence the overwrite happens in two transactions and is not atomic.
        "delete" - ``DELETE FROM ...`` - deletes all rows from the table. Slow relative to the other methods.
    diststyle : str
        Redshift distribution styles. Must be in ["AUTO", "EVEN", "ALL", "KEY"].
        https://docs.aws.amazon.com/redshift/latest/dg/t_Distributing_data.html
    distkey : str, optional
        Specifies a column name or positional number for the distribution key.
    sortstyle : str
        Sorting can be "COMPOUND" or "INTERLEAVED".
        https://docs.aws.amazon.com/redshift/latest/dg/t_Sorting_data.html
    sortkey : List[str], optional
        List of columns to be sorted.
    primary_keys : List[str], optional
        Primary keys.
    varchar_lengths_default : int
        The size that will be set for all VARCHAR columns not specified with varchar_lengths.
    varchar_lengths : Dict[str, int], optional
        Dict of VARCHAR length by columns. (e.g. {"col1": 10, "col5": 200}).
    keep_files : bool
        Should keep stage files?
    use_threads : bool, int
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
        If integer is provided, specified number is used.
    lock : bool
        True to execute LOCK command inside the transaction to force serializable isolation.
    sql_copy_extra_params : Optional[List[str]]
        Additional copy parameters to pass to the command. For example: ["STATUPDATE ON"]
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    s3_additional_kwargs : Dict[str, str], optional
        Forwarded to botocore requests.
        e.g. s3_additional_kwargs={'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'}
    max_rows_by_file : int
        Max number of rows in each file.
        Default is None i.e. dont split the files.
        (e.g. 33554432, 268435456)
    precombine_key : str, optional
        When there is a primary_key match during upsert, this column will change the upsert method,
        comparing the values of the specified column from source and target, and keeping the
        larger of the two. Will only work when mode = upsert.
    use_column_names: bool
        If set to True, will use the column names of the DataFrame for generating the INSERT SQL Query.
        E.g. If the DataFrame has two columns `col1` and `col3` and `use_column_names` is True, data will only be
        inserted into the database columns `col1` and `col3`.

    Returns
    -------
    None
        None.

    Examples
    --------
    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> con = wr.redshift.connect("MY_GLUE_CONNECTION")
    >>> wr.redshift.copy(
    ...     df=pd.DataFrame({'col': [1, 2, 3]}),
    ...     path="s3://bucket/my_parquet_files/",
    ...     con=con,
    ...     table="my_table",
    ...     schema="public",
    ...     iam_role="arn:aws:iam::XXX:role/XXX"
    ... )
    >>> con.close()

    """
    path = path[:-1] if path.endswith("*") else path
    path = path if path.endswith("/") else f"{path}/"
    column_names = [f'"{column}"' for column in df.columns] if use_column_names else []
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    if s3.list_objects(path=path, boto3_session=session, s3_additional_kwargs=s3_additional_kwargs):
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
            boto3_session=session,
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
            boto3_session=session,
            s3_additional_kwargs=s3_additional_kwargs,
            sql_copy_extra_params=sql_copy_extra_params,
            precombine_key=precombine_key,
            column_names=column_names,
        )
    finally:
        if keep_files is False:
            s3.delete_objects(
                path=path, use_threads=use_threads, boto3_session=session, s3_additional_kwargs=s3_additional_kwargs
            )
