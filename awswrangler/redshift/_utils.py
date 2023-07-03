# mypy: disable-error-code=name-defined
"""Amazon Redshift Utils Module (PRIVATE)."""
import json
import logging
import uuid
from typing import Dict, List, Optional, Tuple, Union

import boto3
import botocore
import pandas as pd

from awswrangler import _data_types, _utils, exceptions, s3

redshift_connector = _utils.import_optional_dependency("redshift_connector")

_logger: logging.Logger = logging.getLogger(__name__)


_RS_DISTSTYLES: List[str] = ["AUTO", "EVEN", "ALL", "KEY"]
_RS_SORTSTYLES: List[str] = ["COMPOUND", "INTERLEAVED"]


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


def _begin_transaction(cursor: "redshift_connector.Cursor") -> None:
    sql = "BEGIN TRANSACTION"
    _logger.debug("Executing begin transaction query:\n%s", sql)
    cursor.execute(sql)


def _drop_table(cursor: "redshift_connector.Cursor", schema: Optional[str], table: str, cascade: bool = False) -> None:
    schema_str = f'"{schema}".' if schema else ""
    cascade_str = " CASCADE" if cascade else ""
    sql = f'DROP TABLE IF EXISTS {schema_str}"{table}"' f"{cascade_str}"
    _logger.debug("Executing drop table query:\n%s", sql)
    cursor.execute(sql)


def _truncate_table(cursor: "redshift_connector.Cursor", schema: Optional[str], table: str) -> None:
    schema_str = f'"{schema}".' if schema else ""
    sql = f'TRUNCATE TABLE {schema_str}"{table}"'
    _logger.debug("Executing truncate table query:\n%s", sql)
    cursor.execute(sql)


def _delete_all(cursor: "redshift_connector.Cursor", schema: Optional[str], table: str) -> None:
    schema_str = f'"{schema}".' if schema else ""
    sql = f'DELETE FROM {schema_str}"{table}"'
    _logger.debug("Executing delete query:\n%s", sql)
    cursor.execute(sql)


def _get_primary_keys(cursor: "redshift_connector.Cursor", schema: str, table: str) -> List[str]:
    sql = f"SELECT indexdef FROM pg_indexes WHERE schemaname = '{schema}' AND tablename = '{table}'"
    _logger.debug("Executing select query:\n%s", sql)
    cursor.execute(sql)
    result: str = cursor.fetchall()[0][0]
    rfields: List[str] = result.split("(")[1].strip(")").split(",")
    fields: List[str] = [field.strip().strip('"') for field in rfields]
    return fields


def _does_table_exist(cursor: "redshift_connector.Cursor", schema: Optional[str], table: str) -> bool:
    schema_str = f"TABLE_SCHEMA = '{schema}' AND" if schema else ""
    sql = (
        f"SELECT true WHERE EXISTS ("
        f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE "
        f"{schema_str} TABLE_NAME = '{table}'"
        f");"
    )
    _logger.debug("Executing select query:\n%s", sql)
    cursor.execute(sql)
    return len(cursor.fetchall()) > 0


def _get_paths_from_manifest(path: str, boto3_session: Optional[boto3.Session] = None) -> List[str]:
    client_s3 = _utils.client(service_name="s3", session=boto3_session)
    bucket, key = _utils.parse_path(path)
    manifest_content = json.loads(client_s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8"))
    paths = [path["url"] for path in manifest_content["entries"]]
    _logger.debug("Read %d paths from manifest file in: %s", len(paths), path)
    return paths


def _lock(
    cursor: "redshift_connector.Cursor",
    table_names: List[str],
    schema: Optional[str] = None,
) -> None:
    fmt = '"{schema}"."{table}"' if schema else '"{table}"'
    tables = ", ".join([fmt.format(schema=schema, table=table) for table in table_names])
    sql: str = f"LOCK {tables};\n"
    _logger.debug("Executing lock query:\n%s", sql)
    cursor.execute(sql)


def _upsert(
    cursor: "redshift_connector.Cursor",
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
        _logger.debug("Executing delete query:\n%s", target_del_sql)
        cursor.execute(target_del_sql)
        source_del_sql: str = (
            f'DELETE FROM "{temp_table}" USING "{schema}"."{table}" WHERE {join_clause} {delete_from_temp_filter}'
        )
        _logger.debug("Executing delete query:\n%s", source_del_sql)
        cursor.execute(source_del_sql)
    else:
        sql: str = f'DELETE FROM "{schema}"."{table}" USING "{temp_table}" WHERE {join_clause}'
        _logger.debug("Executing delete query:\n%s", sql)
        cursor.execute(sql)
    if column_names:
        column_names_str = ",".join(column_names)
        insert_sql = (
            f'INSERT INTO "{schema}"."{table}"({column_names_str}) SELECT {column_names_str} FROM "{temp_table}"'
        )
    else:
        insert_sql = f'INSERT INTO "{schema}"."{table}" SELECT * FROM "{temp_table}"'
    _logger.debug("Executing insert query:\n%s", insert_sql)
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
    path: Union[str, List[str]],
    varchar_lengths_default: int,
    varchar_lengths: Optional[Dict[str, int]],
    parquet_infer_sampling: float,
    path_suffix: Optional[str],
    path_ignore_suffix: Union[str, List[str], None],
    use_threads: Union[bool, int],
    boto3_session: Optional[boto3.Session],
    s3_additional_kwargs: Optional[Dict[str, str]],
) -> Dict[str, str]:
    """Extract Redshift data types from a Pandas DataFrame."""
    _varchar_lengths: Dict[str, int] = {} if varchar_lengths is None else varchar_lengths
    _logger.debug("Scanning parquet schemas in S3 path: %s", path)
    athena_types, _ = s3.read_parquet_metadata(
        path=path,
        sampling=parquet_infer_sampling,
        path_suffix=path_suffix,
        path_ignore_suffix=path_ignore_suffix,
        dataset=False,
        use_threads=use_threads,
        boto3_session=boto3_session,
        s3_additional_kwargs=s3_additional_kwargs,
    )
    _logger.debug("Parquet metadata types: %s", athena_types)
    redshift_types: Dict[str, str] = {}
    for col_name, col_type in athena_types.items():
        length: int = _varchar_lengths[col_name] if col_name in _varchar_lengths else varchar_lengths_default
        redshift_types[col_name] = _data_types.athena2redshift(dtype=col_type, varchar_length=length)
    _logger.debug("Converted redshift types: %s", redshift_types)
    return redshift_types


def _create_table(  # pylint: disable=too-many-locals,too-many-arguments,too-many-branches,too-many-statements
    df: Optional[pd.DataFrame],
    path: Optional[Union[str, List[str]]],
    con: "redshift_connector.Connection",
    cursor: "redshift_connector.Cursor",
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
    path_ignore_suffix: Union[str, List[str], None] = None,
    manifest: Optional[bool] = False,
    use_threads: Union[bool, int] = True,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
    lock: bool = False,
) -> Tuple[str, Optional[str]]:
    _logger.debug("Creating table %s with mode %s, and overwrite method %s", table, mode, overwrite_method)
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
        _logger.debug("Table %s exists", table)
        if lock:
            _lock(cursor, [table], schema=schema)
        if mode == "upsert":
            guid: str = uuid.uuid4().hex
            temp_table: str = f"temp_redshift_{guid}"
            sql: str = f'CREATE TEMPORARY TABLE {temp_table} (LIKE "{schema}"."{table}")'
            _logger.debug("Executing create temporary table query:\n%s", sql)
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
        _logger.debug("Converted redshift types from pandas: %s", redshift_types)
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
    primary_keys_str: str = (
        ",\nPRIMARY KEY ({})".format(", ".join('"' + pk + '"' for pk in primary_keys)) if primary_keys else ""
    )
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
    _logger.debug("Executing create table query:\n%s", sql)
    cursor.execute(sql)
    _logger.info("Created table %s", table)
    if lock:
        _lock(cursor, [table], schema=schema)
    return table, schema
