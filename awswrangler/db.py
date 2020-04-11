"""Database Module. Currently wrapping up all Redshift, PostgreSQL and MySQL functionalities."""

import json
import logging
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union
from urllib.parse import quote_plus

import boto3  # type: ignore
import pandas as pd  # type: ignore
import pyarrow as pa  # type: ignore
import sqlalchemy  # type: ignore
from sqlalchemy.sql.visitors import VisitableType  # type: ignore

from awswrangler import _data_types, _utils, exceptions, s3

_logger: logging.Logger = logging.getLogger(__name__)


_RS_DISTSTYLES = ["AUTO", "EVEN", "ALL", "KEY"]

_RS_SORTSTYLES = ["COMPOUND", "INTERLEAVED"]


def to_sql(df: pd.DataFrame, con: sqlalchemy.engine.Engine, **pandas_kwargs) -> None:
    """Write records stored in a DataFrame to a SQL database.

    Support for **Redshift**, **PostgreSQL** and **MySQL**.

    Support for all pandas to_sql() arguments:
    https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_sql.html

    Note
    ----
    Redshift: For large DataFrames (1MM+ rows) consider the function **wr.db.copy_to_redshift()**.

    Parameters
    ----------
    df : pandas.DataFrame
        Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    con : sqlalchemy.engine.Engine
        SQLAlchemy Engine. Please use,
        wr.db.get_engine(), wr.db.get_redshift_temp_engine() or wr.catalog.get_engine()
    pandas_kwargs
        keyword arguments forwarded to pandas.DataFrame.to_csv()
        https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_sql.html

    Returns
    -------
    None
        None.

    Examples
    --------
    Writing to Redshift with temporary credentials

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.db.to_sql(
    ...     df=pd.DataFrame({'col': [1, 2, 3]}),
    ...     con=wr.db.get_redshift_temp_engine(cluster_identifier="...", user="..."),
    ...     name="table_name",
    ...     schema="schema_name"
    ... )

    Writing to Redshift from Glue Catalog Connections

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.db.to_sql(
    ...     df=pd.DataFrame({'col': [1, 2, 3]}),
    ...     con=wr.catalog.get_engine(connection="..."),
    ...     name="table_name",
    ...     schema="schema_name"
    ... )

    """
    if df.empty is True:  # pragma: no cover
        raise exceptions.EmptyDataFrame()
    if not isinstance(con, sqlalchemy.engine.Engine):  # pragma: no cover
        raise exceptions.InvalidConnection(
            "Invalid 'con' argument, please pass a "
            "SQLAlchemy Engine. Use wr.db.get_engine(), "
            "wr.db.get_redshift_temp_engine() or wr.catalog.get_engine()"
        )
    if "dtype" in pandas_kwargs:
        cast_columns: Dict[str, VisitableType] = pandas_kwargs["dtype"]
    else:
        cast_columns = {}
    dtypes: Dict[str, VisitableType] = _data_types.sqlalchemy_types_from_pandas(
        df=df, db_type=con.name, dtype=cast_columns
    )
    pandas_kwargs["dtype"] = dtypes
    pandas_kwargs["con"] = con
    df.to_sql(**pandas_kwargs)


def read_sql_query(
    sql: str,
    con: sqlalchemy.engine.Engine,
    index_col: Optional[Union[str, List[str]]] = None,
    params: Optional[Union[List, Tuple, Dict]] = None,
    chunksize: Optional[int] = None,
    dtype: Optional[Dict[str, pa.DataType]] = None,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Return a DataFrame corresponding to the result set of the query string.

    Support for **Redshift**, **PostgreSQL** and **MySQL**.

    Note
    ----
    Redshift: For large extractions (1MM+ rows) consider the function **wr.db.unload_redshift()**.

    Parameters
    ----------
    sql : str
        Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    con : sqlalchemy.engine.Engine
        SQLAlchemy Engine. Please use,
        wr.db.get_engine(), wr.db.get_redshift_temp_engine() or wr.catalog.get_engine()
    index_col : Union[str, List[str]], optional
        Column(s) to set as index(MultiIndex).
    params :  Union[List, Tuple, Dict], optional
        List of parameters to pass to execute method.
        The syntax used to pass parameters is database driver dependent.
        Check your database driver documentation for which of the five syntax styles,
        described in PEP 249’s paramstyle, is supported.
        Eg. for psycopg2, uses %(name)s so use params={‘name’ : ‘value’}.
    chunksize : int, optional
        If specified, return an iterator where chunksize is the number of rows to include in each chunk.
    dtype : Dict[str, pyarrow.DataType], optional
        Specifying the datatype for columns.
        The keys should be the column names and the values should be the PyArrow types.

    Returns
    -------
    Union[pandas.DataFrame, Iterator[pandas.DataFrame]]
        Result as Pandas DataFrame(s).

    Examples
    --------
    Reading from Redshift with temporary credentials

    >>> import awswrangler as wr
    >>> df = wr.db.read_sql_query(
    ...     sql="SELECT * FROM public.my_table",
    ...     con=wr.db.get_redshift_temp_engine(cluster_identifier="...", user="...")
    ... )

    Reading from Redshift from Glue Catalog Connections

    >>> import awswrangler as wr
    >>> df = wr.db.read_sql_query(
    ...     sql="SELECT * FROM public.my_table",
    ...     con=wr.catalog.get_engine(connection="...")
    ... )

    """
    if not isinstance(con, sqlalchemy.engine.Engine):  # pragma: no cover
        raise exceptions.InvalidConnection(
            "Invalid 'con' argument, please pass a "
            "SQLAlchemy Engine. Use wr.db.get_engine(), "
            "wr.db.get_redshift_temp_engine() or wr.catalog.get_engine()"
        )
    with con.connect() as _con:
        args = _convert_params(sql, params)
        cursor = _con.execute(*args)
        if chunksize is None:
            return _records2df(records=cursor.fetchall(), cols_names=cursor.keys(), index=index_col, dtype=dtype)
        return _iterate_cursor(cursor=cursor, chunksize=chunksize, index=index_col, dtype=dtype)


def _iterate_cursor(
    cursor, chunksize: int, index: Optional[Union[str, List[str]]], dtype: Optional[Dict[str, pa.DataType]] = None
) -> Iterator[pd.DataFrame]:
    while True:
        records = cursor.fetchmany(chunksize)
        if not records:
            break
        df: pd.DataFrame = _records2df(records=records, cols_names=cursor.keys(), index=index, dtype=dtype)
        yield df


def _records2df(
    records: List[Tuple[Any]],
    cols_names: List[str],
    index: Optional[Union[str, List[str]]],
    dtype: Optional[Dict[str, pa.DataType]] = None,
) -> pd.DataFrame:
    arrays: List[pa.Array] = []
    for col_values, col_name in zip(tuple(zip(*records)), cols_names):  # Transposing
        if (dtype is None) or (col_name not in dtype):
            array: pa.Array = pa.array(obj=col_values, safe=True)  # Creating Arrow array
        else:
            array = pa.array(obj=col_values, type=dtype[col_name], safe=True)  # Creating Arrow array with dtype
        arrays.append(array)
    table = pa.Table.from_arrays(arrays=arrays, names=cols_names)  # Creating arrow Table
    df: pd.DataFrame = table.to_pandas(  # Creating Pandas DataFrame
        use_threads=True,
        split_blocks=True,
        self_destruct=True,
        integer_object_nulls=False,
        date_as_object=True,
        types_mapper=_data_types.pyarrow2pandas_extension,
    )
    if index is not None:
        df.set_index(index, inplace=True)
    return df


def _convert_params(sql: str, params: Optional[Union[List, Tuple, Dict]]) -> List[Any]:
    args: List[Any] = [sql]
    if params is not None:
        if hasattr(params, "keys"):
            return args + [params]
        return args + [list(params)]
    return args


def read_sql_table(
    table: str,
    con: sqlalchemy.engine.Engine,
    schema: Optional[str] = None,
    index_col: Optional[Union[str, List[str]]] = None,
    params: Optional[Union[List, Tuple, Dict]] = None,
    chunksize: Optional[int] = None,
    dtype: Optional[Dict[str, pa.DataType]] = None,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Return a DataFrame corresponding to the result set of the query string.

    Support for **Redshift**, **PostgreSQL** and **MySQL**.

    Note
    ----
    Redshift: For large extractions (1MM+ rows) consider the function `wr.db.unload_redshift()`.

    Parameters
    ----------
    table : str
        Nable name.
    con : sqlalchemy.engine.Engine
        SQLAlchemy Engine. Please use,
        wr.db.get_engine(), wr.db.get_redshift_temp_engine() or wr.catalog.get_engine()
    schema : str, optional
        Name of SQL schema in database to query (if database flavor supports this).
        Uses default schema if None (default).
    index_col : Union[str, List[str]], optional
        Column(s) to set as index(MultiIndex).
    params :  Union[List, Tuple, Dict], optional
        List of parameters to pass to execute method.
        The syntax used to pass parameters is database driver dependent.
        Check your database driver documentation for which of the five syntax styles,
        described in PEP 249’s paramstyle, is supported.
        Eg. for psycopg2, uses %(name)s so use params={‘name’ : ‘value’}.
    chunksize : int, optional
        If specified, return an iterator where chunksize is the number of rows to include in each chunk.
    dtype : Dict[str, pyarrow.DataType], optional
        Specifying the datatype for columns.
        The keys should be the column names and the values should be the PyArrow types.

    Returns
    -------
    Union[pandas.DataFrame, Iterator[pandas.DataFrame]]
        Result as Pandas DataFrame(s).

    Examples
    --------
    Reading from Redshift with temporary credentials

    >>> import awswrangler as wr
    >>> df = wr.db.read_sql_table(
    ...     table="my_table",
    ...     schema="public",
    ...     con=wr.db.get_redshift_temp_engine(cluster_identifier="...", user="...")
    ... )

    Reading from Redshift from Glue Catalog Connections

    >>> import awswrangler as wr
    >>> df = wr.db.read_sql_table(
    ...     table="my_table",
    ...     schema="public",
    ...     con=wr.catalog.get_engine(connection="...")
    ... )

    """
    if schema is None:
        sql: str = f"SELECT * FROM {table}"
    else:
        sql = f"SELECT * FROM {schema}.{table}"
    return read_sql_query(sql=sql, con=con, index_col=index_col, params=params, chunksize=chunksize, dtype=dtype)


def get_redshift_temp_engine(
    cluster_identifier: str,
    user: str,
    database: Optional[str] = None,
    duration: int = 900,
    boto3_session: Optional[boto3.Session] = None,
) -> sqlalchemy.engine.Engine:
    """Get Glue connection details.

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
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    sqlalchemy.engine.Engine
        SQLAlchemy Engine.

    Examples
    --------
    >>> import awswrangler as wr
    >>> engine = wr.db.get_redshift_temp_engine('my_cluster', 'my_user')

    """
    client_redshift: boto3.client = _utils.client(service_name="redshift", session=boto3_session)
    res: Dict[str, Any] = client_redshift.get_cluster_credentials(
        DbUser=user, ClusterIdentifier=cluster_identifier, DurationSeconds=duration, AutoCreate=False
    )
    _user: str = quote_plus(res["DbUser"])
    password: str = quote_plus(res["DbPassword"])
    cluster: Dict[str, Any] = client_redshift.describe_clusters(ClusterIdentifier=cluster_identifier)["Clusters"][0]
    host: str = cluster["Endpoint"]["Address"]
    port: str = cluster["Endpoint"]["Port"]
    if database is None:
        database = cluster["DBName"]
    conn_str: str = f"redshift+psycopg2://{_user}:{password}@{host}:{port}/{database}"
    return sqlalchemy.create_engine(
        conn_str, echo=False, executemany_mode="values", executemany_values_page_size=100_000
    )


def get_engine(db_type: str, host: str, port: int, database: str, user: str, password: str) -> sqlalchemy.engine.Engine:
    """Return a SQLAlchemy Engine from the given arguments.

    Only Redshift, PostgreSQL and MySQL are supported.

    Parameters
    ----------
    db_type : str
        Database type: "redshift", "mysql" or "postgresql".
    host : str
        Host address.
    port : str
        Port number.
    database : str
        Database name.
    user : str
        Username.
    password : str
        Password.

    Returns
    -------
    sqlalchemy.engine.Engine
        SQLAlchemy Engine.

    Examples
    --------
    >>> import awswrangler as wr
    >>> engine = wr.db.get_engine(
    ...     db_type="postgresql",
    ...     host="...",
    ...     port=1234,
    ...     database="...",
    ...     user="...",
    ...     password="..."
    ... )

    """
    if db_type == "postgresql":
        _utils.ensure_postgresql_casts()
    if db_type in ("redshift", "postgresql"):
        conn_str: str = f"{db_type}+psycopg2://{user}:{password}@{host}:{port}/{database}"
        return sqlalchemy.create_engine(
            conn_str, echo=False, executemany_mode="values", executemany_values_page_size=100_000
        )
    if db_type == "mysql":
        conn_str = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        return sqlalchemy.create_engine(conn_str, echo=False)
    raise exceptions.InvalidDatabaseType(  # pragma: no cover
        f"{db_type} is not a valid Database type." f" Only Redshift, PostgreSQL and MySQL are supported."
    )


def copy_to_redshift(  # pylint: disable=too-many-arguments
    df: pd.DataFrame,
    path: str,
    con: sqlalchemy.engine.Engine,
    table: str,
    schema: str,
    iam_role: str,
    index: bool = False,
    dtype: Optional[Dict[str, str]] = None,
    mode: str = "append",
    diststyle: str = "AUTO",
    distkey: Optional[str] = None,
    sortstyle: str = "COMPOUND",
    sortkey: Optional[str] = None,
    primary_keys: Optional[List[str]] = None,
    varchar_lengths_default: int = 256,
    varchar_lengths: Optional[Dict[str, int]] = None,
    keep_files: bool = False,
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
) -> None:
    """Load Pandas DataFrame as a Table on Amazon Redshift using parquet files on S3 as stage.

    This is a **HIGH** latency and **HIGH** throughput alternative to `wr.db.to_sql()` to load large
    DataFrames into Amazon Redshift through the ** SQL COPY command**.

    This strategy has more overhead and requires more IAM privileges
    than the regular `wr.db.to_sql()` function, so it is only recommended
    to inserting +1MM rows at once.

    https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html

    Note
    ----
    If the table does not exist yet,
    it will be automatically created for you
    using the Parquet metadata to
    infer the columns data types.

    Note
    ----
    In case of `use_threads=True` the number of process that will be spawned will be get from os.cpu_count().

    Parameters
    ----------
    df: pandas.DataFrame
        Pandas DataFrame.
    path : Union[str, List[str]]
        S3 path to write stage files (e.g. s3://bucket_name/any_name/)
    con : sqlalchemy.engine.Engine
        SQLAlchemy Engine. Please use,
        wr.db.get_engine(), wr.db.get_redshift_temp_engine() or wr.catalog.get_engine()
    table : str
        Table name
    schema : str
        Schema name
    iam_role : str
        AWS IAM role with the related permissions.
    index : bool
        True to store the DataFrame index in file, otherwise False to ignore it.
    dtype: Dict[str, str], optional
        Dictionary of columns names and Athena/Glue types to be casted.
        Useful when you have columns with undetermined or mixed data types.
        Only takes effect if dataset=True.
        (e.g. {'col name': 'bigint', 'col2 name': 'int'})
    mode : str
        Append, overwrite or upsert.
    diststyle : str
        Redshift distribution styles. Must be in ["AUTO", "EVEN", "ALL", "KEY"].
        https://docs.aws.amazon.com/redshift/latest/dg/t_Distributing_data.html
    distkey : str, optional
        Specifies a column name or positional number for the distribution key.
    sortstyle : str
        Sorting can be "COMPOUND" or "INTERLEAVED".
        https://docs.aws.amazon.com/redshift/latest/dg/t_Sorting_data.html
    sortkey : str, optional
        List of columns to be sorted.
    primary_keys : List[str], optional
        Primary keys.
    varchar_lengths_default : int
        The size that will be set for all VARCHAR columns not specified with varchar_lengths.
    varchar_lengths : Dict[str, int], optional
        Dict of VARCHAR length by columns. (e.g. {"col1": 10, "col5": 200}).
    keep_files : bool
        Should keep the stage files?
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    s3_additional_kwargs:
        Forward to s3fs, useful for server side encryption
        https://s3fs.readthedocs.io/en/latest/#serverside-encryption
    Returns
    -------
    None
        None.

    Examples
    --------
    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.db.copy_to_redshift(
    ...     df=pd.DataFrame({'col': [1, 2, 3]}),
    ...     path="s3://bucket/my_parquet_files/",
    ...     con=wr.catalog.get_engine(connection="my_glue_conn_name"),
    ...     table="my_table",
    ...     schema="public"
    ...     iam_role="arn:aws:iam::XXX:role/XXX"
    ... )

    """
    path = path if path.endswith("/") else f"{path}/"
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    paths: List[str] = s3.to_parquet(  # type: ignore
        df=df,
        path=path,
        index=index,
        dataset=True,
        mode="append",
        dtype=dtype,
        use_threads=use_threads,
        boto3_session=session,
        s3_additional_kwargs=s3_additional_kwargs,
    )["paths"]
    s3.wait_objects_exist(paths=paths, use_threads=False, boto3_session=session)
    copy_files_to_redshift(
        path=paths,
        manifest_directory=_utils.get_directory(path=path),
        con=con,
        table=table,
        schema=schema,
        iam_role=iam_role,
        mode=mode,
        diststyle=diststyle,
        distkey=distkey,
        sortstyle=sortstyle,
        sortkey=sortkey,
        primary_keys=primary_keys,
        varchar_lengths_default=varchar_lengths_default,
        varchar_lengths=varchar_lengths,
        use_threads=use_threads,
        boto3_session=session,
    )
    if keep_files is False:
        s3.delete_objects(path=paths, use_threads=use_threads, boto3_session=session)


def copy_files_to_redshift(  # pylint: disable=too-many-locals,too-many-arguments
    path: Union[str, List[str]],
    manifest_directory: str,
    con: sqlalchemy.engine.Engine,
    table: str,
    schema: str,
    iam_role: str,
    mode: str = "append",
    diststyle: str = "AUTO",
    distkey: Optional[str] = None,
    sortstyle: str = "COMPOUND",
    sortkey: Optional[str] = None,
    primary_keys: Optional[List[str]] = None,
    varchar_lengths_default: int = 256,
    varchar_lengths: Optional[Dict[str, int]] = None,
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
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
    In case of `use_threads=True` the number of process that will be spawned will be get from os.cpu_count().

    Parameters
    ----------
    path : Union[str, List[str]]
        S3 prefix (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
    manifest_directory : str
        S3 prefix (e.g. s3://bucket/prefix)
    con : sqlalchemy.engine.Engine
        SQLAlchemy Engine. Please use,
        wr.db.get_engine(), wr.db.get_redshift_temp_engine() or wr.catalog.get_engine()
    table : str
        Table name
    schema : str
        Schema name
    iam_role : str
        AWS IAM role with the related permissions.
    mode : str
        Append, overwrite or upsert.
    diststyle : str
        Redshift distribution styles. Must be in ["AUTO", "EVEN", "ALL", "KEY"].
        https://docs.aws.amazon.com/redshift/latest/dg/t_Distributing_data.html
    distkey : str, optional
        Specifies a column name or positional number for the distribution key.
    sortstyle : str
        Sorting can be "COMPOUND" or "INTERLEAVED".
        https://docs.aws.amazon.com/redshift/latest/dg/t_Sorting_data.html
    sortkey : str, optional
        List of columns to be sorted.
    primary_keys : List[str], optional
        Primary keys.
    varchar_lengths_default : int
        The size that will be set for all VARCHAR columns not specified with varchar_lengths.
    varchar_lengths : Dict[str, int], optional
        Dict of VARCHAR length by columns. (e.g. {"col1": 10, "col5": 200}).
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    None
        None.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.db.copy_files_to_redshift(
    ...     path="s3://bucket/my_parquet_files/",
    ...     con=wr.catalog.get_engine(connection="my_glue_conn_name"),
    ...     table="my_table",
    ...     schema="public"
    ...     iam_role="arn:aws:iam::XXX:role/XXX"
    ... )

    """
    _varchar_lengths: Dict[str, int] = {} if varchar_lengths is None else varchar_lengths
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    paths: List[str] = s3._path2list(path=path, boto3_session=session)  # pylint: disable=protected-access
    manifest_directory = manifest_directory if manifest_directory.endswith("/") else f"{manifest_directory}/"
    manifest_path: str = f"{manifest_directory}manifest.json"
    write_redshift_copy_manifest(
        manifest_path=manifest_path, paths=paths, use_threads=use_threads, boto3_session=session
    )
    s3.wait_objects_exist(paths=paths + [manifest_path], use_threads=False, boto3_session=session)
    athena_types, _ = s3.read_parquet_metadata(
        path=paths, dataset=False, use_threads=use_threads, boto3_session=session
    )
    _logger.debug(f"athena_types: {athena_types}")
    redshift_types: Dict[str, str] = {}
    for col_name, col_type in athena_types.items():
        length: int = _varchar_lengths[col_name] if col_name in _varchar_lengths else varchar_lengths_default
        redshift_types[col_name] = _data_types.athena2redshift(dtype=col_type, varchar_length=length)
    with con.begin() as _con:
        created_table, created_schema = _rs_create_table(
            con=_con,
            table=table,
            schema=schema,
            redshift_types=redshift_types,
            mode=mode,
            diststyle=diststyle,
            sortstyle=sortstyle,
            distkey=distkey,
            sortkey=sortkey,
            primary_keys=primary_keys,
        )
        _rs_copy(
            con=_con,
            table=created_table,
            schema=created_schema,
            manifest_path=manifest_path,
            iam_role=iam_role,
            num_files=len(paths),
        )
        if table != created_table:  # upsert
            _rs_upsert(con=_con, schema=schema, table=table, temp_table=created_table, primary_keys=primary_keys)
    s3.delete_objects(path=[manifest_path], use_threads=use_threads, boto3_session=session)


def _rs_upsert(con: Any, table: str, temp_table: str, schema: str, primary_keys: Optional[List[str]] = None) -> None:
    if not primary_keys:
        primary_keys = _rs_get_primary_keys(con=con, schema=schema, table=table)
    _logger.debug(f"primary_keys: {primary_keys}")
    if not primary_keys:  # pragma: no cover
        raise exceptions.InvalidRedshiftPrimaryKeys()
    equals_clause: str = f"{table}.%s = {temp_table}.%s"
    join_clause: str = " AND ".join([equals_clause % (pk, pk) for pk in primary_keys])
    sql: str = f"DELETE FROM {schema}.{table} USING {temp_table} WHERE {join_clause}"
    _logger.debug(sql)
    con.execute(sql)
    sql = f"INSERT INTO {schema}.{table} SELECT * FROM {temp_table}"
    _logger.debug(sql)
    con.execute(sql)
    _rs_drop_table(con=con, schema=schema, table=temp_table)


def _rs_create_table(
    con: Any,
    table: str,
    schema: str,
    mode: str,
    redshift_types: Dict[str, str],
    diststyle: str,
    sortstyle: str,
    distkey: Optional[str] = None,
    sortkey: Optional[str] = None,
    primary_keys: Optional[List[str]] = None,
) -> Tuple[str, Optional[str]]:
    if mode == "overwrite":
        _rs_drop_table(con=con, schema=schema, table=table)
    else:
        if _rs_does_table_exist(con=con, schema=schema, table=table) is True:
            if mode == "upsert":
                guid: str = pa.compat.guid()
                temp_table: str = f"temp_redshift_{guid}"
                sql: str = f"CREATE TEMPORARY TABLE {temp_table} (LIKE {schema}.{table})"
                _logger.debug(sql)
                con.execute(sql)
                return temp_table, None
            return table, schema
    diststyle = diststyle.upper() if diststyle else "AUTO"
    sortstyle = sortstyle.upper() if sortstyle else "COMPOUND"
    _rs_validate_parameters(
        redshift_types=redshift_types, diststyle=diststyle, distkey=distkey, sortstyle=sortstyle, sortkey=sortkey
    )
    cols_str: str = "".join([f"{k} {v},\n" for k, v in redshift_types.items()])[:-2]
    primary_keys_str: str = f",\nPRIMARY KEY ({', '.join(primary_keys)})" if primary_keys else ""
    distkey_str: str = f"\nDISTKEY({distkey})" if distkey and diststyle == "KEY" else ""
    sortkey_str: str = f"\n{sortstyle} SORTKEY({','.join(sortkey)})" if sortkey else ""
    sql = (
        f"CREATE TABLE IF NOT EXISTS {schema}.{table} (\n"
        f"{cols_str}"
        f"{primary_keys_str}"
        f")\nDISTSTYLE {diststyle}"
        f"{distkey_str}"
        f"{sortkey_str}"
    )
    _logger.debug(f"Create table query:\n{sql}")
    con.execute(sql)
    return table, schema


def _rs_validate_parameters(
    redshift_types: Dict[str, str], diststyle: str, distkey: Optional[str], sortstyle: str, sortkey: Optional[str]
) -> None:
    if diststyle not in _RS_DISTSTYLES:
        raise exceptions.InvalidRedshiftDiststyle(f"diststyle must be in {_RS_DISTSTYLES}")
    cols = list(redshift_types.keys())
    _logger.debug(f"Redshift columns: {cols}")
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


def _rs_copy(
    con: Any, table: str, manifest_path: str, iam_role: str, num_files: int, schema: Optional[str] = None
) -> int:
    if schema is None:
        table_name: str = table
    else:
        table_name = f"{schema}.{table}"
    sql: str = (
        f"COPY {table_name} FROM '{manifest_path}'\n" f"IAM_ROLE '{iam_role}'\n" "MANIFEST\n" "FORMAT AS PARQUET"
    )
    _logger.debug(f"copy query:\n{sql}")
    con.execute(sql)
    sql = "SELECT pg_last_copy_id() AS query_id"
    query_id: int = con.execute(sql).fetchall()[0][0]
    sql = f"SELECT COUNT(DISTINCT filename) as num_files_loaded " f"FROM STL_LOAD_COMMITS WHERE query = {query_id}"
    num_files_loaded: int = con.execute(sql).fetchall()[0][0]
    _logger.debug(f"{num_files_loaded} files counted. {num_files} expected.")
    if num_files_loaded != num_files:  # pragma: no cover
        raise exceptions.RedshiftLoadError(
            f"Redshift load rollbacked. {num_files_loaded} files counted. {num_files} expected."
        )
    return num_files_loaded


def write_redshift_copy_manifest(
    manifest_path: str, paths: List[str], use_threads: bool = True, boto3_session: Optional[boto3.Session] = None
) -> Dict[str, List[Dict[str, Union[str, bool, Dict[str, int]]]]]:
    """Write Redshift copy manifest and return its structure.

    Only Parquet files are supported.

    Note
    ----
    In case of `use_threads=True` the number of process that will be spawned will be get from os.cpu_count().

    Parameters
    ----------
    manifest_path : str
        Amazon S3 manifest path (e.g. s3://...)
    paths: List[str]
        List of S3 paths (Parquet Files) to be copied.
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Dict[str, List[Dict[str, Union[str, bool, Dict[str, int]]]]]
        Manifest content.

    Examples
    --------
    Copying two files to Redshift cluster.

    >>> import awswrangler as wr
    >>> wr.db.write_redshift_copy_manifest(
    ...     path="s3://bucket/my.manifest",
    ...     paths=["s3://...parquet", "s3://...parquet"]
    ... )

    """
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    objects_sizes: Dict[str, Optional[int]] = s3.size_objects(
        path=paths, use_threads=use_threads, boto3_session=session
    )
    manifest: Dict[str, List[Dict[str, Union[str, bool, Dict[str, int]]]]] = {"entries": []}
    path: str
    size: Optional[int]
    for path, size in objects_sizes.items():
        if size is not None:
            entry: Dict[str, Union[str, bool, Dict[str, int]]] = {
                "url": path,
                "mandatory": True,
                "meta": {"content_length": size},
            }
            manifest["entries"].append(entry)
    payload: str = json.dumps(manifest)
    bucket: str
    bucket, key = _utils.parse_path(manifest_path)
    _logger.debug(f"payload: {payload}")
    client_s3: boto3.client = _utils.client(service_name="s3", session=session)
    _logger.debug(f"bucket: {bucket}")
    _logger.debug(f"key: {key}")
    client_s3.put_object(Body=payload, Bucket=bucket, Key=key)
    return manifest


def _rs_drop_table(con: Any, schema: str, table: str) -> None:
    sql = f"DROP TABLE IF EXISTS {schema}.{table}"
    _logger.debug(f"Drop table query:\n{sql}")
    con.execute(sql)


def _rs_get_primary_keys(con: Any, schema: str, table: str) -> List[str]:
    cursor: Any = con.execute(
        f"SELECT indexdef FROM pg_indexes WHERE schemaname = '{schema}' AND tablename = '{table}'"
    )
    result: str = cursor.fetchall()[0][0]
    rfields: List[str] = result.split("(")[1].strip(")").split(",")
    fields: List[str] = [field.strip().strip('"') for field in rfields]
    return fields


def _rs_does_table_exist(con: Any, schema: str, table: str) -> bool:
    cursor = con.execute(
        f"SELECT true WHERE EXISTS ("
        f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE "
        f"TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'"
        f");"
    )
    if len(cursor.fetchall()) > 0:
        return True
    return False  # pragma: no cover


def unload_redshift(
    sql: str,
    path: str,
    con: sqlalchemy.engine.Engine,
    iam_role: str,
    categories: List[str] = None,
    chunked: bool = False,
    keep_files: bool = False,
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Load Pandas DataFrame from a Amazon Redshift query result using Parquet files on s3 as stage.

    This is a **HIGH** latency and **HIGH** throughput alternative to
    `wr.db.read_sql_query()`/`wr.db.read_sql_table()` to extract large
    Amazon Redshift data into a Pandas DataFrames through the **UNLOAD command**.

    This strategy has more overhead and requires more IAM privileges
    than the regular `wr.db.read_sql_query()`/`wr.db.read_sql_table()` function,
    so it is only recommended to fetch +1MM rows at once.

    https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html

    Note
    ----
    In case of `use_threads=True` the number of process that will be spawned will be get from os.cpu_count().

    Parameters
    ----------
    sql: str
        SQL query.
    path : Union[str, List[str]]
        S3 path to write stage files (e.g. s3://bucket_name/any_name/)
    con : sqlalchemy.engine.Engine
        SQLAlchemy Engine. Please use,
        wr.db.get_engine(), wr.db.get_redshift_temp_engine() or wr.catalog.get_engine()
    iam_role : str
        AWS IAM role with the related permissions.
    categories: List[str], optional
        List of columns names that should be returned as pandas.Categorical.
        Recommended for memory restricted environments.
    keep_files : bool
        Should keep the stage files?
    chunked : bool
        If True will break the data in smaller DataFrames (Non deterministic number of lines).
        Otherwise return a single DataFrame with the whole data.
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    s3_additional_kwargs:
        Forward to s3fs, useful for server side encryption
        https://s3fs.readthedocs.io/en/latest/#serverside-encryption
    Returns
    -------
    pandas.DataFrame
        Pandas DataFrame

    Examples
    --------
    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> df = wr.db.unload_redshift(
    ...     sql="SELECT * FROM public.mytable",
    ...     path="s3://bucket/extracted_parquet_files/",
    ...     con=wr.catalog.get_engine(connection="my_glue_connection"),
    ...     iam_role="arn:aws:iam::XXX:role/XXX"
    ... )

    """
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    paths: List[str] = unload_redshift_to_files(
        sql=sql, path=path, con=con, iam_role=iam_role, use_threads=use_threads, boto3_session=session
    )
    s3.wait_objects_exist(paths=paths, use_threads=False, boto3_session=session)
    if chunked is False:
        if not paths:  # pragma: no cover
            return pd.DataFrame()
        df: pd.DataFrame = s3.read_parquet(
            path=paths,
            categories=categories,
            chunked=chunked,
            dataset=False,
            use_threads=use_threads,
            boto3_session=session,
            s3_additional_kwargs=s3_additional_kwargs,
        )
        if keep_files is False:
            s3.delete_objects(path=paths, use_threads=use_threads, boto3_session=session)
        return df
    if not paths:  # pragma: no cover
        return _utils.empty_generator()
    return _read_parquet_iterator(
        paths=paths,
        categories=categories,
        use_threads=use_threads,
        boto3_session=session,
        s3_additional_kwargs=s3_additional_kwargs,
        keep_files=keep_files,
    )


def _read_parquet_iterator(
    paths: List[str],
    keep_files: bool,
    use_threads: bool,
    categories: List[str] = None,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
) -> Iterator[pd.DataFrame]:
    dfs: Iterator[pd.DataFrame] = s3.read_parquet(
        path=paths,
        categories=categories,
        chunked=True,
        dataset=False,
        use_threads=use_threads,
        boto3_session=boto3_session,
        s3_additional_kwargs=s3_additional_kwargs,
    )
    yield from dfs
    if keep_files is False:
        s3.delete_objects(path=paths, use_threads=use_threads, boto3_session=boto3_session)


def unload_redshift_to_files(
    sql: str,
    path: str,
    con: sqlalchemy.engine.Engine,
    iam_role: str,
    use_threads: bool = True,
    manifest: bool = False,
    partition_cols: Optional[List] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> List[str]:
    """Unload Parquet files from a Amazon Redshift query result to parquet files on s3 (Through UNLOAD command).

    https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html

    Note
    ----
    In case of `use_threads=True` the number of process that will be spawned will be get from os.cpu_count().

    Parameters
    ----------
    sql: str
        SQL query.
    path : Union[str, List[str]]
        S3 path to write stage files (e.g. s3://bucket_name/any_name/)
    con : sqlalchemy.engine.Engine
        SQLAlchemy Engine. Please use,
        wr.db.get_engine(), wr.db.get_redshift_temp_engine() or wr.catalog.get_engine()
    iam_role : str
        AWS IAM role with the related permissions.
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
    manifest : bool
        Unload a manifest file on S3.
    partition_cols: List[str], optional
        Specifies the partition keys for the unload operation.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    List[str]
        Paths list with all unloaded files.

    Examples
    --------
    >>> import awswrangler as wr
    >>> paths = wr.db.unload_redshift_to_files(
    ...     sql="SELECT * FROM public.mytable",
    ...     path="s3://bucket/extracted_parquet_files/",
    ...     con=wr.catalog.get_engine(connection="my_glue_connection"),
    ...     iam_role="arn:aws:iam::XXX:role/XXX"
    ... )

    """
    path = path if path.endswith("/") else f"{path}/"
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    s3.delete_objects(path=path, use_threads=use_threads, boto3_session=session)
    with con.connect() as _con:
        partition_str: str = f"PARTITION BY ({','.join(partition_cols)})\n" if partition_cols else ""
        manifest_str: str = "\nmanifest" if manifest is True else ""
        sql = (
            f"UNLOAD ('{sql}')\n"
            f"TO '{path}'\n"
            f"IAM_ROLE '{iam_role}'\n"
            "ALLOWOVERWRITE\n"
            "PARALLEL ON\n"
            "ENCRYPTED\n"
            f"{partition_str}"
            "FORMAT PARQUET"
            f"{manifest_str};"
        )
        _con.execute(sql)
        sql = "SELECT pg_last_query_id() AS query_id"
        query_id: int = _con.execute(sql).fetchall()[0][0]
        sql = f"SELECT path FROM STL_UNLOAD_LOG WHERE query={query_id};"
        paths = [x[0].replace(" ", "") for x in _con.execute(sql).fetchall()]
        _logger.debug(f"paths: {paths}")
        return paths
