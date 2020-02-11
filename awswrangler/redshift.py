"""Amazon Redshift Module."""

import json
from logging import Logger, getLogger
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

import pg8000  # type: ignore
import pyarrow as pa  # type: ignore
from boto3 import client  # type: ignore

from awswrangler import data_types
from awswrangler.exceptions import (InvalidDataframeType, InvalidRedshiftDistkey, InvalidRedshiftDiststyle,
                                    InvalidRedshiftPrimaryKeys, InvalidRedshiftSortkey, InvalidRedshiftSortstyle,
                                    RedshiftLoadError)

if TYPE_CHECKING:
    from awswrangler.session import Session

logger: Logger = getLogger(__name__)

DISTSTYLES = [
    "AUTO",
    "EVEN",
    "ALL",
    "KEY",
]

SORTSTYLES = [
    "COMPOUND",
    "INTERLEAVED",
]


class Redshift:
    """Amazon Redshift Class."""
    def __init__(self, session: "Session"):
        """
        Amazon Redshift Class Constructor.

        Don't use it directly, call through a Session().
        e.g. wr.redshift.your_method()

        :param session: awswrangler.Session()
        """
        self._session: "Session" = session
        self._client_s3: client = session.boto3_session.client(service_name="s3",
                                                               use_ssl=True,
                                                               config=session.botocore_config)

    @staticmethod
    def _validate_connection(database,
                             host,
                             port,
                             user,
                             password,
                             tcp_keepalive=True,
                             application_name="aws-data-wrangler-validation",
                             validation_timeout=10):
        conn = pg8000.connect(database=database,
                              host=host,
                              port=int(port),
                              user=user,
                              password=password,
                              ssl=True,
                              application_name=application_name,
                              tcp_keepalive=tcp_keepalive,
                              timeout=validation_timeout)
        conn.close()

    @staticmethod
    def generate_connection(database,
                            host,
                            port,
                            user,
                            password,
                            tcp_keepalive: bool = True,
                            application_name: Optional[str] = "aws-data-wrangler",
                            connection_timeout: Optional[int] = None,
                            statement_timeout: Optional[int] = None,
                            validation_timeout: int = 10):
        """
        Generate a valid connection object to be passed to the load_table method.

        :param database: The name of the database instance to connect with.
        :param host: The hostname of the Redshift server to connect with.
        :param port: The TCP/IP port of the Redshift server instance.
        :param user: The username to connect to the Redshift server with.
        :param password: The user password to connect to the server with.
        :param tcp_keepalive: If True then use TCP keepalive
        :param application_name: Application name
        :param connection_timeout: Connection Timeout (seconds)
        :param statement_timeout: Redshift statements timeout (milliseconds)
        :param validation_timeout: Timeout to try to validate the connection (seconds)
        :return: pg8000 connection
        """
        Redshift._validate_connection(database=database,
                                      host=host,
                                      port=port,
                                      user=user,
                                      password=password,
                                      tcp_keepalive=tcp_keepalive,
                                      application_name=application_name,
                                      validation_timeout=validation_timeout)
        conn = pg8000.connect(database=database,
                              host=host,
                              port=int(port),
                              user=user,
                              password=password,
                              ssl=True,
                              application_name=application_name,
                              tcp_keepalive=tcp_keepalive,
                              timeout=connection_timeout)
        if statement_timeout is not None:
            cursor = conn.cursor()
            cursor.execute(f"set statement_timeout = {statement_timeout}")
            conn.commit()
            cursor.close()
        return conn

    def get_connection(self, glue_connection: str):
        """
        Get a Glue connection as a PG8000 connection.

        :param glue_connection: Glue connection name
        :return: pg8000 connection
        """
        conn_details: dict = self._session.glue.get_connection_details(name=glue_connection)
        props: dict = conn_details["ConnectionProperties"]
        host: str = props["JDBC_CONNECTION_URL"].split(":")[2].replace("/", "")
        port, database = props["JDBC_CONNECTION_URL"].split(":")[3].split("/")
        user: str = props["USERNAME"]
        password: str = props["PASSWORD"]
        conn = self.generate_connection(database=database, host=host, port=int(port), user=user, password=password)
        return conn

    def write_load_manifest(
            self,
            manifest_path: str,
            objects_paths: List[str],
            procs_io_bound: Optional[int] = None) -> Dict[str, List[Dict[str, Union[str, bool, Dict[str, int]]]]]:
        """
        Write Redshift load manifest.

        :param manifest_path: Amazon S3 manifest path (e.g. s3://...)
        :param objects_paths: List of S3 paths
        :param procs_io_bound: Number of processes to be used on I/O bound operations
        :return: Manifest content
        """
        objects_sizes: Dict[str, int] = self._session.s3.get_objects_sizes(objects_paths=objects_paths,
                                                                           procs_io_bound=procs_io_bound)
        manifest: Dict[str, List[Dict[str, Union[str, bool, Dict[str, int]]]]] = {"entries": []}
        path: str
        size: int
        for path, size in objects_sizes.items():
            entry: Dict[str, Union[str, bool, Dict[str, int]]] = {
                "url": path,
                "mandatory": True,
                "meta": {
                    "content_length": size
                }
            }
            manifest["entries"].append(entry)
        payload: str = json.dumps(manifest)
        bucket: str
        bucket, path = manifest_path.replace("s3://", "").split("/", 1)
        logger.info(f"payload: {payload}")
        self._client_s3.put_object(Body=payload, Bucket=bucket, Key=path)
        return manifest

    @staticmethod
    def get_number_of_slices(redshift_conn) -> int:
        """
        Get the number of slices in the Redshift cluster.

        :param redshift_conn: Redshift connection (PEP 249 compatible)
        :return: Number of slices in the Redshift CLuster
        """
        cursor = redshift_conn.cursor()
        cursor.execute("SELECT COUNT(*) as count_slices FROM (SELECT DISTINCT node, slice from STV_SLICES)")
        count_slices: int = cursor.fetchall()[0][0]
        cursor.close()
        return count_slices

    @staticmethod
    def load_table(dataframe,
                   dataframe_type,
                   manifest_path,
                   schema_name,
                   table_name,
                   redshift_conn,
                   num_files,
                   iam_role,
                   diststyle="AUTO",
                   distkey=None,
                   sortstyle="COMPOUND",
                   sortkey=None,
                   primary_keys: Optional[List[str]] = None,
                   mode="append",
                   preserve_index=False,
                   cast_columns=None,
                   varchar_default_length: int = 256,
                   varchar_lengths: Optional[Dict[str, int]] = None):
        """
        Load Parquet files into a Redshift table using a manifest file and create the table if necessary.

        :param dataframe: Pandas or Spark Dataframe
        :param dataframe_type: "pandas" or "spark"
        :param manifest_path: S3 path for manifest file (E.g. S3://...)
        :param schema_name: Redshift schema
        :param table_name: Redshift table name
        :param redshift_conn: A PEP 249 compatible connection (Can be generated with Redshift.generate_connection())
        :param num_files: Number of files to be loaded
        :param iam_role: AWS IAM role with the related permissions
        :param diststyle: Redshift distribution styles. Must be in ["AUTO", "EVEN", "ALL", "KEY"] (https://docs.aws.amazon.com/redshift/latest/dg/t_Distributing_data.html)
        :param distkey: Specifies a column name or positional number for the distribution key
        :param sortstyle: Sorting can be "COMPOUND" or "INTERLEAVED" (https://docs.aws.amazon.com/redshift/latest/dg/t_Sorting_data.html)
        :param sortkey: List of columns to be sorted
        :param primary_keys: Primary keys
        :param mode: append, overwrite or upsert
        :param preserve_index: Should we preserve the Dataframe index? (ONLY for Pandas Dataframe)
        :param cast_columns: Dictionary of columns names and Redshift types to be casted. (E.g. {"col name": "INT", "col2 name": "FLOAT"})
        :param varchar_default_length: The size that will be set for all VARCHAR columns not specified with varchar_lengths
        :param varchar_lengths: Dict of VARCHAR length by columns. (e.g. {"col1": 10, "col5": 200})
        :return: None
        """
        final_table_name: Optional[str] = None
        temp_table_name: Optional[str] = None
        with redshift_conn.cursor() as cursor:
            if mode == "overwrite":
                Redshift._create_table(cursor=cursor,
                                       dataframe=dataframe,
                                       dataframe_type=dataframe_type,
                                       schema_name=schema_name,
                                       table_name=table_name,
                                       diststyle=diststyle,
                                       distkey=distkey,
                                       sortstyle=sortstyle,
                                       sortkey=sortkey,
                                       primary_keys=primary_keys,
                                       preserve_index=preserve_index,
                                       cast_columns=cast_columns,
                                       varchar_default_length=varchar_default_length,
                                       varchar_lengths=varchar_lengths)
                table_name = f"{schema_name}.{table_name}"
            elif mode == "upsert":
                guid: str = pa.compat.guid()
                temp_table_name = f"temp_redshift_{guid}"
                final_table_name = table_name
                table_name = temp_table_name
                sql: str = f"CREATE TEMPORARY TABLE {temp_table_name} (LIKE {schema_name}.{final_table_name})"
                logger.debug(sql)
                cursor.execute(sql)
            else:
                table_name = f"{schema_name}.{table_name}"

            sql = ("-- AWS DATA WRANGLER\n"
                   f"COPY {table_name} FROM '{manifest_path}'\n"
                   f"IAM_ROLE '{iam_role}'\n"
                   "MANIFEST\n"
                   "FORMAT AS PARQUET")
            logger.debug(sql)
            cursor.execute(sql)
            cursor.execute("-- AWS DATA WRANGLER\n SELECT pg_last_copy_id() AS query_id")
            query_id = cursor.fetchall()[0][0]
            sql = ("-- AWS DATA WRANGLER\n"
                   f"SELECT COUNT(DISTINCT filename) as num_files_loaded "
                   f"FROM STL_LOAD_COMMITS "
                   f"WHERE query = {query_id}")
            logger.debug(sql)
            cursor.execute(sql)
            num_files_loaded = cursor.fetchall()[0][0]
            if num_files_loaded != num_files:
                redshift_conn.rollback()
                raise RedshiftLoadError(
                    f"Redshift load rollbacked. {num_files_loaded} files counted. {num_files} expected.")

            if (mode == "upsert") and (final_table_name is not None):
                if not primary_keys:
                    primary_keys = Redshift.get_primary_keys(connection=redshift_conn,
                                                             schema=schema_name,
                                                             table=final_table_name)
                if not primary_keys:
                    raise InvalidRedshiftPrimaryKeys()
                equals_clause = f"{final_table_name}.%s = {temp_table_name}.%s"
                join_clause = " AND ".join([equals_clause % (pk, pk) for pk in primary_keys])
                sql = f"DELETE FROM {schema_name}.{final_table_name} USING {temp_table_name} WHERE {join_clause}"
                logger.debug(sql)
                cursor.execute(sql)
                sql = f"INSERT INTO {schema_name}.{final_table_name} SELECT * FROM {temp_table_name}"
                logger.debug(sql)
                cursor.execute(sql)

        redshift_conn.commit()

    @staticmethod
    def _create_table(cursor,
                      dataframe,
                      dataframe_type,
                      schema_name,
                      table_name,
                      diststyle="AUTO",
                      distkey=None,
                      sortstyle="COMPOUND",
                      sortkey=None,
                      primary_keys: List[str] = None,
                      preserve_index=False,
                      cast_columns=None,
                      varchar_default_length: int = 256,
                      varchar_lengths: Optional[Dict[str, int]] = None):
        """
        Create Redshift table.

        :param cursor: A PEP 249 compatible cursor
        :param dataframe: Pandas or Spark Dataframe
        :param dataframe_type: "pandas" or "spark"
        :param schema_name: Redshift schema
        :param table_name: Redshift table name
        :param diststyle: Redshift distribution styles. Must be in ["AUTO", "EVEN", "ALL", "KEY"] (https://docs.aws.amazon.com/redshift/latest/dg/t_Distributing_data.html)
        :param distkey: Specifies a column name or positional number for the distribution key
        :param sortstyle: Sorting can be "COMPOUND" or "INTERLEAVED" (https://docs.aws.amazon.com/redshift/latest/dg/t_Sorting_data.html)
        :param sortkey: List of columns to be sorted
        :param primary_keys: Primary keys
        :param preserve_index: Should we preserve the Dataframe index? (ONLY for Pandas Dataframe)
        :param cast_columns: Dictionary of columns names and Redshift types to be casted. (E.g. {"col name": "INT", "col2 name": "FLOAT"})
        :param varchar_default_length: The size that will be set for all VARCHAR columns not specified with varchar_lengths
        :param varchar_lengths: Dict of VARCHAR length by columns. (e.g. {"col1": 10, "col5": 200})
        :return: None
        """
        sql = f"-- AWS DATA WRANGLER\n" \
              f"DROP TABLE IF EXISTS {schema_name}.{table_name}"
        logger.debug(f"Drop table query:\n{sql}")
        cursor.execute(sql)
        schema = Redshift._get_redshift_schema(dataframe=dataframe,
                                               dataframe_type=dataframe_type,
                                               preserve_index=preserve_index,
                                               cast_columns=cast_columns,
                                               varchar_default_length=varchar_default_length,
                                               varchar_lengths=varchar_lengths)
        if diststyle:
            diststyle = diststyle.upper()
        else:
            diststyle = "AUTO"
        if sortstyle:
            sortstyle = sortstyle.upper()
        else:
            sortstyle = "COMPOUND"
        Redshift._validate_parameters(schema=schema,
                                      diststyle=diststyle,
                                      distkey=distkey,
                                      sortstyle=sortstyle,
                                      sortkey=sortkey)
        cols_str: str = "".join([f"{col[0]} {col[1]},\n" for col in schema])[:-2]
        primary_keys_str: str = ""
        if primary_keys:
            primary_keys_str = f",\nPRIMARY KEY ({', '.join(primary_keys)})"
        distkey_str: str = ""
        if distkey and diststyle == "KEY":
            distkey_str = f"\nDISTKEY({distkey})"
        sortkey_str: str = ""
        if sortkey:
            sortkey_str = f"\n{sortstyle} SORTKEY({','.join(sortkey)})"
        sql = (f"-- AWS DATA WRANGLER\n"
               f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (\n"
               f"{cols_str}"
               f"{primary_keys_str}"
               f")\nDISTSTYLE {diststyle}"
               f"{distkey_str}"
               f"{sortkey_str}")
        logger.debug(f"Create table query:\n{sql}")
        cursor.execute(sql)

    @staticmethod
    def get_primary_keys(connection, schema: str, table: str) -> List[str]:
        """
        Get PKs.

        :param connection: A PEP 249 compatible connection (Can be generated with Redshift.generate_connection())
        :param schema: Schema name
        :param table: Redshift table name
        :return: PKs list List[str]
        """
        cursor = connection.cursor()
        cursor.execute(f"SELECT indexdef FROM pg_indexes WHERE schemaname = '{schema}' AND tablename = '{table}'")
        result: str = cursor.fetchall()[0][0]
        rfields: List[str] = result.split('(')[1].strip(')').split(',')
        fields: List[str] = [field.strip().strip('"') for field in rfields]
        cursor.close()
        return fields

    @staticmethod
    def _validate_parameters(schema, diststyle, distkey, sortstyle, sortkey):
        """
        Validate the sanity of Redshift's parameters.

        :param schema: List of tuples (column name, column type)
        :param diststyle: Redshift distribution styles. Must be in ["AUTO", "EVEN", "ALL", "KEY"]
               https://docs.aws.amazon.com/redshift/latest/dg/t_Distributing_data.html
        :param distkey: Specifies a column name or positional number for the distribution key
        :param sortstyle: Sorting can be "COMPOUND" or "INTERLEAVED"
               https://docs.aws.amazon.com/redshift/latest/dg/t_Sorting_data.html
        :param sortkey: List of columns to be sorted
        :return: None
        """
        if diststyle not in DISTSTYLES:
            raise InvalidRedshiftDiststyle(f"diststyle must be in {DISTSTYLES}")
        cols = [x[0] for x in schema]
        logger.debug(f"Redshift columns: {cols}")
        if (diststyle == "KEY") and (not distkey):
            raise InvalidRedshiftDistkey("You must pass a distkey if you intend to use KEY diststyle")
        if distkey and distkey not in cols:
            raise InvalidRedshiftDistkey(f"distkey ({distkey}) must be in the columns list: {cols})")
        if sortstyle and sortstyle not in SORTSTYLES:
            raise InvalidRedshiftSortstyle(f"sortstyle must be in {SORTSTYLES}")
        if sortkey:
            if type(sortkey) != list:
                raise InvalidRedshiftSortkey(f"sortkey must be a List of items in the columns list: {cols}. "
                                             f"Currently value: {sortkey}")
            for key in sortkey:
                if key not in cols:
                    raise InvalidRedshiftSortkey(f"sortkey must be a List of items in the columns list: {cols}. "
                                                 f"Currently value: {key}")

    @staticmethod
    def _get_redshift_schema(dataframe,
                             dataframe_type: str,
                             preserve_index: bool = False,
                             cast_columns=None,
                             varchar_default_length: int = 256,
                             varchar_lengths: Optional[Dict[str, int]] = None) -> List[Tuple[str, str]]:
        cast_columns = {} if cast_columns is None else cast_columns
        varchar_lengths = {} if varchar_lengths is None else varchar_lengths
        schema_built: List[Tuple[str, str]] = []
        if dataframe_type.lower() == "pandas":
            pyarrow_schema = data_types.extract_pyarrow_schema_from_pandas(dataframe=dataframe,
                                                                           preserve_index=preserve_index,
                                                                           indexes_position="right")
            for name, dtype in pyarrow_schema:
                if (cast_columns is not None) and (name in cast_columns.keys()):
                    schema_built.append((name, cast_columns[name]))
                else:
                    varchar_len = varchar_lengths.get(name, varchar_default_length)
                    redshift_type = data_types.pyarrow2redshift(dtype=dtype, varchar_length=varchar_len)
                    schema_built.append((name, redshift_type))
        elif dataframe_type.lower() == "spark":
            logger.debug(f"cast_columns.keys: {cast_columns.keys()}")
            for name, dtype in dataframe.dtypes:
                varchar_len = varchar_lengths.get(name, varchar_default_length)
                if name in cast_columns.keys():
                    redshift_type = data_types.athena2redshift(dtype=cast_columns[name], varchar_length=varchar_len)
                else:
                    redshift_type = data_types.spark2redshift(dtype=dtype, varchar_length=varchar_len)
                schema_built.append((name, redshift_type))
        else:
            raise InvalidDataframeType(
                f"{dataframe_type} is not a valid DataFrame type. Please use 'pandas' or 'spark'!")
        return schema_built

    def to_parquet(self,
                   sql: str,
                   path: str,
                   iam_role: str,
                   connection: Any,
                   partition_cols: Optional[List] = None) -> List[str]:
        """
        Write a query result as parquet files on S3.

        :param sql: SQL Query
        :param path: AWS S3 path to write the data (e.g. s3://...)
        :param iam_role: AWS IAM role with the related permissions
        :param connection: A PEP 249 compatible connection (Can be generated with Redshift.generate_connection())
        :param partition_cols: Specifies the partition keys for the unload operation.
        """
        sql = sql.replace("'", "\'").replace(";", "")  # escaping single quote
        path = path if path[-1] == "/" else path + "/"
        cursor: Any = connection.cursor()
        partition_str: str = ""
        manifest_str: str = ""
        if partition_cols is not None:
            partition_str = f"PARTITION BY ({','.join([x for x in partition_cols])})\n"
        else:
            manifest_str = "\nmanifest"
        query: str = f"-- AWS DATA WRANGLER\n" \
                     f"UNLOAD ('{sql}')\n" \
                     f"TO '{path}'\n" \
                     f"IAM_ROLE '{iam_role}'\n" \
                     f"ALLOWOVERWRITE\n" \
                     f"PARALLEL ON\n" \
                     f"ENCRYPTED \n" \
                     f"{partition_str}" \
                     f"FORMAT PARQUET" \
                     f"{manifest_str};"
        logger.debug(f"query:\n{query}")
        cursor.execute(query)
        query = "-- AWS DATA WRANGLER\nSELECT pg_last_query_id() AS query_id"
        logger.debug(f"query:\n{query}")
        cursor.execute(query)
        query_id = cursor.fetchall()[0][0]
        query = f"-- AWS DATA WRANGLER\n" \
                f"SELECT path FROM STL_UNLOAD_LOG WHERE query={query_id};"
        logger.debug(f"query:\n{query}")
        cursor.execute(query)
        paths: List[str] = [row[0].replace(" ", "") for row in cursor.fetchall()]
        logger.debug(f"paths: {paths}")
        connection.commit()
        cursor.close()
        if paths:
            if manifest_str != "":
                logger.debug(f"Waiting manifest path: {f'{path}manifest'}")
                self._session.s3.wait_object_exists(path=f"{path}manifest", timeout=30.0)
            for p in paths:
                logger.debug(f"Waiting path: {p}")
                self._session.s3.wait_object_exists(path=p, timeout=30.0)
        return paths
