from typing import TYPE_CHECKING, Union, List, Dict, Tuple, Any, Optional
from logging import getLogger, Logger, INFO
import json
import warnings

import pg8000  # type: ignore
from pg8000 import ProgrammingError  # type: ignore
import pymysql  # type: ignore
import pandas as pd  # type: ignore
from boto3 import client  # type: ignore
import tenacity  # type: ignore

from awswrangler import data_types
from awswrangler.exceptions import InvalidEngine, InvalidDataframeType, AuroraLoadError

if TYPE_CHECKING:
    from awswrangler.session import Session

logger: Logger = getLogger(__name__)


class Aurora:
    def __init__(self, session: "Session"):
        self._session: "Session" = session
        self._client_s3: client = session.boto3_session.client(service_name="s3",
                                                               use_ssl=True,
                                                               config=session.botocore_config)

    @staticmethod
    def _validate_connection(database: str,
                             host: str,
                             port: Union[str, int],
                             user: str,
                             password: str,
                             engine: str = "mysql",
                             tcp_keepalive: bool = True,
                             application_name: str = "aws-data-wrangler-validation",
                             validation_timeout: int = 5) -> None:
        if "postgres" in engine.lower():
            conn = pg8000.connect(database=database,
                                  host=host,
                                  port=int(port),
                                  user=user,
                                  password=password,
                                  ssl=True,
                                  application_name=application_name,
                                  tcp_keepalive=tcp_keepalive,
                                  timeout=validation_timeout)
        elif "mysql" in engine.lower():
            conn = pymysql.connect(database=database,
                                   host=host,
                                   port=int(port),
                                   user=user,
                                   password=password,
                                   program_name=application_name,
                                   connect_timeout=validation_timeout)
        else:
            raise InvalidEngine(f"{engine} is not a valid engine. Please use 'mysql' or 'postgres'!")
        conn.close()

    @staticmethod
    def generate_connection(database: str,
                            host: str,
                            port: Union[str, int],
                            user: str,
                            password: str,
                            engine: str = "mysql",
                            tcp_keepalive: bool = True,
                            application_name: str = "aws-data-wrangler",
                            connection_timeout: int = 1_200_000,
                            validation_timeout: int = 5):
        """
        Generates a valid connection object

        :param database: The name of the database instance to connect with.
        :param host: The hostname of the Aurora server to connect with.
        :param port: The TCP/IP port of the Aurora server instance.
        :param user: The username to connect to the Aurora database with.
        :param password: The user password to connect to the server with.
        :param engine: "mysql" or "postgres"
        :param tcp_keepalive: If True then use TCP keepalive
        :param application_name: Application name
        :param connection_timeout: Connection Timeout
        :param validation_timeout: Timeout to try to validate the connection
        :return: PEP 249 compatible connection
        """
        Aurora._validate_connection(database=database,
                                    host=host,
                                    port=port,
                                    user=user,
                                    password=password,
                                    engine=engine,
                                    tcp_keepalive=tcp_keepalive,
                                    application_name=application_name,
                                    validation_timeout=validation_timeout)
        if "postgres" in engine.lower():
            conn = pg8000.connect(database=database,
                                  host=host,
                                  port=int(port),
                                  user=user,
                                  password=password,
                                  ssl=True,
                                  application_name=application_name,
                                  tcp_keepalive=tcp_keepalive,
                                  timeout=connection_timeout)
        elif "mysql" in engine.lower():
            conn = pymysql.connect(database=database,
                                   host=host,
                                   port=int(port),
                                   user=user,
                                   password=password,
                                   program_name=application_name,
                                   connect_timeout=connection_timeout)
        else:
            raise InvalidEngine(f"{engine} is not a valid engine. Please use 'mysql' or 'postgres'!")
        return conn

    def write_load_manifest(self, manifest_path: str,
                            objects_paths: List[str]) -> Dict[str, List[Dict[str, Union[str, bool]]]]:
        manifest: Dict[str, List[Dict[str, Union[str, bool]]]] = {"entries": []}
        path: str
        for path in objects_paths:
            entry: Dict[str, Union[str, bool]] = {"url": path, "mandatory": True}
            manifest["entries"].append(entry)
        payload: str = json.dumps(manifest)
        bucket: str
        bucket, path = manifest_path.replace("s3://", "").split("/", 1)
        logger.info(f"payload: {payload}")
        self._client_s3.put_object(Body=payload, Bucket=bucket, Key=path)
        return manifest

    @staticmethod
    def load_table(dataframe: pd.DataFrame,
                   dataframe_type: str,
                   load_paths: List[str],
                   schema_name: str,
                   table_name: str,
                   connection: Any,
                   num_files: int,
                   columns: Optional[List[str]] = None,
                   mode: str = "append",
                   preserve_index: bool = False,
                   engine: str = "mysql",
                   region: str = "us-east-1"):
        """
        Load text/CSV files into a Aurora table using a manifest file.
        Creates the table if necessary.

        :param dataframe: Pandas or Spark Dataframe
        :param dataframe_type: "pandas" or "spark"
        :param load_paths: S3 paths to be loaded (E.g. S3://...)
        :param schema_name: Aurora schema
        :param table_name: Aurora table name
        :param connection: A PEP 249 compatible connection (Can be generated with Aurora.generate_connection())
        :param num_files: Number of files to be loaded
        :param columns: List of columns to load
        :param mode: append or overwrite
        :param preserve_index: Should we preserve the Dataframe index? (ONLY for Pandas Dataframe)
        :param engine: "mysql" or "postgres"
        :param region: AWS S3 bucket region (Required only for postgres engine)
        :return: None
        """
        if "postgres" in engine.lower():
            Aurora.load_table_postgres(dataframe=dataframe,
                                       dataframe_type=dataframe_type,
                                       load_paths=load_paths,
                                       schema_name=schema_name,
                                       table_name=table_name,
                                       connection=connection,
                                       mode=mode,
                                       preserve_index=preserve_index,
                                       region=region,
                                       columns=columns)
        elif "mysql" in engine.lower():
            Aurora.load_table_mysql(dataframe=dataframe,
                                    dataframe_type=dataframe_type,
                                    manifest_path=load_paths[0],
                                    schema_name=schema_name,
                                    table_name=table_name,
                                    connection=connection,
                                    mode=mode,
                                    preserve_index=preserve_index,
                                    num_files=num_files,
                                    columns=columns)
        else:
            raise InvalidEngine(f"{engine} is not a valid engine. Please use 'mysql' or 'postgres'!")

    @staticmethod
    def load_table_postgres(dataframe: pd.DataFrame,
                            dataframe_type: str,
                            load_paths: List[str],
                            schema_name: str,
                            table_name: str,
                            connection: Any,
                            mode: str = "append",
                            preserve_index: bool = False,
                            region: str = "us-east-1",
                            columns: Optional[List[str]] = None):
        """
        Load text/CSV files into a Aurora table using a manifest file.
        Creates the table if necessary.

        :param dataframe: Pandas or Spark Dataframe
        :param dataframe_type: "pandas" or "spark"
        :param load_paths: S3 paths to be loaded (E.g. S3://...)
        :param schema_name: Aurora schema
        :param table_name: Aurora table name
        :param connection: A PEP 249 compatible connection (Can be generated with Aurora.generate_connection())
        :param mode: append or overwrite
        :param preserve_index: Should we preserve the Dataframe index? (ONLY for Pandas Dataframe)
        :param region: AWS S3 bucket region (Required only for postgres engine)
        :param columns: List of columns to load
        :return: None
        """
        with connection.cursor() as cursor:
            if mode == "overwrite":
                Aurora._create_table(cursor=cursor,
                                     dataframe=dataframe,
                                     dataframe_type=dataframe_type,
                                     schema_name=schema_name,
                                     table_name=table_name,
                                     preserve_index=preserve_index,
                                     engine="postgres",
                                     columns=columns)
                connection.commit()
                logger.debug("CREATE TABLE committed.")
        for path in load_paths:
            sql = Aurora._get_load_sql(path=path,
                                       schema_name=schema_name,
                                       table_name=table_name,
                                       engine="postgres",
                                       region=region,
                                       columns=columns)
            Aurora._load_object_postgres_with_retry(connection=connection, sql=sql)
            logger.debug(f"Load committed for: {path}.")

    @staticmethod
    @tenacity.retry(retry=tenacity.retry_if_exception_type(exception_types=ProgrammingError),
                    wait=tenacity.wait_random_exponential(multiplier=0.5),
                    stop=tenacity.stop_after_attempt(max_attempt_number=5),
                    reraise=True,
                    after=tenacity.after_log(logger, INFO))
    def _load_object_postgres_with_retry(connection: Any, sql: str) -> None:
        logger.debug(sql)
        with connection.cursor() as cursor:
            try:
                cursor.execute(sql)
            except ProgrammingError as ex:
                logger.debug(f"Exception: {ex}")
                connection.rollback()
                if "The file has been modified" in str(ex):
                    raise ex
                elif "0 rows were copied successfully" in str(ex):
                    raise ex
                else:
                    raise AuroraLoadError(str(ex))
        connection.commit()

    @staticmethod
    def load_table_mysql(dataframe: pd.DataFrame,
                         dataframe_type: str,
                         manifest_path: str,
                         schema_name: str,
                         table_name: str,
                         connection: Any,
                         num_files: int,
                         mode: str = "append",
                         preserve_index: bool = False,
                         columns: Optional[List[str]] = None):
        """
        Load text/CSV files into a Aurora table using a manifest file.
        Creates the table if necessary.

        :param dataframe: Pandas or Spark Dataframe
        :param dataframe_type: "pandas" or "spark"
        :param manifest_path: S3 manifest path to be loaded (E.g. S3://...)
        :param schema_name: Aurora schema
        :param table_name: Aurora table name
        :param connection: A PEP 249 compatible connection (Can be generated with Aurora.generate_connection())
        :param num_files: Number of files to be loaded
        :param mode: append or overwrite
        :param preserve_index: Should we preserve the Dataframe index? (ONLY for Pandas Dataframe)
        :param columns: List of columns to load
        :return: None
        """
        with connection.cursor() as cursor:
            if mode == "overwrite":
                Aurora._create_table(cursor=cursor,
                                     dataframe=dataframe,
                                     dataframe_type=dataframe_type,
                                     schema_name=schema_name,
                                     table_name=table_name,
                                     preserve_index=preserve_index,
                                     engine="mysql",
                                     columns=columns)
            sql = Aurora._get_load_sql(path=manifest_path,
                                       schema_name=schema_name,
                                       table_name=table_name,
                                       engine="mysql",
                                       columns=columns)
            logger.debug(sql)
            cursor.execute(sql)
            logger.debug(f"Load done for: {manifest_path}")
            connection.commit()
            logger.debug("Load committed.")

        with connection.cursor() as cursor:
            sql = ("-- AWS DATA WRANGLER\n"
                   f"SELECT COUNT(*) as num_files_loaded FROM mysql.aurora_s3_load_history "
                   f"WHERE load_prefix = '{manifest_path}'")
            logger.debug(sql)
            cursor.execute(sql)
            num_files_loaded = cursor.fetchall()[0][0]
            if num_files_loaded != (num_files + 1):
                raise AuroraLoadError(
                    f"Missing files to load. {num_files_loaded} files counted. {num_files + 1} expected.")

    @staticmethod
    def _parse_path(path):
        path2 = path.replace("s3://", "")
        parts = path2.partition("/")
        return parts[0], parts[2]

    @staticmethod
    def _get_load_sql(path: str,
                      schema_name: str,
                      table_name: str,
                      engine: str,
                      region: str = "us-east-1",
                      columns: Optional[List[str]] = None) -> str:
        if "postgres" in engine.lower():
            bucket, key = Aurora._parse_path(path=path)
            if columns is None:
                cols_str: str = ""
            else:
                cols_str = ",".join(columns)
            sql: str = ("-- AWS DATA WRANGLER\n"
                        "SELECT aws_s3.table_import_from_s3(\n"
                        f"'{schema_name}.{table_name}',\n"
                        f"'{cols_str}',\n"
                        "'(FORMAT CSV, DELIMITER '','', QUOTE ''\"'', ESCAPE ''\"'')',\n"
                        f"'({bucket},{key},{region})')")
        elif "mysql" in engine.lower():
            if columns is None:
                cols_str = ""
            else:
                # building something like: (@col1,@col2) set col1=@col1,col2=@col2
                col_str = [f"@{x}" for x in columns]
                set_str = [f"{x}=@{x}" for x in columns]
                cols_str = f"({','.join(col_str)}) SET {','.join(set_str)}"
                logger.debug(f"cols_str: {cols_str}")
            sql = ("-- AWS DATA WRANGLER\n"
                   f"LOAD DATA FROM S3 MANIFEST '{path}'\n"
                   "REPLACE\n"
                   f"INTO TABLE {schema_name}.{table_name}\n"
                   "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\"'\n"
                   "LINES TERMINATED BY '\\n'"
                   f"{cols_str}")
        else:
            raise InvalidEngine(f"{engine} is not a valid engine. Please use 'mysql' or 'postgres'!")
        return sql

    @staticmethod
    def _create_table(cursor,
                      dataframe,
                      dataframe_type,
                      schema_name,
                      table_name,
                      preserve_index=False,
                      engine: str = "mysql",
                      columns: Optional[List[str]] = None):
        """
        Creates Aurora table.

        :param cursor: A PEP 249 compatible cursor
        :param dataframe: Pandas or Spark Dataframe
        :param dataframe_type: "pandas" or "spark"
        :param schema_name: Redshift schema
        :param table_name: Redshift table name
        :param preserve_index: Should we preserve the Dataframe index? (ONLY for Pandas Dataframe)
        :param engine: "mysql" or "postgres"
        :param columns: List of columns to load
        :return: None
        """
        sql: str = f"-- AWS DATA WRANGLER\n" \
                   f"DROP TABLE IF EXISTS {schema_name}.{table_name}"
        logger.debug(f"Drop table query:\n{sql}")
        if "postgres" in engine.lower():
            cursor.execute(sql)
        elif "mysql" in engine.lower():
            with warnings.catch_warnings():
                warnings.filterwarnings(action="ignore", message=".*Unknown table.*")
                cursor.execute(sql)
        else:
            raise InvalidEngine(f"{engine} is not a valid engine. Please use 'mysql' or 'postgres'!")
        schema = Aurora._get_schema(dataframe=dataframe,
                                    dataframe_type=dataframe_type,
                                    preserve_index=preserve_index,
                                    engine=engine,
                                    columns=columns)
        cols_str: str = "".join([f"{col[0]} {col[1]},\n" for col in schema])[:-2]
        sql = f"-- AWS DATA WRANGLER\n" f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (\n" f"{cols_str})"
        logger.debug(f"Create table query:\n{sql}")
        cursor.execute(sql)

    @staticmethod
    def _get_schema(dataframe,
                    dataframe_type: str,
                    preserve_index: bool,
                    engine: str = "mysql",
                    columns: Optional[List[str]] = None) -> List[Tuple[str, str]]:
        schema_built: List[Tuple[str, str]] = []
        if "postgres" in engine.lower():
            convert_func = data_types.pyarrow2postgres
        elif "mysql" in engine.lower():
            convert_func = data_types.pyarrow2mysql
        else:
            raise InvalidEngine(f"{engine} is not a valid engine. Please use 'mysql' or 'postgres'!")
        if dataframe_type.lower() == "pandas":
            pyarrow_schema: List[Tuple[str, str]] = data_types.extract_pyarrow_schema_from_pandas(
                dataframe=dataframe, preserve_index=preserve_index, indexes_position="right")
            for name, dtype in pyarrow_schema:
                if columns is None or name in columns:
                    aurora_type: str = convert_func(dtype)
                    schema_built.append((name, aurora_type))
        else:
            raise InvalidDataframeType(f"{dataframe_type} is not a valid DataFrame type. Please use 'pandas'!")
        return schema_built

    def to_s3(self, sql: str, path: str, connection: Any, engine: str = "mysql") -> str:
        """
        Write a query result on S3

        :param sql: SQL Query
        :param path: AWS S3 path to write the data (e.g. s3://...)
        :param connection: A PEP 249 compatible connection (Can be generated with Redshift.generate_connection())
        :param engine: Only "mysql" by now
        :return: Manifest S3 path
        """
        if "mysql" not in engine.lower():
            raise InvalidEngine(f"{engine} is not a valid engine. Please use 'mysql'!")
        path = path[-1] if path[-1] == "/" else path
        self._session.s3.delete_objects(path=path)
        sql = f"{sql}\n" \
              f"INTO OUTFILE S3 '{path}'\n" \
              "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\\\\'\n" \
              "LINES TERMINATED BY '\\n'\n" \
              "MANIFEST ON\n" \
              "OVERWRITE ON"
        with connection.cursor() as cursor:
            logger.debug(sql)
            cursor.execute(sql)
        connection.commit()
        return path + ".manifest"

    def extract_manifest_paths(self, path: str) -> List[str]:
        bucket_name, key_path = Aurora._parse_path(path)
        body: bytes = self._client_s3.get_object(Bucket=bucket_name, Key=key_path)["Body"].read()
        return [x["url"] for x in json.loads(body.decode('utf-8'))["entries"]]
