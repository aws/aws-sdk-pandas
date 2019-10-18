import json
import logging

import pg8000  # type: ignore

from awswrangler import data_types
from awswrangler.exceptions import (
    RedshiftLoadError,
    InvalidDataframeType,
    InvalidRedshiftDiststyle,
    InvalidRedshiftDistkey,
    InvalidRedshiftSortstyle,
    InvalidRedshiftSortkey,
)

logger = logging.getLogger(__name__)

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
    def __init__(self, session):
        self._session = session

    @staticmethod
    def _validate_connection(database,
                             host,
                             port,
                             user,
                             password,
                             tcp_keepalive=True,
                             application_name="aws-data-wrangler-validation",
                             validation_timeout=5):
        try:
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
        except pg8000.core.InterfaceError as e:
            raise e

    @staticmethod
    def generate_connection(database,
                            host,
                            port,
                            user,
                            password,
                            tcp_keepalive=True,
                            application_name="aws-data-wrangler",
                            connection_timeout=1_200_000,
                            statement_timeout=1_200_000,
                            validation_timeout=5):
        """
        Generates a valid connection object to be passed to the load_table method

        :param database: The name of the database instance to connect with.
        :param host: The hostname of the Redshift server to connect with.
        :param port: The TCP/IP port of the Redshift server instance.
        :param user: The username to connect to the Redshift server with.
        :param password: The user password to connect to the server with.
        :param tcp_keepalive: If True then use TCP keepalive
        :param application_name: Application name
        :param connection_timeout: Connection Timeout
        :param statement_timeout: Redshift statements timeout
        :param validation_timeout: Timeout to try to validate the connection
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
        if isinstance(type(port), str) or isinstance(type(port), float):
            port = int(port)
        conn = pg8000.connect(database=database,
                              host=host,
                              port=int(port),
                              user=user,
                              password=password,
                              ssl=True,
                              application_name=application_name,
                              tcp_keepalive=tcp_keepalive,
                              timeout=connection_timeout)
        cursor = conn.cursor()
        cursor.execute(f"set statement_timeout = {statement_timeout}")
        conn.commit()
        cursor.close()
        return conn

    def get_connection(self, glue_connection):
        conn_details = self._session.glue.get_connection_details(name=glue_connection)
        props = conn_details["ConnectionProperties"]
        host = props["JDBC_CONNECTION_URL"].split(":")[2].replace("/", "")
        port, database = props["JDBC_CONNECTION_URL"].split(":")[3].split("/")
        user = props["USERNAME"]
        password = props["PASSWORD"]
        conn = self.generate_connection(database=database, host=host, port=int(port), user=user, password=password)
        return conn

    def write_load_manifest(self, manifest_path, objects_paths):
        objects_sizes = self._session.s3.get_objects_sizes(objects_paths=objects_paths)
        manifest = {"entries": []}
        for path, size in objects_sizes.items():
            entry = {"url": path, "mandatory": True, "meta": {"content_length": size}}
            manifest.get("entries").append(entry)
        payload = json.dumps(manifest)
        client_s3 = self._session.boto3_session.client(service_name="s3", config=self._session.botocore_config)
        bucket, path = manifest_path.replace("s3://", "").split("/", 1)
        client_s3.put_object(Body=payload, Bucket=bucket, Key=path)
        return manifest

    @staticmethod
    def get_number_of_slices(redshift_conn):
        cursor = redshift_conn.cursor()
        cursor.execute("SELECT COUNT(*) as count_slices FROM (SELECT DISTINCT node, slice from STV_SLICES)")
        count_slices = cursor.fetchall()[0][0]
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
                   mode="append",
                   preserve_index=False,
                   cast_columns=None):
        """
        Load Parquet files into a Redshift table using a manifest file.
        Creates the table if necessary.

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
        :param mode: append or overwrite
        :param preserve_index: Should we preserve the Dataframe index? (ONLY for Pandas Dataframe)
        :param cast_columns: Dictionary of columns names and Redshift types to be casted. (E.g. {"col name": "INT", "col2 name": "FLOAT"})
        :return: None
        """
        cursor = redshift_conn.cursor()
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
                                   preserve_index=preserve_index,
                                   cast_columns=cast_columns)
        sql = ("-- AWS DATA WRANGLER\n"
               f"COPY {schema_name}.{table_name} FROM '{manifest_path}'\n"
               f"IAM_ROLE '{iam_role}'\n"
               "MANIFEST\n"
               "FORMAT AS PARQUET")
        cursor.execute(sql)
        cursor.execute("-- AWS DATA WRANGLER\n SELECT pg_last_copy_id() AS query_id")
        query_id = cursor.fetchall()[0][0]
        sql = ("-- AWS DATA WRANGLER\n"
               f"SELECT COUNT(*) as num_files_loaded FROM STL_LOAD_COMMITS WHERE query = {query_id}")
        cursor.execute(sql)
        num_files_loaded = cursor.fetchall()[0][0]
        if num_files_loaded != num_files:
            redshift_conn.rollback()
            cursor.close()
            raise RedshiftLoadError(
                f"Redshift load rollbacked. {num_files_loaded} files counted. {num_files} expected.")
        redshift_conn.commit()
        cursor.close()

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
                      preserve_index=False,
                      cast_columns=None):
        """
        Creates Redshift table.

        :param cursor: A PEP 249 compatible cursor
        :param dataframe: Pandas or Spark Dataframe
        :param dataframe_type: "pandas" or "spark"
        :param schema_name: Redshift schema
        :param table_name: Redshift table name
        :param diststyle: Redshift distribution styles. Must be in ["AUTO", "EVEN", "ALL", "KEY"] (https://docs.aws.amazon.com/redshift/latest/dg/t_Distributing_data.html)
        :param distkey: Specifies a column name or positional number for the distribution key
        :param sortstyle: Sorting can be "COMPOUND" or "INTERLEAVED" (https://docs.aws.amazon.com/redshift/latest/dg/t_Sorting_data.html)
        :param sortkey: List of columns to be sorted
        :param preserve_index: Should we preserve the Dataframe index? (ONLY for Pandas Dataframe)
        :param cast_columns: Dictionary of columns names and Redshift types to be casted. (E.g. {"col name": "INT", "col2 name": "FLOAT"})
        :return: None
        """
        sql = f"-- AWS DATA WRANGLER\n" \
              f"DROP TABLE IF EXISTS {schema_name}.{table_name}"
        logger.debug(f"Drop table query:\n{sql}")
        cursor.execute(sql)
        schema = Redshift._get_redshift_schema(
            dataframe=dataframe,
            dataframe_type=dataframe_type,
            preserve_index=preserve_index,
            cast_columns=cast_columns,
        )
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
        cols_str = "".join([f"{col[0]} {col[1]},\n" for col in schema])[:-2]
        distkey_str = ""
        if distkey and diststyle == "KEY":
            distkey_str = f"\nDISTKEY({distkey})"
        sortkey_str = ""
        if sortkey:
            sortkey_str = f"\n{sortstyle} SORTKEY({','.join(sortkey)})"
        sql = (f"-- AWS DATA WRANGLER\n"
               f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (\n"
               f"{cols_str}"
               f")\nDISTSTYLE {diststyle}"
               f"{distkey_str}"
               f"{sortkey_str}")
        logger.debug(f"Create table query:\n{sql}")
        cursor.execute(sql)

    @staticmethod
    def _validate_parameters(schema, diststyle, distkey, sortstyle, sortkey):
        """
        Validates the sanity of Redshift's parameters
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
    def _get_redshift_schema(dataframe, dataframe_type, preserve_index=False, cast_columns=None):
        if cast_columns is None:
            cast_columns = {}
        schema_built = []
        if dataframe_type == "pandas":
            pyarrow_schema = data_types.extract_pyarrow_schema_from_pandas(dataframe=dataframe,
                                                                           preserve_index=preserve_index,
                                                                           indexes_position="right")
            for name, dtype in pyarrow_schema:
                if (cast_columns is not None) and (name in cast_columns.keys()):
                    schema_built.append((name, cast_columns[name]))
                else:
                    redshift_type = data_types.pyarrow2redshift(dtype)
                    schema_built.append((name, redshift_type))
        elif dataframe_type == "spark":
            for name, dtype in dataframe.dtypes:
                if name in cast_columns.keys():
                    redshift_type = data_types.athena2redshift(cast_columns[name])
                else:
                    redshift_type = data_types.spark2redshift(dtype)
                schema_built.append((name, redshift_type))
        else:
            raise InvalidDataframeType(dataframe_type)
        return schema_built
