import json
import logging

import pg8000

from awswrangler.exceptions import (
    RedshiftLoadError,
    UnsupportedType,
    InvalidDataframeType,
)

logger = logging.getLogger(__name__)


class Redshift:
    def __init__(self, session):
        self._session = session

    @staticmethod
    def generate_connection(database, host, port, user, password):
        conn = pg8000.connect(
            database=database,
            host=host,
            port=int(port),
            user=user,
            password=password,
            ssl=False,
        )
        cursor = conn.cursor()
        cursor.execute("set statement_timeout = 1200000")
        conn.commit()
        cursor.close()
        return conn

    def get_connection(self, glue_connection):
        conn_details = self._session.glue.get_connection_details(
            name=glue_connection)
        props = conn_details["ConnectionProperties"]
        host = props["JDBC_CONNECTION_URL"].split(":")[2].replace("/", "")
        port, database = props["JDBC_CONNECTION_URL"].split(":")[3].split("/")
        user = props["USERNAME"]
        password = props["PASSWORD"]
        conn = self.generate_connection(database=database,
                                        host=host,
                                        port=int(port),
                                        user=user,
                                        password=password)
        return conn

    def write_load_manifest(self, manifest_path, objects_paths):
        objects_sizes = self._session.s3.get_objects_sizes(
            objects_paths=objects_paths)
        manifest = {"entries": []}
        for path, size in objects_sizes.items():
            entry = {
                "url": path,
                "mandatory": True,
                "meta": {
                    "content_length": size
                }
            }
            manifest.get("entries").append(entry)
        payload = json.dumps(manifest)
        client_s3 = self._session.boto3_session.client(
            service_name="s3", config=self._session.botocore_config)
        bucket, path = manifest_path.replace("s3://", "").split("/", 1)
        client_s3.put_object(Body=payload, Bucket=bucket, Key=path)
        return manifest

    @staticmethod
    def get_number_of_slices(redshift_conn):
        cursor = redshift_conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) as count_slices FROM (SELECT DISTINCT node, slice from STV_SLICES)"
        )
        count_slices = cursor.fetchall()[0][0]
        cursor.close()
        return count_slices

    @staticmethod
    def load_table(
            dataframe,
            dataframe_type,
            manifest_path,
            schema_name,
            table_name,
            redshift_conn,
            num_files,
            iam_role,
            mode="append",
            preserve_index=False,
    ):
        cursor = redshift_conn.cursor()
        if mode == "overwrite":
            cursor.execute("-- AWS DATA WRANGLER\n"
                           f"DROP TABLE IF EXISTS {schema_name}.{table_name}")
        schema = Redshift._get_redshift_schema(
            dataframe=dataframe,
            dataframe_type=dataframe_type,
            preserve_index=preserve_index,
        )
        cols_str = "".join([f"{col[0]} {col[1]},\n" for col in schema])[:-2]
        sql = (
            "-- AWS DATA WRANGLER\n"
            f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (\n{cols_str}"
            ") DISTSTYLE AUTO")
        cursor.execute(sql)
        sql = ("-- AWS DATA WRANGLER\n"
               f"COPY {schema_name}.{table_name} FROM '{manifest_path}'\n"
               f"IAM_ROLE '{iam_role}'\n"
               "MANIFEST\n"
               "FORMAT AS PARQUET")
        cursor.execute(sql)
        cursor.execute(
            "-- AWS DATA WRANGLER\n SELECT pg_last_copy_id() AS query_id")
        query_id = cursor.fetchall()[0][0]
        sql = (
            "-- AWS DATA WRANGLER\n"
            f"SELECT COUNT(*) as num_files_loaded FROM STL_LOAD_COMMITS WHERE query = {query_id}"
        )
        cursor.execute(sql)
        num_files_loaded = cursor.fetchall()[0][0]
        if num_files_loaded != num_files:
            redshift_conn.rollback()
            cursor.close()
            raise RedshiftLoadError(
                f"Redshift load rollbacked. {num_files_loaded} files counted. {num_files} expected."
            )
        redshift_conn.commit()
        cursor.close()

    @staticmethod
    def _get_redshift_schema(dataframe, dataframe_type, preserve_index=False):
        schema_built = []
        if dataframe_type == "pandas":
            if preserve_index:
                name = str(
                    dataframe.index.name) if dataframe.index.name else "index"
                dataframe.index.name = "index"
                dtype = str(dataframe.index.dtype)
                redshift_type = Redshift._type_pandas2redshift(dtype)
                schema_built.append((name, redshift_type))
            for col in dataframe.columns:
                name = str(col)
                dtype = str(dataframe[name].dtype)
                redshift_type = Redshift._type_pandas2redshift(dtype)
                schema_built.append((name, redshift_type))
        elif dataframe_type == "spark":
            for name, dtype in dataframe.dtypes:
                redshift_type = Redshift._type_spark2redshift(dtype)
                schema_built.append((name, redshift_type))
        else:
            raise InvalidDataframeType(dataframe_type)
        return schema_built

    @staticmethod
    def _type_pandas2redshift(dtype):
        dtype = dtype.lower()
        if dtype == "int32":
            return "INTEGER"
        elif dtype == "int64":
            return "BIGINT"
        elif dtype == "float32":
            return "FLOAT4"
        elif dtype == "float64":
            return "FLOAT8"
        elif dtype == "bool":
            return "BOOLEAN"
        elif dtype == "object" and isinstance(dtype, str):
            return "VARCHAR(256)"
        elif dtype[:10] == "datetime64":
            return "TIMESTAMP"
        else:
            raise UnsupportedType("Unsupported Pandas type: " + dtype)

    @staticmethod
    def _type_spark2redshift(dtype):
        dtype = dtype.lower()
        if dtype == "int":
            return "INTEGER"
        elif dtype == "long":
            return "BIGINT"
        elif dtype == "float":
            return "FLOAT8"
        elif dtype == "bool":
            return "BOOLEAN"
        elif dtype == "string":
            return "VARCHAR(256)"
        elif dtype[:10] == "datetime.datetime":
            return "TIMESTAMP"
        else:
            raise UnsupportedType("Unsupported Spark type: " + dtype)
