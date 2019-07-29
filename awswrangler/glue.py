from math import ceil
import re
import logging
from datetime import datetime, date

from awswrangler.exceptions import UnsupportedType, UnsupportedFileFormat

logger = logging.getLogger(__name__)


class Glue:
    def __init__(self, session):
        self._session = session
        self._client_glue = session.boto3_session.client(
            service_name="glue", config=session.botocore_config)

    def get_table_athena_types(self, database, table):
        """
        Get all columns names and the related data types
        :param database: Glue database's name
        :param table: Glue table's name
        :return: A dictionary as {"col name": "col dtype"}
        """
        response = self._client_glue.get_table(DatabaseName=database,
                                               Name=table)
        logger.debug(f"get_table response:\n{response}")
        dtypes = {}
        for col in response["Table"]["StorageDescriptor"]["Columns"]:
            dtypes[col["Name"]] = col["Type"]
        for par in response["Table"]["PartitionKeys"]:
            dtypes[par["Name"]] = par["Type"]
        return dtypes

    def get_table_python_types(self, database, table):
        """
        Get all columns names and the related python types
        :param database: Glue database's name
        :param table: Glue table's name
        :return: A dictionary as {"col name": "col python type"}
        """
        dtypes = self.get_table_athena_types(database=database, table=table)
        return {k: Glue._type_athena2python(v) for k, v in dtypes.items()}

    @staticmethod
    def _type_pandas2athena(dtype):
        dtype = dtype.lower()
        if dtype == "int32":
            return "int"
        elif dtype in ["int64", "Int64"]:
            return "bigint"
        elif dtype == "float32":
            return "float"
        elif dtype == "float64":
            return "double"
        elif dtype == "bool":
            return "boolean"
        elif dtype == "object" and isinstance(dtype, str):
            return "string"
        elif dtype[:10] == "datetime64":
            return "timestamp"
        else:
            raise UnsupportedType(f"Unsupported Pandas type: {dtype}")

    @staticmethod
    def _type_athena2python(dtype):
        dtype = dtype.lower()
        if dtype in ["int", "integer", "bigint", "smallint", "tinyint"]:
            return int
        elif dtype in ["float", "double", "real"]:
            return float
        elif dtype == "boolean":
            return bool
        elif dtype in ["string", "char", "varchar", "array", "row", "map"]:
            return str
        elif dtype == "timestamp":
            return datetime
        elif dtype == "date":
            return date
        else:
            raise UnsupportedType(f"Unsupported Athena type: {dtype}")

    def metadata_to_glue(self,
                         dataframe,
                         path,
                         objects_paths,
                         file_format,
                         database=None,
                         table=None,
                         partition_cols=None,
                         preserve_index=True,
                         mode="append",
                         cast_columns=None):
        schema = Glue._build_schema(dataframe=dataframe,
                                    partition_cols=partition_cols,
                                    preserve_index=preserve_index,
                                    cast_columns=cast_columns)
        table = table if table else Glue._parse_table_name(path)
        table = table.lower().replace(".", "_")
        if mode == "overwrite":
            self.delete_table_if_exists(database=database, table=table)
        exists = self.does_table_exists(database=database, table=table)
        if not exists:
            self.create_table(
                database=database,
                table=table,
                schema=schema,
                partition_cols=partition_cols,
                path=path,
                file_format=file_format,
            )
        if partition_cols:
            partitions_tuples = Glue._parse_partitions_tuples(
                objects_paths=objects_paths, partition_cols=partition_cols)
            self.add_partitions(
                database=database,
                table=table,
                partition_paths=partitions_tuples,
                file_format=file_format,
            )

    def delete_table_if_exists(self, database, table):
        try:
            self._client_glue.delete_table(DatabaseName=database, Name=table)
        except self._client_glue.exceptions.EntityNotFoundException:
            pass

    def does_table_exists(self, database, table):
        try:
            self._client_glue.get_table(DatabaseName=database, Name=table)
            return True
        except self._client_glue.exceptions.EntityNotFoundException:
            return False

    def create_table(self,
                     database,
                     table,
                     schema,
                     path,
                     file_format,
                     partition_cols=None):
        if file_format == "parquet":
            table_input = Glue.parquet_table_definition(
                table, partition_cols, schema, path)
        elif file_format == "csv":
            table_input = Glue.csv_table_definition(table, partition_cols,
                                                    schema, path)
        else:
            raise UnsupportedFileFormat(file_format)
        self._client_glue.create_table(DatabaseName=database,
                                       TableInput=table_input)

    def add_partitions(self, database, table, partition_paths, file_format):
        if not partition_paths:
            return None
        partitions = list()
        for partition in partition_paths:
            if file_format == "parquet":
                partition_def = Glue.parquet_partition_definition(partition)
            elif file_format == "csv":
                partition_def = Glue.csv_partition_definition(partition)
            else:
                raise UnsupportedFileFormat(file_format)
            partitions.append(partition_def)
        pages_num = int(ceil(len(partitions) / 100.0))
        for _ in range(pages_num):
            page = partitions[:100]
            del partitions[:100]
            self._client_glue.batch_create_partition(DatabaseName=database,
                                                     TableName=table,
                                                     PartitionInputList=page)

    def get_connection_details(self, name):
        return self._client_glue.get_connection(
            Name=name, HidePassword=False)["Connection"]

    @staticmethod
    def _build_schema(dataframe,
                      partition_cols,
                      preserve_index,
                      cast_columns=None):
        print(f"dataframe.dtypes:\n{dataframe.dtypes}")
        if not partition_cols:
            partition_cols = []
        schema_built = []
        if preserve_index:
            name = str(
                dataframe.index.name) if dataframe.index.name else "index"
            dataframe.index.name = "index"
            dtype = str(dataframe.index.dtype)
            if name not in partition_cols:
                athena_type = Glue._type_pandas2athena(dtype)
                schema_built.append((name, athena_type))
        for col in dataframe.columns:
            name = str(col)
            if cast_columns and name in cast_columns:
                dtype = cast_columns[name]
            else:
                dtype = str(dataframe[name].dtype)
            if name not in partition_cols:
                athena_type = Glue._type_pandas2athena(dtype)
                schema_built.append((name, athena_type))
        logger.debug(f"schema_built:\n{schema_built}")
        return schema_built

    @staticmethod
    def _parse_table_name(path):
        if path[-1] == "/":
            path = path[:-1]
        return path.rpartition("/")[2]

    @staticmethod
    def csv_table_definition(table, partition_cols, schema, path):
        if not partition_cols:
            partition_cols = []
        return {
            "Name":
            table,
            "PartitionKeys": [{
                "Name": x,
                "Type": "string"
            } for x in partition_cols],
            "TableType":
            "EXTERNAL_TABLE",
            "Parameters": {
                "classification": "csv",
                "compressionType": "none",
                "typeOfData": "file",
                "delimiter": ",",
                "columnsOrdered": "true",
                "areColumnsQuoted": "false",
            },
            "StorageDescriptor": {
                "Columns": [{
                    "Name": x[0],
                    "Type": x[1]
                } for x in schema],
                "Location": path,
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat":
                "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "Compressed": False,
                "NumberOfBuckets": -1,
                "SerdeInfo": {
                    "Parameters": {
                        "field.delim": ","
                    },
                    "SerializationLibrary":
                    "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                },
                "StoredAsSubDirectories": False,
                "SortColumns": [],
                "Parameters": {
                    "classification": "csv",
                    "compressionType": "none",
                    "typeOfData": "file",
                    "delimiter": ",",
                    "columnsOrdered": "true",
                    "areColumnsQuoted": "false",
                },
            },
        }

    @staticmethod
    def csv_partition_definition(partition):
        return {
            "StorageDescriptor": {
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "Location": partition[0],
                "SerdeInfo": {
                    "Parameters": {
                        "field.delim": ","
                    },
                    "SerializationLibrary":
                    "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                },
                "StoredAsSubDirectories": False,
            },
            "Values": partition[1],
        }

    @staticmethod
    def parquet_table_definition(table, partition_cols, schema, path):
        if not partition_cols:
            partition_cols = []
        return {
            "Name":
            table,
            "PartitionKeys": [{
                "Name": x,
                "Type": "string"
            } for x in partition_cols],
            "TableType":
            "EXTERNAL_TABLE",
            "Parameters": {
                "classification": "parquet",
                "compressionType": "none",
                "typeOfData": "file",
            },
            "StorageDescriptor": {
                "Columns": [{
                    "Name": x[0],
                    "Type": x[1]
                } for x in schema],
                "Location": path,
                "InputFormat":
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat":
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "Compressed": False,
                "NumberOfBuckets": -1,
                "SerdeInfo": {
                    "SerializationLibrary":
                    "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                    "Parameters": {
                        "serialization.format": "1"
                    },
                },
                "StoredAsSubDirectories": False,
                "SortColumns": [],
                "Parameters": {
                    "CrawlerSchemaDeserializerVersion": "1.0",
                    "classification": "parquet",
                    "compressionType": "none",
                    "typeOfData": "file",
                },
            },
        }

    @staticmethod
    def parquet_partition_definition(partition):
        return {
            "StorageDescriptor": {
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "Location": partition[0],
                "SerdeInfo": {
                    "Parameters": {
                        "serialization.format": "1"
                    },
                    "SerializationLibrary":
                    "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                },
                "StoredAsSubDirectories": False,
            },
            "Values": partition[1],
        }

    @staticmethod
    def _parse_partitions_tuples(objects_paths, partition_cols):
        paths = {f"{path.rpartition('/')[0]}/" for path in objects_paths}
        return [(
            path,
            Glue._parse_partition_values(path=path,
                                         partition_cols=partition_cols),
        ) for path in paths]

    @staticmethod
    def _parse_partition_values(path, partition_cols):
        return [
            re.search(f"/{col}=(.*?)/", path).group(1)
            for col in partition_cols
        ]
