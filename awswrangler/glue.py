from math import ceil

from awswrangler.exceptions import UnsupportedType, UnsupportedFileFormat


class Glue:
    def __init__(self, session):
        self._session = session

    def metadata_to_glue(
        self,
        dataframe,
        path,
        partition_paths,
        file_format,
        database=None,
        table=None,
        partition_cols=None,
        preserve_index=True,
        mode="append",
    ):
        schema = Glue._build_schema(
            dataframe=dataframe,
            partition_cols=partition_cols,
            preserve_index=preserve_index,
        )
        table = table if table else Glue._parse_table_name(path)
        if mode == "overwrite":
            self.delete_table_if_exists(database=database, table=table)
        exists = self.table_exists(database=database, table=table)
        if not exists:
            self.create_table(
                database=database,
                table=table,
                schema=schema,
                partition_cols=partition_cols,
                path=path,
                file_format=file_format,
            )
        self.add_partitions(
            database=database,
            table=table,
            partition_paths=partition_paths,
            file_format=file_format,
        )

    def delete_table_if_exists(self, database, table):
        client = self._session.boto3_session.client("glue")
        try:
            client.delete_table(DatabaseName=database, Name=table)
        except client.exceptions.EntityNotFoundException:
            pass

    def table_exists(self, database, table):
        client = self._session.boto3_session.client("glue")
        try:
            client.get_table(DatabaseName=database, Name=table)
            return True
        except client.exceptions.EntityNotFoundException:
            return False

    def create_table(
        self, database, table, schema, path, file_format, partition_cols=None
    ):
        client = self._session.boto3_session.client("glue")
        if file_format == "parquet":
            table_input = Glue.parquet_table_definition(
                table, partition_cols, schema, path
            )
        elif file_format == "csv":
            table_input = Glue.csv_table_definition(table, partition_cols, schema, path)
        else:
            raise UnsupportedFileFormat(file_format)
        client.create_table(DatabaseName=database, TableInput=table_input)

    def add_partitions(self, database, table, partition_paths, file_format):
        client = self._session.boto3_session.client("glue")
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
            client.batch_create_partition(
                DatabaseName=database, TableName=table, PartitionInputList=page
            )

    def get_connection_details(self, name):
        client = self._session.boto3_session.client("glue")
        return client.get_connection(Name=name, HidePassword=False)["Connection"]

    @staticmethod
    def _build_schema(dataframe, partition_cols, preserve_index):
        if not partition_cols:
            partition_cols = []
        schema_built = []
        if preserve_index:
            name = str(dataframe.index.name) if dataframe.index.name else "index"
            dataframe.index.name = "index"
            dtype = str(dataframe.index.dtype)
            if name not in partition_cols:
                athena_type = Glue._type_pandas2athena(dtype)
                schema_built.append((name, athena_type))
        for col in dataframe.columns:
            name = str(col)
            dtype = str(dataframe[name].dtype)
            if name not in partition_cols:
                athena_type = Glue._type_pandas2athena(dtype)
                schema_built.append((name, athena_type))
        return schema_built

    @staticmethod
    def _type_pandas2athena(dtype):
        dtype = dtype.lower()
        if dtype == "int32":
            return "int"
        elif dtype == "int64":
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
            return "string"
        else:
            raise UnsupportedType("Unsupported Pandas type: " + dtype)

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
            "Name": table,
            "PartitionKeys": [{"Name": x, "Type": "string"} for x in partition_cols],
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {
                "classification": "csv",
                "compressionType": "none",
                "typeOfData": "file",
                "delimiter": ",",
                "columnsOrdered": "true",
                "areColumnsQuoted": "false",
            },
            "StorageDescriptor": {
                "Columns": [{"Name": x[0], "Type": x[1]} for x in schema],
                "Location": path,
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "Compressed": False,
                "NumberOfBuckets": -1,
                "SerdeInfo": {
                    "Parameters": {"field.delim": ","},
                    "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
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
            u"StorageDescriptor": {
                u"InputFormat": u"org.apache.hadoop.mapred.TextInputFormat",
                u"Location": partition[0],
                u"SerdeInfo": {
                    u"Parameters": {u"field.delim": ","},
                    u"SerializationLibrary": u"org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                },
                u"StoredAsSubDirectories": False,
            },
            u"Values": partition[1],
        }

    @staticmethod
    def parquet_table_definition(table, partition_cols, schema, path):
        if not partition_cols:
            partition_cols = []
        return {
            "Name": table,
            "PartitionKeys": [{"Name": x, "Type": "string"} for x in partition_cols],
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {
                "classification": "parquet",
                "compressionType": "none",
                "typeOfData": "file",
            },
            "StorageDescriptor": {
                "Columns": [{"Name": x[0], "Type": x[1]} for x in schema],
                "Location": path,
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "Compressed": False,
                "NumberOfBuckets": -1,
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                    "Parameters": {"serialization.format": "1"},
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
            u"StorageDescriptor": {
                u"InputFormat": u"org.apache.hadoop.mapred.TextInputFormat",
                u"Location": partition[0],
                u"SerdeInfo": {
                    u"Parameters": {u"serialization.format": u"1"},
                    u"SerializationLibrary": u"org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                },
                u"StoredAsSubDirectories": False,
            },
            u"Values": partition[1],
        }
