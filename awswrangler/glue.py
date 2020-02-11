"""AWS Glue Module."""

import re
from itertools import islice
from logging import Logger, getLogger
from math import ceil
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Tuple, Union

from boto3 import client  # type: ignore
from pandas import DataFrame  # type: ignore

from awswrangler import data_types
from awswrangler.athena import Athena
from awswrangler.aurora import Aurora
from awswrangler.exceptions import (ApiError, InvalidArguments, InvalidSerDe, InvalidTable, UndetectedType,
                                    UnsupportedFileFormat, UnsupportedType)
from awswrangler.redshift import Redshift

if TYPE_CHECKING:
    from awswrangler.session import Session

logger: Logger = getLogger(__name__)


class Glue:
    """AWS Glue Class."""
    def __init__(self, session: "Session"):
        """
        AWS Glue Class Constructor.

        Don't use it directly, call through a Session().
        e.g. wr.redshift.your_method()

        :param session: awswrangler.Session()
        """
        self._session: "Session" = session
        self._client_glue: client = session.boto3_session.client(service_name="glue", config=session.botocore_config)

    def get_table_athena_types(self, database: str, table: str) -> Dict[str, str]:
        """
        Get all columns names and the related data types.

        :param database: Glue database's name
        :param table: Glue table's name
        :return: A dictionary as {"col name": "col dtype"}
        """
        response = self._client_glue.get_table(DatabaseName=database, Name=table)
        logger.debug(f"get_table response:\n{response}")
        dtypes: Dict[str, str] = {}
        for col in response["Table"]["StorageDescriptor"]["Columns"]:
            dtypes[col["Name"]] = col["Type"]
        for par in response["Table"]["PartitionKeys"]:
            dtypes[par["Name"]] = par["Type"]
        return dtypes

    def get_table_python_types(self, database: str, table: str) -> Dict[str, Optional[type]]:
        """
        Get all columns names and the related python types.

        :param database: Glue database's name
        :param table: Glue table's name
        :return: A dictionary as {"col name": "col python type"}
        """
        dtypes = self.get_table_athena_types(database=database, table=table)
        return {k: data_types.athena2python(v) for k, v in dtypes.items()}

    def metadata_to_glue(self,
                         dataframe,
                         path: str,
                         objects_paths: List[str],
                         file_format: str,
                         database: str,
                         table: Optional[str],
                         partition_cols: Optional[List[str]] = None,
                         preserve_index: bool = True,
                         mode: str = "append",
                         compression: Optional[str] = None,
                         cast_columns: Optional[Dict[str, str]] = None,
                         extra_args: Optional[Dict[str, Optional[Union[str, int, List[str]]]]] = None,
                         description: Optional[str] = None,
                         parameters: Optional[Dict[str, str]] = None,
                         columns_comments: Optional[Dict[str, str]] = None) -> None:
        """
        Create/update a table in the Glue catalog based on a dataframe.

        :param dataframe: Pandas Dataframe
        :param path: AWS S3 path (E.g. s3://bucket-name/folder_name/
        :param objects_paths: Files paths on S3
        :param file_format: "csv" or "parquet"
        :param database: AWS Glue Database name
        :param table: AWS Glue table name
        :param partition_cols: partitions names
        :param preserve_index: Should preserve index on S3?
        :param mode: "append", "overwrite", "overwrite_partitions"
        :param compression: None, gzip, snappy, etc
        :param cast_columns: Dictionary of columns names and Athena/Glue types to be casted. (E.g. {"col name": "bigint", "col2 name": "int"}) (Only for "parquet" file_format)
        :param extra_args: Extra arguments specific for each file formats (E.g. "sep" for CSV)
        :param description: Table description
        :param parameters: Key/value pairs to tag the table (Optional[Dict[str, str]])
        :param columns_comments: Columns names and the related comments (Optional[Dict[str, str]])
        :return: None
        """
        indexes_position = "left" if file_format == "csv" else "right"
        schema: List[Tuple[str, str]]
        partition_cols_schema: List[Tuple[str, str]]
        schema, partition_cols_schema = Glue._build_schema(dataframe=dataframe,
                                                           partition_cols=partition_cols,
                                                           preserve_index=preserve_index,
                                                           indexes_position=indexes_position,
                                                           cast_columns=cast_columns)
        table = table if table else Glue._parse_table_name(path)
        table = Athena.normalize_table_name(name=table)
        if mode == "overwrite":
            self.delete_table_if_exists(database=database, table=table)
        exists = self.does_table_exists(database=database, table=table)
        if not exists:
            self.create_table(database=database,
                              table=table,
                              schema=schema,
                              partition_cols_schema=partition_cols_schema,
                              path=path,
                              file_format=file_format,
                              compression=compression,
                              extra_args=extra_args,
                              description=description,
                              parameters=parameters,
                              columns_comments=columns_comments)
        if partition_cols:
            partitions_tuples = Glue._parse_partitions_tuples(objects_paths=objects_paths,
                                                              partition_cols=partition_cols)
            self.add_partitions(database=database,
                                table=table,
                                partition_paths=partitions_tuples,
                                file_format=file_format,
                                compression=compression,
                                extra_args=extra_args)

    def delete_table_if_exists(self, table: str = None, database: Optional[str] = None):
        """
        Delete Glue table if exists.

        :param table: Table name
        :param database: Database name
        :return: None
        """
        if database is None and self._session.athena_database is not None:
            database = self._session.athena_database
        if database is None:
            raise InvalidArguments("You must pass a valid database or have one defined in your Session!")
        try:
            self._client_glue.delete_table(DatabaseName=database, Name=table)
        except self._client_glue.exceptions.EntityNotFoundException:
            pass

    def does_table_exists(self, database, table):
        """
        Check if the table exists.

        :param database: Database name
        :param table: Table name
        :return: True or False
        """
        try:
            self._client_glue.get_table(DatabaseName=database, Name=table)
            return True
        except self._client_glue.exceptions.EntityNotFoundException:
            return False

    def create_table(self,
                     database: str,
                     table: str,
                     schema: List[Tuple[str, str]],
                     path: str,
                     file_format: str,
                     compression: Optional[str],
                     partition_cols_schema: List[Tuple[str, str]],
                     extra_args: Optional[Dict[str, Union[str, int, List[str], None]]] = None,
                     description: Optional[str] = None,
                     parameters: Optional[Dict[str, str]] = None,
                     columns_comments: Optional[Dict[str, str]] = None) -> None:
        """
        Create Glue table (Catalog).

        :param database: AWS Glue Database name
        :param table: AWS Glue table name
        :param schema: Table schema
        :param path: AWS S3 path (E.g. s3://bucket-name/folder_name/
        :param file_format: "csv" or "parquet"
        :param compression: None, gzip, snappy, etc
        :param partition_cols_schema: Partitions schema
        :param extra_args: Extra arguments specific for each file formats (E.g. "sep" for CSV)
        :param description: Table description
        :param parameters: Key/value pairs to tag the table (Optional[Dict[str, str]])
        :param columns_comments: Columns names and the related comments (Optional[Dict[str, str]])
        :return: None
        """
        if file_format == "parquet":
            table_input: Dict[str, Any] = Glue._parquet_table_definition(table=table,
                                                                         partition_cols_schema=partition_cols_schema,
                                                                         schema=schema,
                                                                         path=path,
                                                                         compression=compression)
        elif file_format == "csv":
            table_input = Glue._csv_table_definition(table=table,
                                                     partition_cols_schema=partition_cols_schema,
                                                     schema=schema,
                                                     path=path,
                                                     compression=compression,
                                                     extra_args=extra_args)
        else:
            raise UnsupportedFileFormat(file_format)
        if description is not None:
            table_input["Description"] = description
        if parameters is not None:
            for k, v in parameters.items():
                table_input["Parameters"][k] = v
        if columns_comments is not None:
            for col in table_input["StorageDescriptor"]["Columns"]:
                name = col["Name"]
                if name in columns_comments:
                    col["Comment"] = columns_comments[name]
            for par in table_input["PartitionKeys"]:
                name = par["Name"]
                if name in columns_comments:
                    par["Comment"] = columns_comments[name]
        self._client_glue.create_table(DatabaseName=database, TableInput=table_input)

    def add_partitions(self, database, table, partition_paths, file_format, compression, extra_args=None):
        """
        Add partitions to Glue Table.

        :param database: Database name
        :param table: Table name
        :param partition_paths: Partitions paths
        :param file_format: File format
        :param compression: Compression format
        :param extra_args: Extra arguments
        :return: None
        """
        if not partition_paths:
            return None
        partitions = list()
        for partition in partition_paths:
            if file_format == "parquet":
                partition_def = Glue._parquet_partition_definition(partition=partition, compression=compression)
            elif file_format == "csv":
                partition_def = Glue._csv_partition_definition(partition=partition,
                                                               compression=compression,
                                                               extra_args=extra_args)
            else:
                raise UnsupportedFileFormat(file_format)
            partitions.append(partition_def)
        pages_num = int(ceil(len(partitions) / 100.0))
        for _ in range(pages_num):
            page = partitions[:100]
            del partitions[:100]
            res = self._client_glue.batch_create_partition(DatabaseName=database,
                                                           TableName=table,
                                                           PartitionInputList=page)
            for error in res["Errors"]:
                if "ErrorDetail" in error:
                    if "ErrorCode" in error["ErrorDetail"]:
                        if error["ErrorDetail"]["ErrorCode"] != "AlreadyExistsException":
                            raise ApiError(f"{error}")

    def get_connection_details(self, name: str) -> Dict:
        """
        Get Glue connection details.

        :param name: Connection name
        :return: Details
        """
        return self._client_glue.get_connection(Name=name, HidePassword=False)["Connection"]

    @staticmethod
    def _build_schema(
            dataframe,
            partition_cols: Optional[List[str]],
            preserve_index: bool,
            indexes_position: str,
            cast_columns: Optional[Dict[str, str]] = None) -> Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]:
        cast_columns = {} if cast_columns is None else cast_columns
        partition_cols = [] if partition_cols is None else partition_cols
        logger.debug(f"dataframe.dtypes:\n{dataframe.dtypes}")
        pyarrow_schema: List[Tuple[str, Any]] = data_types.extract_pyarrow_schema_from_pandas(
            dataframe=dataframe, preserve_index=preserve_index, indexes_position=indexes_position)

        schema_built: List[Tuple[str, str]] = []
        partition_cols_types: Dict[str, str] = {}
        for name, dtype in pyarrow_schema:
            if (cast_columns is not None) and (name in cast_columns.keys()):
                if name in partition_cols:
                    partition_cols_types[name] = cast_columns[name]
                else:
                    schema_built.append((name, cast_columns[name]))
            else:
                try:
                    athena_type = data_types.pyarrow2athena(dtype)
                except UndetectedType:
                    raise UndetectedType(f"We can't infer the data type from an entire null object column ({name}). "
                                         f"Please consider pass the type of this column explicitly using the cast "
                                         f"columns argument")
                except UnsupportedType:
                    raise UnsupportedType(f"Unsupported Pyarrow type for column {name}: {dtype}")
                if name in partition_cols:
                    partition_cols_types[name] = athena_type
                else:
                    schema_built.append((name, athena_type))

        partition_cols_schema_built: List = [(name, partition_cols_types[name]) for name in partition_cols]

        logger.debug(f"schema_built:\n{schema_built}")
        logger.debug(f"partition_cols_schema_built:\n{partition_cols_schema_built}")
        return schema_built, partition_cols_schema_built

    @staticmethod
    def _parse_table_name(path):
        if path[-1] == "/":
            path = path[:-1]
        return path.rpartition("/")[2]

    @staticmethod
    def _csv_table_definition(table: str,
                              partition_cols_schema: List[Tuple[str, str]],
                              schema: List[Tuple[str, str]],
                              path: str,
                              compression: Optional[str],
                              extra_args: Optional[Dict[str, Optional[Union[str, int, List[str]]]]] = None):
        if extra_args is None:
            extra_args = {"sep": ","}
        if partition_cols_schema is None:
            partition_cols_schema = []
        compressed = False if compression is None else True
        sep = extra_args["sep"] if "sep" in extra_args else ","
        sep = "," if sep is None else sep
        serde = extra_args.get("serde", "OpenCSVSerDe")
        if serde == "OpenCSVSerDe":
            serde_fullname = "org.apache.hadoop.hive.serde2.OpenCSVSerde"
            param = {
                "separatorChar": sep,
                "quoteChar": "\"",
                "escapeChar": "\\",
            }
            refined_par_schema = [(name, "string") for name, dtype in partition_cols_schema]
            refined_schema = [(name, "string") for name, dtype in schema]
        elif serde == "LazySimpleSerDe":
            serde_fullname = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
            param = {"field.delim": sep, "escape.delim": "\\"}
            dtypes_allowed = ["int", "bigint", "float", "double"]
            refined_par_schema = [(name, dtype) if dtype in dtypes_allowed else (name, "string")
                                  for name, dtype in partition_cols_schema]
            refined_schema = [(name, dtype) if dtype in dtypes_allowed else (name, "string") for name, dtype in schema]
        else:
            raise InvalidSerDe(f"{serde} in not in the valid SerDe list.")
        if "columns" in extra_args and extra_args["columns"] is not None:
            refined_schema = [(name, dtype) for name, dtype in refined_schema
                              if name in extra_args["columns"]]  # type: ignore
        return {
            "Name": table,
            "PartitionKeys": [{
                "Name": x[0],
                "Type": x[1]
            } for x in refined_par_schema],
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {
                "classification": "csv",
                "compressionType": str(compression).lower(),
                "typeOfData": "file",
                "delimiter": sep,
                "columnsOrdered": "true",
                "areColumnsQuoted": "false",
            },
            "StorageDescriptor": {
                "Columns": [{
                    "Name": x[0],
                    "Type": x[1]
                } for x in refined_schema],
                "Location": path,
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "Compressed": compressed,
                "NumberOfBuckets": -1,
                "SerdeInfo": {
                    "Parameters": param,
                    "SerializationLibrary": serde_fullname,
                },
                "StoredAsSubDirectories": False,
                "SortColumns": [],
                "Parameters": {
                    "classification": "csv",
                    "compressionType": str(compression).lower(),
                    "typeOfData": "file",
                    "delimiter": sep,
                    "columnsOrdered": "true",
                    "areColumnsQuoted": "false",
                },
            },
        }

    @staticmethod
    def _csv_partition_definition(partition, compression, extra_args=None):
        if extra_args is None:
            extra_args = {}
        compressed = False if compression is None else True
        sep = extra_args["sep"] if "sep" in extra_args else ","
        sep = "," if sep is None else sep
        serde = extra_args.get("serde")
        if serde == "OpenCSVSerDe":
            serde_fullname = "org.apache.hadoop.hive.serde2.OpenCSVSerde"
            param = {
                "separatorChar": sep,
                "quoteChar": "\"",
                "escapeChar": "\\",
            }
        elif serde == "LazySimpleSerDe":
            serde_fullname = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
            param = {"field.delim": sep, "escape.delim": "\\"}
        else:
            raise InvalidSerDe(f"{serde} in not in the valid SerDe list.")
        return {
            "StorageDescriptor": {
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "Location": partition[0],
                "Compressed": compressed,
                "SerdeInfo": {
                    "Parameters": param,
                    "SerializationLibrary": serde_fullname,
                },
                "StoredAsSubDirectories": False,
            },
            "Values": partition[1],
        }

    @staticmethod
    def _parquet_table_definition(table: str, partition_cols_schema: List[Tuple[str, str]],
                                  schema: List[Tuple[str, str]], path: str, compression: Optional[str]):
        if not partition_cols_schema:
            partition_cols_schema = []
        compressed = False if compression is None else True
        return {
            "Name": table,
            "PartitionKeys": [{
                "Name": x[0],
                "Type": x[1]
            } for x in partition_cols_schema],
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {
                "classification": "parquet",
                "compressionType": str(compression).lower(),
                "typeOfData": "file",
            },
            "StorageDescriptor": {
                "Columns": [{
                    "Name": x[0],
                    "Type": x[1]
                } for x in schema],
                "Location": path,
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "Compressed": compressed,
                "NumberOfBuckets": -1,
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                    "Parameters": {
                        "serialization.format": "1"
                    },
                },
                "StoredAsSubDirectories": False,
                "SortColumns": [],
                "Parameters": {
                    "CrawlerSchemaDeserializerVersion": "1.0",
                    "classification": "parquet",
                    "compressionType": str(compression).lower(),
                    "typeOfData": "file",
                },
            },
        }

    @staticmethod
    def _parquet_partition_definition(partition, compression):
        compressed = False if compression is None else True
        return {
            "StorageDescriptor": {
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "Location": partition[0],
                "Compressed": compressed,
                "SerdeInfo": {
                    "Parameters": {
                        "serialization.format": "1"
                    },
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
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
            Glue._parse_partition_values(path=path, partition_cols=partition_cols),
        ) for path in paths]

    @staticmethod
    def _parse_partition_values(path, partition_cols):
        return [re.search(f"/{col}=(.*?)/", path).group(1) for col in partition_cols]

    def get_table_location(self, database: str, table: str):
        """
        Get table's location on Glue catalog.

        :param database: Database name
        :param table: table name
        """
        res: Dict = self._client_glue.get_table(DatabaseName=database, Name=table)
        try:
            return res["Table"]["StorageDescriptor"]["Location"]
        except KeyError:
            raise InvalidTable(f"{database}.{table}")

    def get_databases(self, catalog_id: Optional[str] = None) -> Iterator[Dict[str, Any]]:
        """
        Get an iterator of databases.

        :param catalog_id: The ID of the Data Catalog from which to retrieve Databases. If none is provided, the AWS account ID is used by default.
        :return: Iterator[Dict[str, Any]] of Databases
        """
        paginator = self._client_glue.get_paginator("get_databases")
        if catalog_id is None:
            response_iterator = paginator.paginate()
        else:
            response_iterator = paginator.paginate(CatalogId=catalog_id)
        for page in response_iterator:
            for db in page["DatabaseList"]:
                yield db

    def get_tables(self,
                   catalog_id: Optional[str] = None,
                   database: Optional[str] = None,
                   name_contains: Optional[str] = None,
                   name_prefix: Optional[str] = None,
                   name_suffix: Optional[str] = None) -> Iterator[Dict[str, Any]]:
        """
        Get an iterator of tables.

        :param catalog_id: The ID of the Data Catalog from which to retrieve Databases. If none is provided, the AWS account ID is used by default.
        :param database: Filter a specific database
        :param name_contains: Select by a specific string on table name
        :param name_prefix: Select by a specific prefix on table name
        :param name_suffix: Select by a specific suffix on table name
        :return: Iterator[Dict[str, Any]] of Tables
        """
        paginator = self._client_glue.get_paginator("get_tables")
        args: Dict[str, str] = {}
        if catalog_id is not None:
            args["CatalogId"] = catalog_id
        if (name_prefix is not None) and (name_suffix is not None) and (name_contains is not None):
            args["Expression"] = f"{name_prefix}.*{name_contains}.*{name_suffix}"
        elif (name_prefix is not None) and (name_suffix is not None):
            args["Expression"] = f"{name_prefix}.*{name_suffix}"
        elif name_contains is not None:
            args["Expression"] = f".*{name_contains}.*"
        elif name_prefix is not None:
            args["Expression"] = f"{name_prefix}.*"
        elif name_suffix is not None:
            args["Expression"] = f".*{name_suffix}"
        if database is not None:
            databases = [database]
        else:
            databases = [x["Name"] for x in self.get_databases(catalog_id=catalog_id)]
        for db in databases:
            args["DatabaseName"] = db
            response_iterator = paginator.paginate(**args)
            for page in response_iterator:
                for tbl in page["TableList"]:
                    yield tbl

    def tables(self,
               limit: int = 100,
               catalog_id: Optional[str] = None,
               database: Optional[str] = None,
               search_text: Optional[str] = None,
               name_contains: Optional[str] = None,
               name_prefix: Optional[str] = None,
               name_suffix: Optional[str] = None) -> DataFrame:
        """
        Get a Dataframe with tables filtered by a search term, prefix, suffix.

        :param limit: Max number of tables
        :param catalog_id: The ID of the Data Catalog from which to retrieve Databases. If none is provided, the AWS account ID is used by default.
        :param database: Glue database name
        :param search_text: Select only tables with the given string in table's properties
        :param name_contains: Select by a specific string on table name
        :param name_prefix: Select only tables with the given string in the name prefix
        :param name_suffix: Select only tables with the given string in the name suffix
        :return: Pandas Dataframe filled by formatted infos
        """
        if search_text is None:
            table_iter = self.get_tables(catalog_id=catalog_id,
                                         database=database,
                                         name_contains=name_contains,
                                         name_prefix=name_prefix,
                                         name_suffix=name_suffix)
            tables: List[Dict[str, Any]] = list(islice(table_iter, limit))
        else:
            tables = list(self.search_tables(text=search_text, catalog_id=catalog_id))
            if database is not None:
                tables = [x for x in tables if x["DatabaseName"] == database]
            if name_contains is not None:
                tables = [x for x in tables if name_contains in x["Name"]]
            if name_prefix is not None:
                tables = [x for x in tables if x["Name"].startswith(name_prefix)]
            if name_suffix is not None:
                tables = [x for x in tables if x["Name"].endswith(name_suffix)]
            tables = tables[:limit]

        df_dict: Dict[str, List] = {"Database": [], "Table": [], "Description": [], "Columns": [], "Partitions": []}
        for table in tables:
            df_dict["Database"].append(table["DatabaseName"])
            df_dict["Table"].append(table["Name"])
            if "Description" in table:
                df_dict["Description"].append(table["Description"])
            else:
                df_dict["Description"].append("")
            df_dict["Columns"].append(", ".join([x["Name"] for x in table["StorageDescriptor"]["Columns"]]))
            df_dict["Partitions"].append(", ".join([x["Name"] for x in table["PartitionKeys"]]))
        return DataFrame(data=df_dict)

    def search_tables(self, text: str, catalog_id: Optional[str] = None):
        """
        Get iterator of tables filtered by a search string.

        :param text: Select only tables with the given string in table's properties.
        :param catalog_id: The ID of the Data Catalog from which to retrieve Databases. If none is provided, the AWS account ID is used by default.
        :return: Iterator of tables
        """
        args: Dict[str, Any] = {"SearchText": text}
        if catalog_id is not None:
            args["CatalogId"] = catalog_id
        response = self._client_glue.search_tables(**args)
        for tbl in response["TableList"]:
            yield tbl
        while "NextToken" in response:
            args["NextToken"] = response["NextToken"]
            response = self._client_glue.search_tables(**args)
            for tbl in response["TableList"]:
                yield tbl

    def databases(self, limit: int = 100, catalog_id: Optional[str] = None) -> DataFrame:
        """
        Get iterator of databases.

        :param limit: Max number of tables
        :param catalog_id: The ID of the Data Catalog from which to retrieve Databases. If none is provided, the AWS account ID is used by default.
        :return: Pandas Dataframe filled by formatted infos
        """
        database_iter = self.get_databases(catalog_id=catalog_id)
        dbs = islice(database_iter, limit)
        df_dict: Dict[str, List] = {"Database": [], "Description": []}
        for db in dbs:
            df_dict["Database"].append(db["Name"])
            if "Description" in db:
                df_dict["Description"].append(db["Description"])
            else:
                df_dict["Description"].append("")
        return DataFrame(data=df_dict)

    def table(self, database: str, name: str, catalog_id: Optional[str] = None) -> DataFrame:
        """
        Get table details as Pandas Dataframe.

        :param database: Glue database name
        :param name: Table name
        :param catalog_id: The ID of the Data Catalog from which to retrieve Databases. If none is provided, the AWS account ID is used by default.
        :return: Pandas Dataframe filled by formatted infos
        """
        if catalog_id is None:
            table: Dict[str, Any] = self._client_glue.get_table(DatabaseName=database, Name=name)["Table"]
        else:
            table = self._client_glue.get_table(CatalogId=catalog_id, DatabaseName=database, Name=name)["Table"]
        df_dict: Dict[str, List] = {"Column Name": [], "Type": [], "Partition": [], "Comment": []}
        for col in table["StorageDescriptor"]["Columns"]:
            df_dict["Column Name"].append(col["Name"])
            df_dict["Type"].append(col["Type"])
            df_dict["Partition"].append(False)
            if "Comment" in col:
                df_dict["Comment"].append(col["Comment"])
            else:
                df_dict["Comment"].append("")
        for col in table["PartitionKeys"]:
            df_dict["Column Name"].append(col["Name"])
            df_dict["Type"].append(col["Type"])
            df_dict["Partition"].append(True)
            if "Comment" in table:
                df_dict["Comment"].append(table["Comment"])
            else:
                df_dict["Comment"].append("")
        return DataFrame(data=df_dict)

    def get_connection(self,
                       name: str,
                       application_name: str = "aws-data-wrangler",
                       connection_timeout: int = 1_200_000,
                       validation_timeout: int = 5) -> Any:
        """
        Generate a valid connection object (PEP 249 compatible).

        :param name: Glue connection name
        :param application_name: Application name
        :param connection_timeout: Connection Timeout
        :param validation_timeout: Timeout to try to validate the connection
        :return: PEP 249 compatible connection
        """
        details: Dict[str, Any] = self._session.glue.get_connection_details(name=name)["ConnectionProperties"]
        engine: str = details["JDBC_CONNECTION_URL"].split(":")[1]
        host: str = details["JDBC_CONNECTION_URL"].split(":")[2].replace("/", "")
        port, database = details["JDBC_CONNECTION_URL"].split(":")[3].split("/")
        user: str = details["USERNAME"]
        password: str = details["PASSWORD"]
        if engine == "redshift":
            return Redshift.generate_connection(database=database,
                                                host=host,
                                                port=int(port),
                                                user=user,
                                                password=password,
                                                application_name=application_name,
                                                connection_timeout=connection_timeout,
                                                validation_timeout=validation_timeout)
        else:
            return Aurora.generate_connection(database=database,
                                              host=host,
                                              port=int(port),
                                              user=user,
                                              password=password,
                                              engine=engine,
                                              application_name=application_name,
                                              connection_timeout=connection_timeout,
                                              validation_timeout=validation_timeout)
