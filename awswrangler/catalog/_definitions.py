"""AWS Glue Catalog Delete Module."""

import logging
from typing import Any, Dict, List, Optional, Tuple

_logger: logging.Logger = logging.getLogger(__name__)

_LEGAL_COLUMN_TYPES = [
    "array",
    "bigint",
    "binary",
    "boolean",
    "char",
    "date",
    "decimal",
    "double",
    "float",
    "int",
    "interval",
    "map",
    "set",
    "smallint",
    "string",
    "struct",
    "timestamp",
    "tinyint",
]


def _parquet_table_definition(
    table: str,
    path: str,
    columns_types: Dict[str, str],
    partitions_types: Dict[str, str],
    bucketing_info: Optional[Tuple[List[str], int]],
    compression: Optional[str],
) -> Dict[str, Any]:
    compressed: bool = compression is not None
    return {
        "Name": table,
        "PartitionKeys": [{"Name": cname, "Type": dtype} for cname, dtype in partitions_types.items()],
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {"classification": "parquet", "compressionType": str(compression).lower(), "typeOfData": "file"},
        "StorageDescriptor": {
            "Columns": [{"Name": cname, "Type": dtype} for cname, dtype in columns_types.items()],
            "Location": path,
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "Compressed": compressed,
            "NumberOfBuckets": -1 if bucketing_info is None else bucketing_info[1],
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {"serialization.format": "1"},
            },
            "BucketColumns": [] if bucketing_info is None else bucketing_info[0],
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


def _parquet_partition_definition(
    location: str,
    values: List[str],
    bucketing_info: Optional[Tuple[List[str], int]],
    compression: Optional[str],
    columns_types: Optional[Dict[str, str]],
) -> Dict[str, Any]:
    compressed: bool = compression is not None
    definition: Dict[str, Any] = {
        "StorageDescriptor": {
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "Location": location,
            "Compressed": compressed,
            "SerdeInfo": {
                "Parameters": {"serialization.format": "1"},
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            },
            "StoredAsSubDirectories": False,
            "NumberOfBuckets": -1 if bucketing_info is None else bucketing_info[1],
            "BucketColumns": [] if bucketing_info is None else bucketing_info[0],
        },
        "Values": values,
    }
    if columns_types is not None:
        definition["StorageDescriptor"]["Columns"] = [
            {"Name": cname, "Type": dtype} for cname, dtype in columns_types.items()
        ]
    return definition


def _csv_table_definition(
    table: str,
    path: str,
    columns_types: Dict[str, str],
    partitions_types: Dict[str, str],
    bucketing_info: Optional[Tuple[List[str], int]],
    compression: Optional[str],
    sep: str,
    skip_header_line_count: Optional[int],
) -> Dict[str, Any]:
    compressed: bool = compression is not None
    parameters: Dict[str, str] = {
        "classification": "csv",
        "compressionType": str(compression).lower(),
        "typeOfData": "file",
        "delimiter": sep,
        "columnsOrdered": "true",
        "areColumnsQuoted": "false",
    }
    if skip_header_line_count is not None:
        parameters["skip.header.line.count"] = "1"
    return {
        "Name": table,
        "PartitionKeys": [{"Name": cname, "Type": dtype} for cname, dtype in partitions_types.items()],
        "TableType": "EXTERNAL_TABLE",
        "Parameters": parameters,
        "StorageDescriptor": {
            "Columns": [{"Name": cname, "Type": dtype} for cname, dtype in columns_types.items()],
            "Location": path,
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "Compressed": compressed,
            "NumberOfBuckets": -1 if bucketing_info is None else bucketing_info[1],
            "SerdeInfo": {
                "Parameters": {"field.delim": sep, "escape.delim": "\\"},
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
            },
            "BucketColumns": [] if bucketing_info is None else bucketing_info[0],
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


def _csv_partition_definition(
    location: str,
    values: List[str],
    bucketing_info: Optional[Tuple[List[str], int]],
    compression: Optional[str],
    sep: str,
    columns_types: Optional[Dict[str, str]],
) -> Dict[str, Any]:
    compressed: bool = compression is not None
    definition: Dict[str, Any] = {
        "StorageDescriptor": {
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "Location": location,
            "Compressed": compressed,
            "SerdeInfo": {
                "Parameters": {"field.delim": sep, "escape.delim": "\\"},
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
            },
            "StoredAsSubDirectories": False,
            "NumberOfBuckets": -1 if bucketing_info is None else bucketing_info[1],
            "BucketColumns": [] if bucketing_info is None else bucketing_info[0],
        },
        "Values": values,
    }
    if columns_types is not None:
        definition["StorageDescriptor"]["Columns"] = [
            {"Name": cname, "Type": dtype} for cname, dtype in columns_types.items()
        ]
    return definition


def _check_column_type(column_type: str) -> bool:
    if column_type not in _LEGAL_COLUMN_TYPES:
        raise ValueError(f"{column_type} is not a legal data type.")
    return True


def _update_table_definition(current_definition: Dict[str, Any]) -> Dict[str, Any]:
    definition: Dict[str, Any] = dict()
    keep_keys = [
        "Name",
        "Description",
        "Owner",
        "LastAccessTime",
        "LastAnalyzedTime",
        "Retention",
        "StorageDescriptor",
        "PartitionKeys",
        "ViewOriginalText",
        "ViewExpandedText",
        "TableType",
        "Parameters",
        "TargetTable",
    ]
    for key in current_definition["Table"]:
        if key in keep_keys:
            definition[key] = current_definition["Table"][key]
    return definition
