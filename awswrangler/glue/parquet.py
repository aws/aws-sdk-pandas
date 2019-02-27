def get_table_definition(table, partition_cols, schema, path):
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


def get_partition_definition(partition):
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
