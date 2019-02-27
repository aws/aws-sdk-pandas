def get_table_definition(table, partition_cols, schema, path):
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


def get_partition_definition(partition):
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
