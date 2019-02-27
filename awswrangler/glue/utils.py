import sys
from math import ceil

from ..common import get_session
from ..exceptions import UnsupportedFileFormat
from .parquet import (
    get_partition_definition as p_get_partition_definition,
    get_table_definition as p_get_table_definition,
)
from .csv import (
    get_partition_definition as c_get_partition_definition,
    get_table_definition as c_get_table_definition,
)


if sys.version_info.major > 2:
    xrange = range


def table_exists(database, table, session_primitives=None):
    """
    Check if a specific Glue table exists
    """
    client = get_session(session_primitives=session_primitives).client("glue")
    try:
        client.get_table(DatabaseName=database, Name=table)
        return True
    except client.exceptions.EntityNotFoundException:
        return False


def add_partitions(
    database, table, partition_paths, file_format, session_primitives=None
):
    """
    Add a list of partitions in a Glue table
    """
    client = get_session(session_primitives=session_primitives).client("glue")
    if not partition_paths:
        return None
    partitions = list()
    for partition in partition_paths:
        if file_format == "parquet":
            partition_def = p_get_partition_definition(partition)
        elif file_format == "csv":
            partition_def = c_get_partition_definition(partition)
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


def create_table(
    database, table, schema, partition_cols, path, file_format, session_primitives=None
):
    """
    Create Glue table
    """
    client = get_session(session_primitives=session_primitives).client("glue")
    if file_format == "parquet":
        table_input = p_get_table_definition(table, partition_cols, schema, path)
    elif file_format == "csv":
        table_input = c_get_table_definition(table, partition_cols, schema, path)
    else:
        raise UnsupportedFileFormat(file_format)
    client.create_table(DatabaseName=database, TableInput=table_input)


def delete_table_if_exists(database, table, session_primitives=None):
    client = get_session(session_primitives=session_primitives).client("glue")
    try:
        client.delete_table(DatabaseName=database, Name=table)
    except client.exceptions.EntityNotFoundException:
        pass
