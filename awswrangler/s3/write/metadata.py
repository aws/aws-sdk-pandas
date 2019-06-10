from awswrangler.exceptions import UnsupportedType
from awswrangler.glue.utils import (
    delete_table_if_exists,
    create_table,
    add_partitions,
    table_exists,
)


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


def _build_schema(df, partition_cols, preserve_index):
    schema_built = []
    if preserve_index:
        name = str(df.index.name) if df.index.name else "index"
        df.index.name = "index"
        dtype = str(df.index.dtype)
        if name not in partition_cols:
            athena_type = _type_pandas2athena(dtype)
            schema_built.append((name, athena_type))
    for col in df.columns:
        name = str(col)
        dtype = str(df[name].dtype)
        if name not in partition_cols:
            athena_type = _type_pandas2athena(dtype)
            schema_built.append((name, athena_type))
    return schema_built


def _get_table_name(path):
    if path[-1] == "/":
        path = path[:-1]
    return path.rpartition("/")[2]


def write_metadata(
    df,
    path,
    session_primitives,
    partition_paths,
    database=None,
    table=None,
    partition_cols=None,
    preserve_index=True,
    file_format="parquet",
    mode="append",
):
    schema = _build_schema(
        df=df, partition_cols=partition_cols, preserve_index=preserve_index
    )
    table = table if table else _get_table_name(path)
    if mode == "overwrite":
        delete_table_if_exists(
            database=database, table=table, session_primitives=session_primitives
        )
    exists = table_exists(
        database=database, table=table, session_primitives=session_primitives
    )
    if not exists:
        create_table(
            database=database,
            table=table,
            schema=schema,
            partition_cols=partition_cols,
            path=path,
            file_format=file_format,
            session_primitives=session_primitives,
        )
    add_partitions(
        database=database,
        table=table,
        partition_paths=partition_paths,
        file_format=file_format,
        session_primitives=session_primitives,
    )
