import pg

from awswrangler.exceptions import UnsupportedType, RedshiftLoadError
from awswrangler.glue.utils import get_connection_details


def get_redshift_connection(glue_connection, session_primitives=None):
    conn_details = get_connection_details(
        name=glue_connection, session_primitives=session_primitives
    )
    props = conn_details["ConnectionProperties"]
    host = props["JDBC_CONNECTION_URL"].split(":")[2].replace("/", "")
    port, dbname = props["JDBC_CONNECTION_URL"].split(":")[3].split("/")
    user = props["USERNAME"]
    password = props["PASSWORD"]
    conn = pg.DB(dbname=dbname, host=host, port=int(port), user=user, passwd=password)
    conn.query("set statement_timeout = 1200000")
    return conn


def get_number_of_slices(redshift_conn):
    res = redshift_conn.query(
        "SELECT COUNT(*) as count_slices FROM (SELECT DISTINCT node, slice from STV_SLICES)"
    )
    count_slices = res.dictresult()[0]["count_slices"]
    return count_slices


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


def _get_redshift_schema(df, preserve_index=False):
    schema_built = []
    if preserve_index:
        name = str(df.index.name) if df.index.name else "index"
        df.index.name = "index"
        dtype = str(df.index.dtype)
        redshift_type = _type_pandas2redshift(dtype)
        schema_built.append((name, redshift_type))
    for col in df.columns:
        name = str(col)
        dtype = str(df[name].dtype)
        redshift_type = _type_pandas2redshift(dtype)
        schema_built.append((name, redshift_type))
    return schema_built


def load_table(
    df,
    path,
    schema_name,
    table_name,
    redshift_conn,
    num_files,
    iam_role,
    mode="append",
    preserve_index=False,
):
    redshift_conn.begin()
    if mode == "overwrite":
        redshift_conn.query(
            "-- AWS DATA WRANGLER\n" f"DROP TABLE IF EXISTS {schema_name}.{table_name}"
        )
    schema = _get_redshift_schema(df=df, preserve_index=preserve_index)
    cols_str = "".join([f"{col[0]} {col[1]},\n" for col in schema])[:-2]
    sql = (
        "-- AWS DATA WRANGLER\n"
        f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (\n{cols_str}"
        ") DISTSTYLE AUTO"
    )
    redshift_conn.query(sql)
    if path[-1] != "/":
        path += "/"
    sql = (
        "-- AWS DATA WRANGLER\n"
        f"COPY {schema_name}.{table_name} FROM '{path}'\n"
        f"IAM_ROLE '{iam_role}'\n"
        "FORMAT AS PARQUET"
    )
    redshift_conn.query(sql)
    res = redshift_conn.query(
        "-- AWS DATA WRANGLER\n SELECT pg_last_copy_id() AS query_id"
    )
    query_id = res.dictresult()[0]["query_id"]
    sql = (
        "-- AWS DATA WRANGLER\n"
        f"SELECT COUNT(*) as num_files_loaded FROM STL_LOAD_COMMITS WHERE query = {query_id}"
    )
    res = redshift_conn.query(sql)
    num_files_loaded = res.dictresult()[0]["num_files_loaded"]
    if num_files_loaded != num_files:
        redshift_conn.rollback()
        raise RedshiftLoadError(
            f"Redshift load rollbacked. {num_files_loaded} files counted. {num_files} expected."
        )
    redshift_conn.commit()
