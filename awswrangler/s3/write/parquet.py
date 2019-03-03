import pyarrow as pa
import pyarrow.parquet as pq


def write_parquet_dataframe(df, path, preserve_index, fs):
    table = pa.Table.from_pandas(df, preserve_index=preserve_index, safe=False)
    with fs.open(path, "wb") as f:
        pq.write_table(table, f, coerce_timestamps="ms")
