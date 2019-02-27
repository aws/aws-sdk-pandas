import pyarrow as pa
from pyarrow.compat import guid
import pyarrow.parquet as pq

from .utils import mkdir_if_not_exists, delete_listed_objects, list_objects


def write(df, fs, path, preserve_index):
    outfile = guid() + ".parquet"
    full_path = "/".join([path, outfile])
    table = pa.Table.from_pandas(df, preserve_index=preserve_index)
    with fs.open(full_path, "wb") as f:
        pq.write_table(table, f, coerce_timestamps="ms")


def write_dataset(
    df, fs, path, partition_cols, preserve_index, session_primitives, mode
):
    partition_paths = []
    dead_keys = []
    for keys, subgroup in df.groupby(partition_cols):
        subgroup = subgroup.drop(partition_cols, axis="columns")
        if not isinstance(keys, tuple):
            keys = (keys,)
        subdir = "/".join(
            [
                "{colname}={value}".format(colname=name, value=val)
                for name, val in zip(partition_cols, keys)
            ]
        )
        subtable = pa.Table.from_pandas(
            subgroup, preserve_index=preserve_index, safe=False
        )
        prefix = "/".join([path, subdir])
        if mode == "overwrite_partitions":
            dead_keys += list_objects(prefix, session_primitives=session_primitives)
        mkdir_if_not_exists(fs, prefix)
        outfile = guid() + ".parquet"
        full_path = "/".join([prefix, outfile])
        with fs.open(full_path, "wb") as f:
            pq.write_table(subtable, f, coerce_timestamps="ms")
        partition_path = full_path.rpartition("/")[0] + "/"
        keys_str = [str(x) for x in keys]
        partition_paths.append((partition_path, keys_str))
    if mode == "overwrite_partitions" and dead_keys:
        bucket = path.replace("s3://", "").split("/", 1)[0]
        delete_listed_objects(bucket, dead_keys, session_primitives=session_primitives)
    return partition_paths
