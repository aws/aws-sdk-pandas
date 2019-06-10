import multiprocessing as mp

from pyarrow.compat import guid
from pyarrow.filesystem import _ensure_filesystem

from awswrangler.common import calculate_bounders
from awswrangler.s3.utils import (
    mkdir_if_not_exists,
    delete_listed_objects,
    list_objects,
    get_fs,
)
from awswrangler.s3.write.parquet import write_parquet_dataframe
from awswrangler.s3.write.csv import write_csv_dataframe


def _get_bounders(df, num_partitions):
    num_rows = len(df.index)
    return calculate_bounders(num_items=num_rows, num_groups=num_partitions)


def write_files(df, path, preserve_index, session_primitives, file_format):
    fs = get_fs(session_primitives=session_primitives)
    fs = _ensure_filesystem(fs)
    mkdir_if_not_exists(fs, path)
    if file_format == "parquet":
        outfile = guid() + ".parquet"
    elif file_format == "csv":
        outfile = guid() + ".csv"
    full_path = "/".join([path, outfile])
    if file_format == "parquet":
        write_parquet_dataframe(
            df=df, path=full_path, preserve_index=preserve_index, fs=fs
        )
    elif file_format == "csv":
        write_csv_dataframe(df=df, path=full_path, preserve_index=preserve_index, fs=fs)
    return full_path


def write_files_manager(
    df, path, preserve_index, session_primitives, file_format, num_procs, num_files=2
):
    if num_procs > 1:
        bounders = _get_bounders(df=df, num_partitions=num_procs * num_files)
        for counter in range(num_files):
            procs = []
            for bounder in bounders[
                counter * num_procs : (counter * num_procs) + num_procs
            ]:
                proc = mp.Process(
                    target=write_files,
                    args=(
                        df.iloc[bounder[0] : bounder[1], :],
                        path,
                        preserve_index,
                        session_primitives,
                        file_format,
                    ),
                )
                proc.daemon = True
                proc.start()
                procs.append(proc)
        for i in range(len(procs)):
            procs[i].join()
    else:
        write_files(
            df=df,
            path=path,
            preserve_index=preserve_index,
            session_primitives=session_primitives,
            file_format=file_format,
        )


def write_dataset(
    df, path, partition_cols, preserve_index, session_primitives, file_format, mode
):
    fs = get_fs(session_primitives=session_primitives)
    fs = _ensure_filesystem(fs)
    mkdir_if_not_exists(fs, path)
    partition_paths = []
    dead_keys = []
    for keys, subgroup in df.groupby(partition_cols):
        subgroup = subgroup.drop(partition_cols, axis="columns")
        if not isinstance(keys, tuple):
            keys = (keys,)
        subdir = "/".join([f"{name}={val}" for name, val in zip(partition_cols, keys)])
        prefix = "/".join([path, subdir])
        if mode == "overwrite_partitions":
            dead_keys += list_objects(prefix, session_primitives=session_primitives)
        full_path = write_files(
            df=subgroup,
            path=prefix,
            preserve_index=preserve_index,
            session_primitives=session_primitives,
            file_format=file_format,
        )
        partition_path = full_path.rpartition("/")[0] + "/"
        keys_str = [str(x) for x in keys]
        partition_paths.append((partition_path, keys_str))
    if mode == "overwrite_partitions" and dead_keys:
        bucket = path.replace("s3://", "").split("/", 1)[0]
        delete_listed_objects(bucket, dead_keys, session_primitives=session_primitives)
    return partition_paths


def write_dataset_remote(
    send_pipe,
    df,
    path,
    partition_cols,
    preserve_index,
    session_primitives,
    file_format,
    mode,
):
    send_pipe.send(
        write_dataset(
            df=df,
            path=path,
            partition_cols=partition_cols,
            preserve_index=preserve_index,
            session_primitives=session_primitives,
            file_format=file_format,
            mode=mode,
        )
    )
    send_pipe.close()


def write_dataset_manager(
    df,
    path,
    partition_cols,
    session_primitives,
    preserve_index,
    file_format,
    mode,
    num_procs,
    num_files=2,
):
    partition_paths = []
    if num_procs > 1:
        bounders = _get_bounders(df=df, num_partitions=num_procs * num_files)
        for counter in range(num_files):
            procs = []
            receive_pipes = []
            for bounder in bounders[
                counter * num_procs : (counter * num_procs) + num_procs
            ]:
                receive_pipe, send_pipe = mp.Pipe()
                proc = mp.Process(
                    target=write_dataset_remote,
                    args=(
                        send_pipe,
                        df.iloc[bounder[0] : bounder[1], :],
                        path,
                        partition_cols,
                        preserve_index,
                        session_primitives,
                        file_format,
                        mode,
                    ),
                )
                proc.daemon = True
                proc.start()
                procs.append(proc)
                receive_pipes.append(receive_pipe)
            for i in range(len(procs)):
                partition_paths += receive_pipes[i].recv()
                procs[i].join()
                receive_pipes[i].close()
    else:
        partition_paths += write_dataset(
            df=df,
            path=path,
            partition_cols=partition_cols,
            preserve_index=preserve_index,
            session_primitives=session_primitives,
            file_format=file_format,
            mode=mode,
        )
    return partition_paths
