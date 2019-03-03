import multiprocessing as mp

from awswrangler.exceptions import UnsupportedFileFormat, UnsupportedWriteMode
from awswrangler.common import SessionPrimitives
from awswrangler.s3.utils import delete_objects
from awswrangler.s3.write.manager import write_dataset_manager, write_file_manager
from awswrangler.s3.write.metadata import write_metadata


def _write_data(
    df,
    path,
    session_primitives,
    partition_cols=None,
    preserve_index=True,
    file_format="parquet",
    mode="append",
    num_procs=None,
):
    """
    Write the parquet files to s3
    """
    if not num_procs:
        num_procs = mp.cpu_count()
    if path[-1] == "/":
        path = path[:-1]
    file_format = file_format.lower()
    if file_format not in ["parquet", "csv"]:
        raise UnsupportedFileFormat(file_format)
    partition_paths = None

    if partition_cols is not None and len(partition_cols) > 0:
        partition_paths = write_dataset_manager(
            df=df,
            path=path,
            partition_cols=partition_cols,
            session_primitives=session_primitives,
            preserve_index=preserve_index,
            file_format=file_format,
            mode=mode,
            num_procs=num_procs,
        )
    else:
        write_file_manager(
            df=df,
            path=path,
            preserve_index=preserve_index,
            session_primitives=session_primitives,
            file_format=file_format,
            num_procs=num_procs,
        )

    return partition_paths


def write(
    df,
    path,
    database=None,
    table=None,
    partition_cols=None,
    preserve_index=True,
    file_format="parquet",
    mode="append",
    region=None,
    key=None,
    secret=None,
    profile=None,
    num_procs=None,
):
    """
    Convert a given Pandas Dataframe to a Glue Parquet table
    """
    session_primitives = SessionPrimitives(
        region=region, key=key, secret=secret, profile=profile
    )
    if mode == "overwrite" or (mode == "overwrite_partitions" and not partition_cols):
        delete_objects(path, session_primitives=session_primitives)
    elif mode not in ["overwrite_partitions", "append"]:
        raise UnsupportedWriteMode(mode)
    partition_paths = _write_data(
        df=df,
        path=path,
        partition_cols=partition_cols,
        preserve_index=preserve_index,
        file_format=file_format,
        mode=mode,
        session_primitives=session_primitives,
        num_procs=num_procs,
    )
    if database:
        write_metadata(
            df=df,
            path=path,
            session_primitives=session_primitives,
            partition_paths=partition_paths,
            database=database,
            table=table,
            partition_cols=partition_cols,
            preserve_index=preserve_index,
            file_format=file_format,
            mode=mode,
        )
