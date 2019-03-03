from .write.write import write as _write


def write(
    df,
    path,
    database=None,
    table=None,
    partition_cols=[],
    preserve_index=False,
    file_format="parquet",
    mode="append",
    region=None,
    key=None,
    secret=None,
    profile=None,
    num_procs=None,
):
    return _write(**locals())
