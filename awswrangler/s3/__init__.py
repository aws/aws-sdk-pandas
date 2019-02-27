from . import write as _write


def write(
    df,
    database,
    path,
    table=None,
    partition_cols=[],
    preserve_index=False,
    file_format="parquet",
    mode="append",
    region=None,
    key=None,
    secret=None,
    profile=None,
):
    return _write.write(**locals())
