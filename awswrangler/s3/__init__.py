from .write.write import write as _write
from .read.read import read as _read


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
    num_files=2,
):
    return _write(**locals())


def read(
    path,
    file_format="csv",
    header="infer",
    names=None,
    dtype=None,
    sep=",",
    lineterminator="\n",
    quotechar='"',
    quoting=0,
    escapechar=None,
    parse_dates=False,
    infer_datetime_format=False,
    encoding=None,
    region=None,
    key=None,
    secret=None,
    profile=None,
):
    return _read(**locals())
