import multiprocessing as mp

from awswrangler import s3
from awswrangler.s3.utils import delete_objects
from awswrangler.common import SessionPrimitives, lcm
from awswrangler.redshift.utils import (
    get_redshift_connection,
    get_number_of_slices,
    load_table,
)


def write(
    df,
    path,
    glue_connection,
    schema,
    table,
    iam_role,
    preserve_index=False,
    mode="append",
    region=None,
    key=None,
    secret=None,
    profile=None,
    num_procs=None,
):
    if not num_procs:
        num_procs = mp.cpu_count()
    session_primitives = SessionPrimitives(
        region=region, key=key, secret=secret, profile=profile
    )
    conn = get_redshift_connection(
        glue_connection=glue_connection, session_primitives=session_primitives
    )
    num_slices = get_number_of_slices(redshift_conn=conn)
    num_files_per_core = int(lcm(num_procs, num_slices) / num_procs)
    s3.write(
        df=df,
        path=path,
        preserve_index=preserve_index,
        file_format="parquet",
        mode="overwrite",
        region=region,
        key=key,
        secret=secret,
        profile=profile,
        num_procs=num_procs,
        num_files=num_files_per_core,
    )
    num_files_total = num_files_per_core * num_procs
    load_table(
        df=df,
        path=path,
        schema_name=schema,
        table_name=table,
        redshift_conn=conn,
        preserve_index=False,
        num_files=num_files_total,
        iam_role=iam_role,
        mode=mode,
    )
    delete_objects(path=path, session_primitives=session_primitives)
