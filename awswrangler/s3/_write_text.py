"""Amazon S3 Text Write Module (PRIVATE)."""

from __future__ import annotations

import csv
import logging
import uuid
from typing import TYPE_CHECKING, Any, Literal, cast

import boto3
import pandas as pd
from pandas.io.common import infer_compression

from awswrangler import _data_types, _utils, catalog, exceptions, typing
from awswrangler._config import apply_configs
from awswrangler._distributed import engine
from awswrangler._utils import copy_df_shallow
from awswrangler.s3._delete import delete_objects
from awswrangler.s3._fs import open_s3_object
from awswrangler.s3._write import _COMPRESSION_2_EXT, _apply_dtype, _sanitize, _validate_args
from awswrangler.s3._write_dataset import _to_dataset
from awswrangler.typing import BucketingInfoTuple, GlueTableSettings, _S3WriteDataReturnValue

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

_logger: logging.Logger = logging.getLogger(__name__)


def _get_write_details(path: str, pandas_kwargs: dict[str, Any]) -> tuple[str, str | None, str | None]:
    if pandas_kwargs.get("compression", "infer") == "infer":
        pandas_kwargs["compression"] = infer_compression(path, compression="infer")
    mode: str = "w" if pandas_kwargs.get("compression") is None else "wb"
    encoding: str | None = pandas_kwargs.get("encoding", "utf-8")
    newline: str | None = pandas_kwargs.get("lineterminator", "")
    return mode, encoding, newline


@engine.dispatch_on_engine
def _to_text(
    df: pd.DataFrame,
    file_format: str,
    use_threads: bool | int,
    s3_client: "S3Client" | None,
    s3_additional_kwargs: dict[str, str] | None,
    path: str | None = None,
    path_root: str | None = None,
    filename_prefix: str | None = None,
    bucketing: bool = False,
    **pandas_kwargs: Any,
) -> list[str]:
    s3_client = s3_client if s3_client else _utils.client(service_name="s3")
    if df.empty is True:
        _logger.warning("Empty DataFrame will be written.")
    if path is None and path_root is not None:
        file_path: str = (
            f"{path_root}{filename_prefix}.{file_format}{_COMPRESSION_2_EXT.get(pandas_kwargs.get('compression'))}"
        )
    elif path is not None and path_root is None:
        file_path = path
    else:
        raise RuntimeError("path and path_root received at the same time.")

    mode, encoding, newline = _get_write_details(path=file_path, pandas_kwargs=pandas_kwargs)
    with open_s3_object(
        path=file_path,
        mode=mode,
        use_threads=use_threads,
        s3_client=s3_client,
        s3_additional_kwargs=s3_additional_kwargs,
        encoding=encoding,
        newline=newline,
    ) as f:
        _logger.debug("pandas_kwargs: %s", pandas_kwargs)
        if file_format == "csv":
            df.to_csv(f, mode=mode, **pandas_kwargs)
        elif file_format == "json":
            df.to_json(f, **pandas_kwargs)
    return [file_path]


@apply_configs
@_utils.validate_distributed_kwargs(
    unsupported_kwargs=["boto3_session"],
)
def to_csv(  # noqa: PLR0912,PLR0915
    df: pd.DataFrame,
    path: str | None = None,
    sep: str = ",",
    index: bool = True,
    columns: list[str] | None = None,
    use_threads: bool | int = True,
    boto3_session: boto3.Session | None = None,
    s3_additional_kwargs: dict[str, Any] | None = None,
    sanitize_columns: bool = False,
    dataset: bool = False,
    filename_prefix: str | None = None,
    partition_cols: list[str] | None = None,
    bucketing_info: BucketingInfoTuple | None = None,
    concurrent_partitioning: bool = False,
    mode: Literal["append", "overwrite", "overwrite_partitions"] | None = None,
    catalog_versioning: bool = False,
    schema_evolution: bool = False,
    dtype: dict[str, str] | None = None,
    database: str | None = None,
    table: str | None = None,
    glue_table_settings: GlueTableSettings | None = None,
    athena_partition_projection_settings: typing.AthenaPartitionProjectionSettings | None = None,
    catalog_id: str | None = None,
    **pandas_kwargs: Any,
) -> _S3WriteDataReturnValue:
    """Write CSV file or dataset on Amazon S3.

    The concept of Dataset goes beyond the simple idea of ordinary files and enable more
    complex features like partitioning and catalog integration (Amazon Athena/AWS Glue Catalog).

    Note
    ----
    If database` and `table` arguments are passed, the table name and all column names
    will be automatically sanitized using `wr.catalog.sanitize_table_name` and `wr.catalog.sanitize_column_name`.
    Please, pass `sanitize_columns=True` to enforce this behaviour always.

    Note
    ----
    If `table` and `database` arguments are passed, `pandas_kwargs` will be ignored due
    restrictive quoting, date_format, escapechar and encoding required by Athena/Glue Catalog.

    Note
    ----
    In case of `use_threads=True` the number of threads
    that will be spawned will be gotten from os.cpu_count().

    Parameters
    ----------
    df: pandas.DataFrame
        Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    path : str, optional
        Amazon S3 path (e.g. s3://bucket/prefix/filename.csv) (for dataset e.g. ``s3://bucket/prefix``).
        Required if dataset=False or when creating a new dataset
    sep : str
        String of length 1. Field delimiter for the output file.
    index : bool
        Write row names (index).
    columns: list[str], optional
        Columns to write.
    use_threads : bool, int
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
        If integer is provided, specified number is used.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.
    s3_additional_kwargs: dict[str, Any], optional
        Forwarded to botocore requests.
        e.g. s3_additional_kwargs={'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'}
    sanitize_columns : bool
        True to sanitize columns names or False to keep it as is.
        True value is forced if `dataset=True`.
    dataset : bool
        If True store as a dataset instead of ordinary file(s)
        If True, enable all follow arguments:
        partition_cols, mode, database, table, description, parameters, columns_comments, concurrent_partitioning,
        catalog_versioning, projection_params, catalog_id, schema_evolution.
    filename_prefix: str, optional
        If dataset=True, add a filename prefix to the output files.
    partition_cols: List[str], optional
        List of column names that will be used to create partitions. Only takes effect if dataset=True.
    bucketing_info: Tuple[List[str], int], optional
        Tuple consisting of the column names used for bucketing as the first element and the number of buckets as the
        second element.
        Only `str`, `int` and `bool` are supported as column data types for bucketing.
    concurrent_partitioning: bool
        If True will increase the parallelism level during the partitions writing. It will decrease the
        writing time and increase the memory usage.
        https://aws-sdk-pandas.readthedocs.io/en/3.7.3/tutorials/022%20-%20Writing%20Partitions%20Concurrently.html
    mode : str, optional
        ``append`` (Default), ``overwrite``, ``overwrite_partitions``. Only takes effect if dataset=True.
        For details check the related tutorial:
        https://aws-sdk-pandas.readthedocs.io/en/3.7.3/stubs/awswrangler.s3.to_parquet.html#awswrangler.s3.to_parquet
    catalog_versioning : bool
        If True and `mode="overwrite"`, creates an archived version of the table catalog before updating it.
    schema_evolution : bool
        If True allows schema evolution (new or missing columns), otherwise a exception will be raised.
        (Only considered if dataset=True and mode in ("append", "overwrite_partitions")). False by default.
        Related tutorial:
        https://aws-sdk-pandas.readthedocs.io/en/3.7.3/tutorials/014%20-%20Schema%20Evolution.html
    database : str, optional
        Glue/Athena catalog: Database name.
    table : str, optional
        Glue/Athena catalog: Table name.
    glue_table_settings: dict (GlueTableSettings), optional
        Settings for writing to the Glue table.
    dtype : Dict[str, str], optional
        Dictionary of columns names and Athena/Glue types to be casted.
        Useful when you have columns with undetermined or mixed data types.
        (e.g. {'col name': 'bigint', 'col2 name': 'int'})
    athena_partition_projection_settings: typing.AthenaPartitionProjectionSettings, optional
        Parameters of the Athena Partition Projection (https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html).
        AthenaPartitionProjectionSettings is a `TypedDict`, meaning the passed parameter can be instantiated either as an
        instance of AthenaPartitionProjectionSettings or as a regular Python dict.

        Following projection parameters are supported:

        .. list-table:: Projection Parameters
           :header-rows: 1

           * - Name
             - Type
             - Description
           * - projection_types
             - Optional[Dict[str, str]]
             - Dictionary of partitions names and Athena projections types.
               Valid types: "enum", "integer", "date", "injected"
               https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
               (e.g. {'col_name': 'enum', 'col2_name': 'integer'})
           * - projection_ranges
             - Optional[Dict[str, str]]
             - Dictionary of partitions names and Athena projections ranges.
               https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
               (e.g. {'col_name': '0,10', 'col2_name': '-1,8675309'})
           * - projection_values
             - Optional[Dict[str, str]]
             - Dictionary of partitions names and Athena projections values.
               https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
               (e.g. {'col_name': 'A,B,Unknown', 'col2_name': 'foo,boo,bar'})
           * - projection_intervals
             - Optional[Dict[str, str]]
             - Dictionary of partitions names and Athena projections intervals.
               https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
               (e.g. {'col_name': '1', 'col2_name': '5'})
           * - projection_digits
             - Optional[Dict[str, str]]
             - Dictionary of partitions names and Athena projections digits.
               https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
               (e.g. {'col_name': '1', 'col2_name': '2'})
           * - projection_formats
             - Optional[Dict[str, str]]
             - Dictionary of partitions names and Athena projections formats.
               https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
               (e.g. {'col_date': 'yyyy-MM-dd', 'col2_timestamp': 'yyyy-MM-dd HH:mm:ss'})
           * - projection_storage_location_template
             - Optional[str]
             - Value which is allows Athena to properly map partition values if the S3 file locations do not follow
               a typical `.../column=value/...` pattern.
               https://docs.aws.amazon.com/athena/latest/ug/partition-projection-setting-up.html
               (e.g. s3://bucket/table_root/a=${a}/${b}/some_static_subdirectory/${c}/)
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    pandas_kwargs :
        KEYWORD arguments forwarded to pandas.DataFrame.to_csv(). You can NOT pass `pandas_kwargs` explicit, just add
        valid Pandas arguments in the function call and awswrangler will accept it.
        e.g. wr.s3.to_csv(df, path, sep='|', na_rep='NULL', decimal=',')
        https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_csv.html

    Returns
    -------
    wr.typing._S3WriteDataReturnValue
        Dictionary with:
        'paths': List of all stored files paths on S3.
        'partitions_values': Dictionary of partitions added with keys as S3 path locations
        and values as a list of partitions values as str.

    Examples
    --------
    Writing single file

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_csv(
    ...     df=pd.DataFrame({'col': [1, 2, 3]}),
    ...     path='s3://bucket/prefix/my_file.csv',
    ... )
    {
        'paths': ['s3://bucket/prefix/my_file.csv'],
        'partitions_values': {}
    }

    Writing single file with pandas_kwargs

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_csv(
    ...     df=pd.DataFrame({'col': [1, 2, 3]}),
    ...     path='s3://bucket/prefix/my_file.csv',
    ...     sep='|',
    ...     na_rep='NULL',
    ...     decimal=','
    ... )
    {
        'paths': ['s3://bucket/prefix/my_file.csv'],
        'partitions_values': {}
    }

    Writing single file encrypted with a KMS key

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_csv(
    ...     df=pd.DataFrame({'col': [1, 2, 3]}),
    ...     path='s3://bucket/prefix/my_file.csv',
    ...     s3_additional_kwargs={
    ...         'ServerSideEncryption': 'aws:kms',
    ...         'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'
    ...     }
    ... )
    {
        'paths': ['s3://bucket/prefix/my_file.csv'],
        'partitions_values': {}
    }

    Writing partitioned dataset

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_csv(
    ...     df=pd.DataFrame({
    ...         'col': [1, 2, 3],
    ...         'col2': ['A', 'A', 'B']
    ...     }),
    ...     path='s3://bucket/prefix',
    ...     dataset=True,
    ...     partition_cols=['col2']
    ... )
    {
        'paths': ['s3://.../col2=A/x.csv', 's3://.../col2=B/y.csv'],
        'partitions_values: {
            's3://.../col2=A/': ['A'],
            's3://.../col2=B/': ['B']
        }
    }

    Writing partitioned dataset with partition projection

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> from datetime import datetime
    >>> dt = lambda x: datetime.strptime(x, "%Y-%m-%d").date()
    >>> wr.s3.to_csv(
    ...     df=pd.DataFrame({
    ...         "id": [1, 2, 3],
    ...         "value": [1000, 1001, 1002],
    ...         "category": ['A', 'B', 'C'],
    ...     }),
    ...     path='s3://bucket/prefix',
    ...     dataset=True,
    ...     partition_cols=['value', 'category'],
    ...     athena_partition_projection_settings={
    ...        "projection_types": {
    ...             "value": "integer",
    ...             "category": "enum",
    ...         },
    ...         "projection_ranges": {
    ...             "value": "1000,2000",
    ...             "category": "A,B,C",
    ...         },
    ...     },
    ... )
    {
        'paths': [
            's3://.../value=1000/category=A/x.json', ...
        ],
        'partitions_values': {
            's3://.../value=1000/category=A/': [
                '1000',
                'A',
            ], ...
        }
    }

    Writing bucketed dataset

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_csv(
    ...     df=pd.DataFrame({
    ...         'col': [1, 2, 3],
    ...         'col2': ['A', 'A', 'B']
    ...     }),
    ...     path='s3://bucket/prefix',
    ...     dataset=True,
    ...     bucketing_info=(["col2"], 2)
    ... )
    {
        'paths': ['s3://.../x_bucket-00000.csv', 's3://.../col2=B/x_bucket-00001.csv'],
        'partitions_values: {}
    }

    Writing dataset to S3 with metadata on Athena/Glue Catalog.

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_csv(
    ...     df=pd.DataFrame({
    ...         'col': [1, 2, 3],
    ...         'col2': ['A', 'A', 'B']
    ...     }),
    ...     path='s3://bucket/prefix',
    ...     dataset=True,
    ...     partition_cols=['col2'],
    ...     database='default',  # Athena/Glue database
    ...     table='my_table'  # Athena/Glue table
    ... )
    {
        'paths': ['s3://.../col2=A/x.csv', 's3://.../col2=B/y.csv'],
        'partitions_values: {
            's3://.../col2=A/': ['A'],
            's3://.../col2=B/': ['B']
        }
    }

    Writing dataset casting empty column data type

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_csv(
    ...     df=pd.DataFrame({
    ...         'col': [1, 2, 3],
    ...         'col2': ['A', 'A', 'B'],
    ...         'col3': [None, None, None]
    ...     }),
    ...     path='s3://bucket/prefix',
    ...     dataset=True,
    ...     database='default',  # Athena/Glue database
    ...     table='my_table'  # Athena/Glue table
    ...     dtype={'col3': 'date'}
    ... )
    {
        'paths': ['s3://.../x.csv'],
        'partitions_values: {}
    }

    """
    if "pandas_kwargs" in pandas_kwargs:
        raise exceptions.InvalidArgument(
            "You can NOT pass `pandas_kwargs` explicit, just add valid "
            "Pandas arguments in the function call and awswrangler will accept it."
            "e.g. wr.s3.to_csv(df, path, sep='|', na_rep='NULL', decimal=',', compression='gzip')"
        )

    glue_table_settings = cast(
        GlueTableSettings,
        glue_table_settings if glue_table_settings else {},
    )

    table_type = glue_table_settings.get("table_type")
    description = glue_table_settings.get("description")
    parameters = glue_table_settings.get("parameters")
    columns_comments = glue_table_settings.get("columns_comments")
    regular_partitions = glue_table_settings.get("regular_partitions", True)

    _validate_args(
        df=df,
        table=table,
        database=database,
        dataset=dataset,
        path=path,
        partition_cols=partition_cols,
        bucketing_info=bucketing_info,
        mode=mode,
        description=description,
        parameters=parameters,
        columns_comments=columns_comments,
        execution_engine=engine.get(),
    )

    # Initializing defaults
    partition_cols = partition_cols if partition_cols else []
    dtype = dtype if dtype else {}
    partitions_values: dict[str, list[str]] = {}
    mode = "append" if mode is None else mode

    filename_prefix = filename_prefix + uuid.uuid4().hex if filename_prefix else uuid.uuid4().hex
    s3_client = _utils.client(service_name="s3", session=boto3_session)

    # Sanitize table to respect Athena's standards
    if (sanitize_columns is True) or (database is not None and table is not None):
        df, dtype, partition_cols, bucketing_info = _sanitize(
            df=copy_df_shallow(df),
            dtype=dtype,
            partition_cols=partition_cols,
            bucketing_info=bucketing_info,
        )

    # Evaluating dtype
    catalog_table_input: dict[str, Any] | None = None
    if database and table:
        catalog_table_input = catalog._get_table_input(
            database=database,
            table=table,
            boto3_session=boto3_session,
            catalog_id=catalog_id,
        )

        catalog_path: str | None = None
        if catalog_table_input:
            table_type = catalog_table_input["TableType"]
            catalog_path = catalog_table_input.get("StorageDescriptor", {}).get("Location")
        if path is None:
            if catalog_path:
                path = catalog_path
            else:
                raise exceptions.InvalidArgumentValue(
                    "Glue table does not exist in the catalog. Please pass the `path` argument to create it."
                )
        elif path and catalog_path:
            if path.rstrip("/") != catalog_path.rstrip("/"):
                raise exceptions.InvalidArgumentValue(
                    f"The specified path: {path}, does not match the existing Glue catalog table path: {catalog_path}"
                )
        if pandas_kwargs.get("compression") not in ("gzip", "bz2", None):
            raise exceptions.InvalidArgumentCombination(
                "If database and table are given, you must use one of these compressions: gzip, bz2 or None."
            )

    df = _apply_dtype(df=df, dtype=dtype, catalog_table_input=catalog_table_input, mode=mode)

    paths: list[str] = []
    if dataset is False:
        pandas_kwargs["sep"] = sep
        pandas_kwargs["index"] = index
        pandas_kwargs["columns"] = columns
        _to_text(
            df,
            file_format="csv",
            use_threads=use_threads,
            path=path,
            s3_client=s3_client,
            s3_additional_kwargs=s3_additional_kwargs,
            **pandas_kwargs,
        )
        paths = [path]  # type: ignore[list-item]
    else:
        compression: str | None = pandas_kwargs.get("compression", None)
        if database and table:
            quoting: int | None = csv.QUOTE_NONE
            escapechar: str | None = "\\"
            header: bool | list[str] = pandas_kwargs.get("header", False)
            date_format: str | None = "%Y-%m-%d %H:%M:%S.%f"
            pd_kwargs: dict[str, Any] = {}
        else:
            quoting = pandas_kwargs.get("quoting", None)
            escapechar = pandas_kwargs.get("escapechar", None)
            header = pandas_kwargs.get("header", True)
            date_format = pandas_kwargs.get("date_format", None)
            pd_kwargs = pandas_kwargs.copy()
            pd_kwargs.pop("quoting", None)
            pd_kwargs.pop("escapechar", None)
            pd_kwargs.pop("header", None)
            pd_kwargs.pop("date_format", None)
            pd_kwargs.pop("compression", None)

        df = df[columns] if columns else df

        columns_types: dict[str, str] = {}
        partitions_types: dict[str, str] = {}

        if database and table:
            columns_types, partitions_types = _data_types.athena_types_from_pandas_partitioned(
                df=df, index=index, partition_cols=partition_cols, dtype=dtype, index_left=True
            )
            if schema_evolution is False:
                _utils.check_schema_changes(columns_types=columns_types, table_input=catalog_table_input, mode=mode)

            create_table_args: dict[str, Any] = {
                "database": database,
                "table": table,
                "path": path,
                "columns_types": columns_types,
                "table_type": table_type,
                "partitions_types": partitions_types,
                "bucketing_info": bucketing_info,
                "description": description,
                "parameters": parameters,
                "columns_comments": columns_comments,
                "boto3_session": boto3_session,
                "mode": mode,
                "schema_evolution": schema_evolution,
                "catalog_versioning": catalog_versioning,
                "sep": sep,
                "athena_partition_projection_settings": athena_partition_projection_settings,
                "catalog_table_input": catalog_table_input,
                "catalog_id": catalog_id,
                "compression": pandas_kwargs.get("compression"),
                "skip_header_line_count": True if header else None,
                "serde_library": None,
                "serde_parameters": None,
            }

        paths, partitions_values = _to_dataset(
            func=_to_text,
            concurrent_partitioning=concurrent_partitioning,
            df=df,
            path_root=path,  # type: ignore[arg-type]
            index=index,
            sep=sep,
            compression=compression,
            filename_prefix=filename_prefix,
            use_threads=use_threads,
            partition_cols=partition_cols,
            bucketing_info=bucketing_info,
            mode=mode,
            boto3_session=boto3_session,
            s3_additional_kwargs=s3_additional_kwargs,
            file_format="csv",
            quoting=quoting,
            escapechar=escapechar,
            header=header,
            date_format=date_format,
            **pd_kwargs,
        )
        if database and table:
            try:
                serde_info: dict[str, Any] = {}
                if catalog_table_input:
                    serde_info = catalog_table_input["StorageDescriptor"]["SerdeInfo"]
                create_table_args["serde_library"] = serde_info.get("SerializationLibrary", None)
                create_table_args["serde_parameters"] = serde_info.get("Parameters", None)
                catalog._create_csv_table(**create_table_args)
                if partitions_values and (regular_partitions is True):
                    catalog.add_csv_partitions(
                        database=database,
                        table=table,
                        partitions_values=partitions_values,
                        bucketing_info=bucketing_info,
                        boto3_session=boto3_session,
                        sep=sep,
                        serde_library=create_table_args["serde_library"],
                        serde_parameters=create_table_args["serde_parameters"],
                        catalog_id=catalog_id,
                        columns_types=columns_types,
                        compression=pandas_kwargs.get("compression"),
                    )
            except Exception:
                _logger.debug("Catalog write failed, cleaning up S3 objects (len(paths): %s).", len(paths))
                delete_objects(
                    path=paths,
                    use_threads=use_threads,
                    boto3_session=boto3_session,
                    s3_additional_kwargs=s3_additional_kwargs,
                )
                raise
    return {"paths": paths, "partitions_values": partitions_values}


@apply_configs
@_utils.validate_distributed_kwargs(
    unsupported_kwargs=["boto3_session"],
)
def to_json(  # noqa: PLR0912,PLR0915
    df: pd.DataFrame,
    path: str | None = None,
    index: bool = True,
    columns: list[str] | None = None,
    use_threads: bool | int = True,
    boto3_session: boto3.Session | None = None,
    s3_additional_kwargs: dict[str, Any] | None = None,
    sanitize_columns: bool = False,
    dataset: bool = False,
    filename_prefix: str | None = None,
    partition_cols: list[str] | None = None,
    bucketing_info: BucketingInfoTuple | None = None,
    concurrent_partitioning: bool = False,
    mode: Literal["append", "overwrite", "overwrite_partitions"] | None = None,
    catalog_versioning: bool = False,
    schema_evolution: bool = True,
    dtype: dict[str, str] | None = None,
    database: str | None = None,
    table: str | None = None,
    glue_table_settings: GlueTableSettings | None = None,
    athena_partition_projection_settings: typing.AthenaPartitionProjectionSettings | None = None,
    catalog_id: str | None = None,
    **pandas_kwargs: Any,
) -> _S3WriteDataReturnValue:
    """Write JSON file on Amazon S3.

    Note
    ----
    In case of `use_threads=True` the number of threads
    that will be spawned will be gotten from os.cpu_count().

    Parameters
    ----------
    df: pandas.DataFrame
        Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    path: str
        Amazon S3 path (e.g. s3://bucket/filename.json).
    index: bool
        Write row names (index).
    columns: list[str], optional
        Columns to write.
    use_threads: bool | int
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
        If integer is provided, specified number is used.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.
    s3_additional_kwarg: dict[str, Any], optional
        Forwarded to botocore requests.
        e.g. s3_additional_kwargs={'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'}
    sanitize_columns : bool
        True to sanitize columns names or False to keep it as is.
        True value is forced if `dataset=True`.
    dataset : bool
        If True store as a dataset instead of ordinary file(s)
        If True, enable all follow arguments:
        partition_cols, mode, database, table, description, parameters, columns_comments, concurrent_partitioning,
        catalog_versioning, projection_params, catalog_id, schema_evolution.
    filename_prefix: str, optional
        If dataset=True, add a filename prefix to the output files.
    partition_cols: list[str], optional
        List of column names that will be used to create partitions. Only takes effect if dataset=True.
    bucketing_info: tuple[list[str], int], optional
        Tuple consisting of the column names used for bucketing as the first element and the number of buckets as the
        second element.
        Only `str`, `int` and `bool` are supported as column data types for bucketing.
    concurrent_partitioning: bool
        If True will increase the parallelism level during the partitions writing. It will decrease the
        writing time and increase the memory usage.
        https://aws-sdk-pandas.readthedocs.io/en/3.7.3/tutorials/022%20-%20Writing%20Partitions%20Concurrently.html
    mode : str, optional
        ``append`` (Default), ``overwrite``, ``overwrite_partitions``. Only takes effect if dataset=True.
        For details check the related tutorial:
        https://aws-sdk-pandas.readthedocs.io/en/3.7.3/stubs/awswrangler.s3.to_parquet.html#awswrangler.s3.to_parquet
    catalog_versioning : bool
        If True and `mode="overwrite"`, creates an archived version of the table catalog before updating it.
    schema_evolution : bool
        If True allows schema evolution (new or missing columns), otherwise a exception will be raised.
        (Only considered if dataset=True and mode in ("append", "overwrite_partitions"))
        Related tutorial:
        https://aws-sdk-pandas.readthedocs.io/en/3.7.3/tutorials/014%20-%20Schema%20Evolution.html
    database : str, optional
        Glue/Athena catalog: Database name.
    table : str, optional
        Glue/Athena catalog: Table name.
    glue_table_settings: dict (GlueTableSettings), optional
        Settings for writing to the Glue table.
    dtype : Dict[str, str], optional
        Dictionary of columns names and Athena/Glue types to be casted.
        Useful when you have columns with undetermined or mixed data types.
        (e.g. {'col name': 'bigint', 'col2 name': 'int'})
    athena_partition_projection_settings: typing.AthenaPartitionProjectionSettings, optional
        Parameters of the Athena Partition Projection (https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html).
        AthenaPartitionProjectionSettings is a `TypedDict`, meaning the passed parameter can be instantiated either as an
        instance of AthenaPartitionProjectionSettings or as a regular Python dict.

        Following projection parameters are supported:

        .. list-table:: Projection Parameters
           :header-rows: 1

           * - Name
             - Type
             - Description
           * - projection_types
             - Optional[Dict[str, str]]
             - Dictionary of partitions names and Athena projections types.
               Valid types: "enum", "integer", "date", "injected"
               https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
               (e.g. {'col_name': 'enum', 'col2_name': 'integer'})
           * - projection_ranges
             - Optional[Dict[str, str]]
             - Dictionary of partitions names and Athena projections ranges.
               https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
               (e.g. {'col_name': '0,10', 'col2_name': '-1,8675309'})
           * - projection_values
             - Optional[Dict[str, str]]
             - Dictionary of partitions names and Athena projections values.
               https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
               (e.g. {'col_name': 'A,B,Unknown', 'col2_name': 'foo,boo,bar'})
           * - projection_intervals
             - Optional[Dict[str, str]]
             - Dictionary of partitions names and Athena projections intervals.
               https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
               (e.g. {'col_name': '1', 'col2_name': '5'})
           * - projection_digits
             - Optional[Dict[str, str]]
             - Dictionary of partitions names and Athena projections digits.
               https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
               (e.g. {'col_name': '1', 'col2_name': '2'})
           * - projection_formats
             - Optional[Dict[str, str]]
             - Dictionary of partitions names and Athena projections formats.
               https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
               (e.g. {'col_date': 'yyyy-MM-dd', 'col2_timestamp': 'yyyy-MM-dd HH:mm:ss'})
           * - projection_storage_location_template
             - Optional[str]
             - Value which is allows Athena to properly map partition values if the S3 file locations do not follow
               a typical `.../column=value/...` pattern.
               https://docs.aws.amazon.com/athena/latest/ug/partition-projection-setting-up.html
               (e.g. s3://bucket/table_root/a=${a}/${b}/some_static_subdirectory/${c}/)
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    pandas_kwargs:
        KEYWORD arguments forwarded to pandas.DataFrame.to_json(). You can NOT pass `pandas_kwargs` explicit, just add
        valid Pandas arguments in the function call and awswrangler will accept it.
        e.g. wr.s3.to_json(df, path, lines=True, date_format='iso')
        https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_json.html

    Returns
    -------
    wr.typing._S3WriteDataReturnValue
        Dictionary with:
        'paths': List of all stored files paths on S3.
        'partitions_values': Dictionary of partitions added with keys as S3 path locations
        and values as a list of partitions values as str.

    Examples
    --------
    Writing JSON file

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_json(
    ...     df=pd.DataFrame({'col': [1, 2, 3]}),
    ...     path='s3://bucket/filename.json',
    ... )

    Writing JSON file using pandas_kwargs

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_json(
    ...     df=pd.DataFrame({'col': [1, 2, 3]}),
    ...     path='s3://bucket/filename.json',
    ...     lines=True,
    ...     date_format='iso'
    ... )

    Writing CSV file encrypted with a KMS key

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_json(
    ...     df=pd.DataFrame({'col': [1, 2, 3]}),
    ...     path='s3://bucket/filename.json',
    ...     s3_additional_kwargs={
    ...         'ServerSideEncryption': 'aws:kms',
    ...         'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'
    ...     }
    ... )

    Writing partitioned dataset with partition projection

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> from datetime import datetime
    >>> dt = lambda x: datetime.strptime(x, "%Y-%m-%d").date()
    >>> wr.s3.to_json(
    ...     df=pd.DataFrame({
    ...         "id": [1, 2, 3],
    ...         "value": [1000, 1001, 1002],
    ...         "category": ['A', 'B', 'C'],
    ...     }),
    ...     path='s3://bucket/prefix',
    ...     dataset=True,
    ...     partition_cols=['value', 'category'],
    ...     athena_partition_projection_settings={
    ...        "projection_types": {
    ...             "value": "integer",
    ...             "category": "enum",
    ...         },
    ...         "projection_ranges": {
    ...             "value": "1000,2000",
    ...             "category": "A,B,C",
    ...         },
    ...     },
    ... )
    {
        'paths': [
            's3://.../value=1000/category=A/x.json', ...
        ],
        'partitions_values': {
            's3://.../value=1000/category=A/': [
                '1000',
                'A',
            ], ...
        }
    }

    """
    if "pandas_kwargs" in pandas_kwargs:
        raise exceptions.InvalidArgument(
            "You can NOT pass `pandas_kwargs` explicit, just add valid "
            "Pandas arguments in the function call and awswrangler will accept it."
            "e.g. wr.s3.to_json(df, path, lines=True, date_format='iso')"
        )

    glue_table_settings = cast(
        GlueTableSettings,
        glue_table_settings if glue_table_settings else {},
    )

    table_type = glue_table_settings.get("table_type")
    description = glue_table_settings.get("description")
    parameters = glue_table_settings.get("parameters")
    columns_comments = glue_table_settings.get("columns_comments")
    regular_partitions = glue_table_settings.get("regular_partitions", True)

    _validate_args(
        df=df,
        table=table,
        database=database,
        dataset=dataset,
        path=path,
        partition_cols=partition_cols,
        bucketing_info=bucketing_info,
        mode=mode,
        description=description,
        parameters=parameters,
        columns_comments=columns_comments,
        execution_engine=engine.get(),
    )

    # Initializing defaults
    partition_cols = partition_cols if partition_cols else []
    dtype = dtype if dtype else {}
    partitions_values: dict[str, list[str]] = {}
    mode = "append" if mode is None else mode

    filename_prefix = filename_prefix + uuid.uuid4().hex if filename_prefix else uuid.uuid4().hex
    s3_client = _utils.client(service_name="s3", session=boto3_session)

    # Sanitize table to respect Athena's standards
    if (sanitize_columns is True) or (database is not None and table is not None):
        df, dtype, partition_cols, bucketing_info = _sanitize(
            df=copy_df_shallow(df),
            dtype=dtype,
            partition_cols=partition_cols,
            bucketing_info=bucketing_info,
        )

    # Evaluating dtype
    catalog_table_input: dict[str, Any] | None = None

    if database and table:
        catalog_table_input = catalog._get_table_input(
            database=database,
            table=table,
            boto3_session=boto3_session,
            catalog_id=catalog_id,
        )
        catalog_path: str | None = None
        if catalog_table_input:
            table_type = catalog_table_input["TableType"]
            catalog_path = catalog_table_input.get("StorageDescriptor", {}).get("Location")
        if path is None:
            if catalog_path:
                path = catalog_path
            else:
                raise exceptions.InvalidArgumentValue(
                    "Glue table does not exist in the catalog. Please pass the `path` argument to create it."
                )
        elif path and catalog_path:
            if path.rstrip("/") != catalog_path.rstrip("/"):
                raise exceptions.InvalidArgumentValue(
                    f"The specified path: {path}, does not match the existing Glue catalog table path: {catalog_path}"
                )
        if pandas_kwargs.get("compression") not in ("gzip", "bz2", None):
            raise exceptions.InvalidArgumentCombination(
                "If database and table are given, you must use one of these compressions: gzip, bz2 or None."
            )

    df = _apply_dtype(df=df, dtype=dtype, catalog_table_input=catalog_table_input, mode=mode)

    if dataset is False:
        output_paths = _to_text(
            df,
            file_format="json",
            path=path,
            use_threads=use_threads,
            s3_client=s3_client,
            s3_additional_kwargs=s3_additional_kwargs,
            **pandas_kwargs,
        )
        return {"paths": output_paths, "partitions_values": {}}

    compression: str | None = pandas_kwargs.pop("compression", None)
    df = df[columns] if columns else df

    columns_types: dict[str, str] = {}
    partitions_types: dict[str, str] = {}

    if database and table:
        columns_types, partitions_types = _data_types.athena_types_from_pandas_partitioned(
            df=df, index=index, partition_cols=partition_cols, dtype=dtype
        )
        if schema_evolution is False:
            _utils.check_schema_changes(columns_types=columns_types, table_input=catalog_table_input, mode=mode)

        create_table_args: dict[str, Any] = {
            "database": database,
            "table": table,
            "path": path,
            "columns_types": columns_types,
            "table_type": table_type,
            "partitions_types": partitions_types,
            "bucketing_info": bucketing_info,
            "description": description,
            "parameters": parameters,
            "columns_comments": columns_comments,
            "boto3_session": boto3_session,
            "mode": mode,
            "catalog_versioning": catalog_versioning,
            "schema_evolution": schema_evolution,
            "athena_partition_projection_settings": athena_partition_projection_settings,
            "catalog_table_input": catalog_table_input,
            "catalog_id": catalog_id,
            "compression": compression,
            "serde_library": None,
            "serde_parameters": None,
        }

    paths, partitions_values = _to_dataset(
        func=_to_text,
        concurrent_partitioning=concurrent_partitioning,
        df=df,
        path_root=path,  # type: ignore[arg-type]
        filename_prefix=filename_prefix,
        index=index,
        compression=compression,
        use_threads=use_threads,
        partition_cols=partition_cols,
        bucketing_info=bucketing_info,
        mode=mode,
        boto3_session=boto3_session,
        s3_additional_kwargs=s3_additional_kwargs,
        file_format="json",
        **pandas_kwargs,
    )
    if database and table:
        try:
            serde_info: dict[str, Any] = {}
            if catalog_table_input:
                serde_info = catalog_table_input["StorageDescriptor"]["SerdeInfo"]
            create_table_args["serde_library"] = serde_info.get("SerializationLibrary", None)
            create_table_args["serde_parameters"] = serde_info.get("Parameters", None)
            catalog._create_json_table(**create_table_args)
            if partitions_values and (regular_partitions is True):
                catalog.add_json_partitions(
                    database=database,
                    table=table,
                    partitions_values=partitions_values,
                    bucketing_info=bucketing_info,
                    boto3_session=boto3_session,
                    serde_library=create_table_args["serde_library"],
                    serde_parameters=create_table_args["serde_parameters"],
                    catalog_id=catalog_id,
                    columns_types=columns_types,
                    compression=compression,
                )
        except Exception:
            _logger.debug("Catalog write failed, cleaning up S3 objects (len(paths): %s).", len(paths))
            delete_objects(
                path=paths,
                use_threads=use_threads,
                boto3_session=boto3_session,
                s3_additional_kwargs=s3_additional_kwargs,
            )
            raise
    return {"paths": paths, "partitions_values": partitions_values}
