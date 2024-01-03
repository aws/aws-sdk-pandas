"""Amazon Timestream Module."""

from __future__ import annotations

import itertools
import logging
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Literal, cast

import boto3
from botocore.config import Config

import awswrangler.pandas as pd
from awswrangler import _data_types, _utils, exceptions, s3
from awswrangler._config import apply_configs
from awswrangler._distributed import engine
from awswrangler._executor import _BaseExecutor, _get_executor
from awswrangler.distributed.ray import ray_get
from awswrangler.typing import TimestreamBatchLoadReportS3Configuration

if TYPE_CHECKING:
    from mypy_boto3_timestream_write.client import TimestreamWriteClient

_BATCH_LOAD_FINAL_STATES: list[str] = ["SUCCEEDED", "FAILED", "PROGRESS_STOPPED", "PENDING_RESUME"]
_BATCH_LOAD_WAIT_POLLING_DELAY: float = 2  # SECONDS
_TIME_UNITS_MAPPING = {
    "SECONDS": (9, 0),
    "MILLISECONDS": (6, 3),
    "MICROSECONDS": (3, 6),
    "NANOSECONDS": (0, 9),
}

_logger: logging.Logger = logging.getLogger(__name__)

_TimeUnitLiteral = Literal["MILLISECONDS", "SECONDS", "MICROSECONDS", "NANOSECONDS"]


def _df2list(df: pd.DataFrame) -> list[list[Any]]:
    """Extract Parameters."""
    parameters: list[list[Any]] = df.values.tolist()
    for i, row in enumerate(parameters):
        for j, value in enumerate(row):
            if pd.isna(value):
                parameters[i][j] = None
            elif hasattr(value, "to_pydatetime"):
                parameters[i][j] = value.to_pydatetime()
    return parameters


def _check_time_unit(time_unit: _TimeUnitLiteral) -> str:
    time_unit = time_unit if time_unit else "MILLISECONDS"
    if time_unit not in _TIME_UNITS_MAPPING.keys():
        raise exceptions.InvalidArgumentValue(
            f"Invalid time unit: {time_unit}. Must be one of {_TIME_UNITS_MAPPING.keys()}."
        )
    return time_unit


def _format_timestamp(timestamp: int | datetime, time_unit: _TimeUnitLiteral) -> str:
    if isinstance(timestamp, int):
        return str(round(timestamp / pow(10, _TIME_UNITS_MAPPING[time_unit][0])))
    if isinstance(timestamp, datetime):
        return str(round(timestamp.timestamp() * pow(10, _TIME_UNITS_MAPPING[time_unit][1])))
    raise exceptions.InvalidArgumentType("`time_col` must be of type timestamp.")


def _format_measure(
    measure_name: str, measure_value: Any, measure_type: str, time_unit: _TimeUnitLiteral
) -> dict[str, str]:
    return {
        "Name": measure_name,
        "Value": _format_timestamp(measure_value, time_unit) if measure_type == "TIMESTAMP" else str(measure_value),
        "Type": measure_type,
    }


def _sanitize_common_attributes(
    common_attributes: dict[str, Any] | None,
    version: int,
    time_unit: _TimeUnitLiteral,
    measure_name: str | None,
) -> dict[str, Any]:
    common_attributes = {} if not common_attributes else common_attributes
    # Values in common_attributes take precedence
    common_attributes.setdefault("Version", version)
    common_attributes.setdefault("TimeUnit", _check_time_unit(common_attributes.get("TimeUnit", time_unit)))

    if "Time" not in common_attributes and common_attributes["TimeUnit"] == "NANOSECONDS":
        raise exceptions.InvalidArgumentValue("Python datetime objects do not support nanoseconds precision.")

    if "MeasureValue" in common_attributes and "MeasureValueType" not in common_attributes:
        raise exceptions.InvalidArgumentCombination(
            "MeasureValueType must be supplied alongside MeasureValue in common_attributes."
        )

    if measure_name:
        common_attributes.setdefault("MeasureName", measure_name)
    elif "MeasureName" not in common_attributes:
        raise exceptions.InvalidArgumentCombination(
            "MeasureName must be supplied with the `measure_name` argument or in common_attributes."
        )
    return common_attributes


@engine.dispatch_on_engine
def _write_batch(
    timestream_client: "TimestreamWriteClient" | None,
    database: str,
    table: str,
    common_attributes: dict[str, Any],
    cols_names: list[str | None],
    measure_cols: list[str | None],
    measure_types: list[str],
    dimensions_cols: list[str | None],
    batch: list[Any],
) -> list[dict[str, str]]:
    client_timestream = timestream_client if timestream_client else _utils.client(service_name="timestream-write")
    records: list[dict[str, Any]] = []
    scalar = bool(len(measure_cols) == 1 and "MeasureValues" not in common_attributes)
    time_loc = 0
    measure_cols_loc = 1 if cols_names[0] else 0
    dimensions_cols_loc = 1 if len(measure_cols) == 1 else 1 + len(measure_cols)
    if all(cols_names):
        # Time and Measures are supplied in the data frame
        dimensions_cols_loc = 1 + len(measure_cols)
    elif all(v is None for v in cols_names[:2]):
        # Time and Measures are supplied in common_attributes
        dimensions_cols_loc = 0
    time_unit = common_attributes["TimeUnit"]

    for row in batch:
        record: dict[str, Any] = {}
        if "Time" not in common_attributes:
            record["Time"] = _format_timestamp(row[time_loc], time_unit)
        if scalar and "MeasureValue" not in common_attributes:
            measure_value = row[measure_cols_loc]
            if pd.isnull(measure_value):
                continue
            record["MeasureValue"] = str(measure_value)
        elif not scalar and "MeasureValues" not in common_attributes:
            record["MeasureValues"] = [
                _format_measure(measure_name, measure_value, measure_value_type, time_unit)  # type: ignore[arg-type]
                for measure_name, measure_value, measure_value_type in zip(
                    measure_cols, row[measure_cols_loc:dimensions_cols_loc], measure_types
                )
                if not pd.isnull(measure_value)
            ]
            if len(record["MeasureValues"]) == 0:
                continue
        if "MeasureValueType" not in common_attributes:
            record["MeasureValueType"] = measure_types[0] if scalar else "MULTI"
        # Dimensions can be specified in both common_attributes and the data frame
        dimensions = (
            [
                {"Name": name, "DimensionValueType": "VARCHAR", "Value": str(value)}
                for name, value in zip(dimensions_cols, row[dimensions_cols_loc:])
            ]
            if all(dimensions_cols)
            else []
        )
        if dimensions:
            record["Dimensions"] = dimensions
        if record:
            records.append(record)

    try:
        if records:
            _utils.try_it(
                f=client_timestream.write_records,
                ex=(
                    client_timestream.exceptions.ThrottlingException,
                    client_timestream.exceptions.InternalServerException,
                ),
                max_num_tries=5,
                DatabaseName=database,
                TableName=table,
                CommonAttributes=common_attributes,
                Records=records,
            )
    except client_timestream.exceptions.RejectedRecordsException as ex:
        return cast(List[Dict[str, str]], ex.response["RejectedRecords"])
    return []


@engine.dispatch_on_engine
def _write_df(
    df: pd.DataFrame,
    executor: _BaseExecutor,
    database: str,
    table: str,
    common_attributes: dict[str, Any],
    cols_names: list[str | None],
    measure_cols: list[str | None],
    measure_types: list[str],
    dimensions_cols: list[str | None],
    boto3_session: boto3.Session | None,
) -> list[dict[str, str]]:
    timestream_client = _utils.client(
        service_name="timestream-write",
        session=boto3_session,
        botocore_config=Config(read_timeout=20, max_pool_connections=5000, retries={"max_attempts": 10}),
    )
    batches: list[list[Any]] = _utils.chunkify(lst=_df2list(df=df), max_length=100)
    _logger.debug("Writing %d batches of data", len(batches))
    return executor.map(
        _write_batch,  # type: ignore[arg-type]
        timestream_client,
        itertools.repeat(database),
        itertools.repeat(table),
        itertools.repeat(common_attributes),
        itertools.repeat(cols_names),
        itertools.repeat(measure_cols),
        itertools.repeat(measure_types),
        itertools.repeat(dimensions_cols),
        batches,
    )


@_utils.validate_distributed_kwargs(
    unsupported_kwargs=["boto3_session"],
)
def write(
    df: pd.DataFrame,
    database: str,
    table: str,
    time_col: str | None = None,
    measure_col: str | list[str | None] | None = None,
    dimensions_cols: list[str | None] | None = None,
    version: int = 1,
    time_unit: _TimeUnitLiteral = "MILLISECONDS",
    use_threads: bool | int = True,
    measure_name: str | None = None,
    common_attributes: dict[str, Any] | None = None,
    boto3_session: boto3.Session | None = None,
) -> list[dict[str, str]]:
    """Store a Pandas DataFrame into an Amazon Timestream table.

    Note
    ----
    In case `use_threads=True`, the number of threads from os.cpu_count() is used.

    If the Timestream service rejects a record(s),
    this function will not throw a Python exception.
    Instead it will return the rejection information.

    Note
    ----
    If ``time_col`` column is supplied, it must be of type timestamp. ``time_unit`` is set to MILLISECONDS by default.
    NANOSECONDS is not supported as python datetime objects are limited to microseconds precision.

    Parameters
    ----------
    df : pandas.DataFrame
        Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    database : str
        Amazon Timestream database name.
    table : str
        Amazon Timestream table name.
    time_col: str, optional
        DataFrame column name to be used as time. MUST be a timestamp column.
    measure_col: str | List[str] | None
        DataFrame column name(s) to be used as measure.
    dimensions_cols: list[str]
        List of DataFrame column names to be used as dimensions.
    version : int
        Version number used for upserts.
        Documentation https://docs.aws.amazon.com/timestream/latest/developerguide/API_WriteRecords.html.
    time_unit: str, optional
        Time unit for the time column. MILLISECONDS by default.
    use_threads: bool | int
        True to enable concurrent writing, False to disable multiple threads.
        If enabled, os.cpu_count() is used as the number of threads.
        If integer is provided, specified number is used.
    measure_name: str, optional
        Name that represents the data attribute of the time series.
        Overrides ``measure_col`` if specified.
    common_attributes: dict[str, Any], optional
        Dictionary of attributes shared across all records in the request.
        Using common attributes can optimize the cost of writes by reducing the size of request payloads.
        Values in ``common_attributes`` take precedence over all other arguments and data frame values.
        Dimension attributes are merged with attributes in record objects.
        Example: ``{"Dimensions": [{"Name": "device_id", "Value": "12345"}], "MeasureValueType": "DOUBLE"}``.
    boto3_session: boto3.Session(), optional
        Boto3 Session. If None, the default boto3 Session is used.

    Returns
    -------
    List[Dict[str, str]]
        Rejected records.
        Possible reasons for rejection are described here:
        https://docs.aws.amazon.com/timestream/latest/developerguide/API_RejectedRecord.html

    Examples
    --------
    Store a Pandas DataFrame into a Amazon Timestream table.

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> df = pd.DataFrame(
    >>>     {
    >>>         "time": [datetime.now(), datetime.now(), datetime.now()],
    >>>         "dim0": ["foo", "boo", "bar"],
    >>>         "dim1": [1, 2, 3],
    >>>         "measure": [1.0, 1.1, 1.2],
    >>>     }
    >>> )
    >>> rejected_records = wr.timestream.write(
    >>>     df=df,
    >>>     database="sampleDB",
    >>>     table="sampleTable",
    >>>     time_col="time",
    >>>     measure_col="measure",
    >>>     dimensions_cols=["dim0", "dim1"],
    >>> )
    >>> assert len(rejected_records) == 0

    Return value if some records are rejected.

    >>> [
    >>>     {
    >>>         'ExistingVersion': 2,
    >>>         'Reason': 'The record version 1 is lower than the existing version 2. A '
    >>>                   'higher version is required to update the measure value.',
    >>>         'RecordIndex': 0
    >>>     }
    >>> ]

    """
    measure_cols = measure_col if isinstance(measure_col, list) else [measure_col]
    measure_types: list[str] = (
        _data_types.timestream_type_from_pandas(df.loc[:, measure_cols]) if all(measure_cols) else []
    )
    dimensions_cols = dimensions_cols if dimensions_cols else [dimensions_cols]  # type: ignore[list-item]
    cols_names: list[str | None] = [time_col] + measure_cols + dimensions_cols
    measure_name = measure_name if measure_name else measure_cols[0]
    common_attributes = _sanitize_common_attributes(common_attributes, version, time_unit, measure_name)

    _logger.debug(
        "Writing to Timestream table %s in database %s\ncommon_attributes: %s\n, cols_names: %s\n, measure_types: %s",
        table,
        database,
        common_attributes,
        cols_names,
        measure_types,
    )

    # User can supply arguments in one of two ways:
    # 1. With the `common_attributes` dictionary which takes precedence
    # 2. With data frame columns
    # However, the data frame cannot be completely empty.
    # So if all values in `cols_names` are None, an exception is raised.
    if any(cols_names):
        dfs = _utils.split_pandas_frame(
            df.loc[:, [c for c in cols_names if c]], _utils.ensure_cpu_count(use_threads=use_threads)
        )
    else:
        raise exceptions.InvalidArgumentCombination(
            "At least one of `time_col`, `measure_col` or `dimensions_cols` must be specified."
        )
    _logger.debug("Writing %d dataframes to Timestream table", len(dfs))

    executor: _BaseExecutor = _get_executor(use_threads=use_threads)
    errors = list(
        itertools.chain(
            *ray_get(
                [
                    _write_df(
                        df=df,
                        executor=executor,
                        database=database,
                        table=table,
                        common_attributes=common_attributes,
                        cols_names=cols_names,
                        measure_cols=measure_cols,
                        measure_types=measure_types,
                        dimensions_cols=dimensions_cols,
                        boto3_session=boto3_session,
                    )
                    for df in dfs
                ]
            )
        )
    )
    return list(itertools.chain(*ray_get(errors)))


@apply_configs
def wait_batch_load_task(
    task_id: str,
    timestream_batch_load_wait_polling_delay: float = _BATCH_LOAD_WAIT_POLLING_DELAY,
    boto3_session: boto3.Session | None = None,
) -> dict[str, Any]:
    """
    Wait for the Timestream batch load task to complete.

    Parameters
    ----------
    task_id : str
        The ID of the batch load task.
    timestream_batch_load_wait_polling_delay : float, optional
        Time to wait between two polling attempts.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session is used if None.

    Returns
    -------
    Dict[str, Any]
        Dictionary with the describe_batch_load_task response.

    Examples
    --------
    >>> import awswrangler as wr
    >>> res = wr.timestream.wait_batch_load_task(task_id='task-id')

    Raises
    ------
    exceptions.TimestreamLoadError
        Error message raised by failed task.
    """
    timestream_client = _utils.client(service_name="timestream-write", session=boto3_session)

    response = timestream_client.describe_batch_load_task(TaskId=task_id)
    status = response["BatchLoadTaskDescription"]["TaskStatus"]
    while status not in _BATCH_LOAD_FINAL_STATES:
        time.sleep(timestream_batch_load_wait_polling_delay)
        response = timestream_client.describe_batch_load_task(TaskId=task_id)
        status = response["BatchLoadTaskDescription"]["TaskStatus"]
    _logger.debug("Task status: %s", status)
    if status != "SUCCEEDED":
        _logger.debug("Task response: %s", response)
        raise exceptions.TimestreamLoadError(response.get("ErrorMessage"))
    return response  # type: ignore[return-value]


@apply_configs
@_utils.validate_distributed_kwargs(
    unsupported_kwargs=["boto3_session", "s3_additional_kwargs"],
)
def batch_load(
    df: pd.DataFrame,
    path: str,
    database: str,
    table: str,
    time_col: str,
    dimensions_cols: list[str],
    measure_cols: list[str],
    measure_name_col: str,
    report_s3_configuration: TimestreamBatchLoadReportS3Configuration,
    time_unit: _TimeUnitLiteral = "MILLISECONDS",
    record_version: int = 1,
    timestream_batch_load_wait_polling_delay: float = _BATCH_LOAD_WAIT_POLLING_DELAY,
    keep_files: bool = False,
    use_threads: bool | int = True,
    boto3_session: boto3.Session | None = None,
    s3_additional_kwargs: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Batch load a Pandas DataFrame into a Amazon Timestream table.

    Note
    ----
    The supplied column names (time, dimension, measure) MUST match those in the Timestream table.

    Note
    ----
    Only ``MultiMeasureMappings`` is supported.
    See https://docs.aws.amazon.com/timestream/latest/developerguide/batch-load-data-model-mappings.html

    Parameters
    ----------
    df : pandas.DataFrame
        Pandas DataFrame.
    path : str
        S3 prefix to write the data.
    database : str
        Amazon Timestream database name.
    table : str
        Amazon Timestream table name.
    time_col : str
        Column name with the time data. It must be a long data type that represents the time since the Unix epoch.
    dimensions_cols : List[str]
        List of column names with the dimensions data.
    measure_cols : List[str]
        List of column names with the measure data.
    measure_name_col : str
        Column name with the measure name.
    report_s3_configuration : TimestreamBatchLoadReportS3Configuration
        Dictionary of the configuration for the S3 bucket where the error report is stored.
        https://docs.aws.amazon.com/timestream/latest/developerguide/API_ReportS3Configuration.html
        Example: {"BucketName": 'error-report-bucket-name'}
    time_unit : str, optional
        Time unit for the time column. MILLISECONDS by default.
    record_version : int, optional
        Record version.
    timestream_batch_load_wait_polling_delay : float, optional
        Time to wait between two polling attempts.
    keep_files : bool, optional
        Whether to keep the files after the operation.
    use_threads : Union[bool, int], optional
        True to enable concurrent requests, False to disable multiple threads.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session is used if None.
    s3_additional_kwargs: dict[str, str], optional
        Forwarded to S3 botocore requests.

    Returns
    -------
    Dict[str, Any]
        A dictionary of the batch load task response.

    Examples
    --------
    >>> import awswrangler as wr

    >>> response = wr.timestream.batch_load(
    >>>     df=df,
    >>>     path='s3://bucket/path/',
    >>>     database='sample_db',
    >>>     table='sample_table',
    >>>     time_col='time',
    >>>     dimensions_cols=['region', 'location'],
    >>>     measure_cols=['memory_utilization', 'cpu_utilization'],
    >>>     report_s3_configuration={'BucketName': 'error-report-bucket-name'},
    >>> )
    """
    path = path if path.endswith("/") else f"{path}/"
    if s3.list_objects(path=path, boto3_session=boto3_session, s3_additional_kwargs=s3_additional_kwargs):
        raise exceptions.InvalidArgument(
            f"The received S3 path ({path}) is not empty. "
            "Please, provide a different path or use wr.s3.delete_objects() to clean up the current one."
        )
    columns = [time_col, *dimensions_cols, *measure_cols, measure_name_col]

    try:
        s3.to_csv(
            df=df.loc[:, columns],
            path=path,
            index=False,
            dataset=True,
            mode="append",
            use_threads=use_threads,
            boto3_session=boto3_session,
            s3_additional_kwargs=s3_additional_kwargs,
        )
        measure_types: list[str] = _data_types.timestream_type_from_pandas(df.loc[:, measure_cols])
        return batch_load_from_files(
            path=path,
            database=database,
            table=table,
            time_col=time_col,
            dimensions_cols=dimensions_cols,
            measure_cols=measure_cols,
            measure_types=measure_types,
            report_s3_configuration=report_s3_configuration,
            time_unit=time_unit,
            measure_name_col=measure_name_col,
            record_version=record_version,
            timestream_batch_load_wait_polling_delay=timestream_batch_load_wait_polling_delay,
            boto3_session=boto3_session,
        )
    finally:
        if not keep_files:
            _logger.debug("Deleting objects in S3 path: %s", path)
            s3.delete_objects(
                path=path,
                use_threads=use_threads,
                boto3_session=boto3_session,
                s3_additional_kwargs=s3_additional_kwargs,
            )


@apply_configs
def batch_load_from_files(
    path: str,
    database: str,
    table: str,
    time_col: str,
    dimensions_cols: list[str],
    measure_cols: list[str],
    measure_types: list[str],
    measure_name_col: str,
    report_s3_configuration: TimestreamBatchLoadReportS3Configuration,
    time_unit: _TimeUnitLiteral = "MILLISECONDS",
    record_version: int = 1,
    data_source_csv_configuration: dict[str, str | bool] | None = None,
    timestream_batch_load_wait_polling_delay: float = _BATCH_LOAD_WAIT_POLLING_DELAY,
    boto3_session: boto3.Session | None = None,
) -> dict[str, Any]:
    """Batch load files from S3 into a Amazon Timestream table.

    Note
    ----
    The supplied column names (time, dimension, measure) MUST match those in the Timestream table.

    Note
    ----
    Only ``MultiMeasureMappings`` is supported.
    See https://docs.aws.amazon.com/timestream/latest/developerguide/batch-load-data-model-mappings.html

    Parameters
    ----------
    path : str
        S3 prefix to write the data.
    database : str
        Amazon Timestream database name.
    table : str
        Amazon Timestream table name.
    time_col : str
        Column name with the time data. It must be a long data type that represents the time since the Unix epoch.
    dimensions_cols : List[str]
        List of column names with the dimensions data.
    measure_cols : List[str]
        List of column names with the measure data.
    measure_name_col : str
        Column name with the measure name.
    report_s3_configuration : TimestreamBatchLoadReportS3Configuration
        Dictionary of the configuration for the S3 bucket where the error report is stored.
        https://docs.aws.amazon.com/timestream/latest/developerguide/API_ReportS3Configuration.html
        Example: {"BucketName": 'error-report-bucket-name'}
    time_unit : str, optional
        Time unit for the time column. MILLISECONDS by default.
    record_version : int, optional
        Record version.
    data_source_csv_configuration : Dict[str, Union[str, bool]], optional
        Dictionary of the data source CSV configuration.
        https://docs.aws.amazon.com/timestream/latest/developerguide/API_CsvConfiguration.html
    timestream_batch_load_wait_polling_delay : float, optional
        Time to wait between two polling attempts.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session is used if None.

    Returns
    -------
    Dict[str, Any]
        A dictionary of the batch load task response.

    Examples
    --------
    >>> import awswrangler as wr

    >>> response = wr.timestream.batch_load_from_files(
    >>>     path='s3://bucket/path/',
    >>>     database='sample_db',
    >>>     table='sample_table',
    >>>     time_col='time',
    >>>     dimensions_cols=['region', 'location'],
    >>>     measure_cols=['memory_utilization', 'cpu_utilization'],
    >>>     report_s3_configuration={'BucketName': 'error-report-bucket-name'},
    >>> )
    """
    timestream_client = _utils.client(service_name="timestream-write", session=boto3_session)
    bucket, prefix = _utils.parse_path(path=path)

    kwargs: dict[str, Any] = {
        "TargetDatabaseName": database,
        "TargetTableName": table,
        "DataModelConfiguration": {
            "DataModel": {
                "TimeColumn": time_col,
                "TimeUnit": _check_time_unit(time_unit),
                "DimensionMappings": [{"SourceColumn": c} for c in dimensions_cols],
                "MeasureNameColumn": measure_name_col,
                "MultiMeasureMappings": {
                    "MultiMeasureAttributeMappings": [
                        {"SourceColumn": c, "MeasureValueType": t} for c, t in zip(measure_cols, measure_types)
                    ],
                },
            }
        },
        "DataSourceConfiguration": {
            "DataSourceS3Configuration": {"BucketName": bucket, "ObjectKeyPrefix": prefix},
            "DataFormat": "CSV",
            "CsvConfiguration": data_source_csv_configuration if data_source_csv_configuration else {},
        },
        "ReportConfiguration": {"ReportS3Configuration": report_s3_configuration},
        "RecordVersion": record_version,
    }

    task_id = timestream_client.create_batch_load_task(**kwargs)["TaskId"]
    return wait_batch_load_task(
        task_id=task_id,
        timestream_batch_load_wait_polling_delay=timestream_batch_load_wait_polling_delay,
        boto3_session=boto3_session,
    )
