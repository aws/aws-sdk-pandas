"""Amazon Timestream Module."""

import concurrent.futures
import itertools
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, cast

import boto3
import pandas as pd
from botocore.config import Config

from awswrangler import _data_types, _utils

_logger: logging.Logger = logging.getLogger(__name__)


def _df2list(df: pd.DataFrame) -> List[List[Any]]:
    """Extract Parameters."""
    parameters: List[List[Any]] = df.values.tolist()
    for i, row in enumerate(parameters):
        for j, value in enumerate(row):
            if pd.isna(value):
                parameters[i][j] = None
            elif hasattr(value, "to_pydatetime"):
                parameters[i][j] = value.to_pydatetime()
    return parameters


def _write_batch(
    database: str,
    table: str,
    cols_names: List[str],
    measure_type: str,
    batch: List[Any],
    boto3_primitives: _utils.Boto3PrimitivesType,
) -> List[Dict[str, str]]:
    boto3_session: boto3.Session = _utils.boto3_from_primitives(primitives=boto3_primitives)
    client: boto3.client = _utils.client(
        service_name="timestream-write",
        session=boto3_session,
        botocore_config=Config(read_timeout=20, max_pool_connections=5000, retries={"max_attempts": 10}),
    )
    try:
        _utils.try_it(
            f=client.write_records,
            ex=(client.exceptions.ThrottlingException, client.exceptions.InternalServerException),
            max_num_tries=5,
            DatabaseName=database,
            TableName=table,
            Records=[
                {
                    "Dimensions": [
                        {"Name": name, "DimensionValueType": "VARCHAR", "Value": str(value)}
                        for name, value in zip(cols_names[2:], rec[2:])
                    ],
                    "MeasureName": cols_names[1],
                    "MeasureValueType": measure_type,
                    "MeasureValue": str(rec[1]),
                    "Time": str(round(rec[0].timestamp() * 1_000)),
                    "TimeUnit": "MILLISECONDS",
                }
                for rec in batch
            ],
        )
    except client.exceptions.RejectedRecordsException as ex:
        return cast(List[Dict[str, str]], ex.response["RejectedRecords"])
    return []


def _cast_value(value: str, dtype: str) -> Any:  # pylint: disable=too-many-branches,too-many-return-statements
    if dtype == "VARCHAR":
        return value
    if dtype in ("INTEGER", "BIGINT"):
        return int(value)
    if dtype == "DOUBLE":
        return float(value)
    if dtype == "BOOLEAN":
        return value.lower() == "true"
    if dtype == "TIMESTAMP":
        return datetime.strptime(value[:-3], "%Y-%m-%d %H:%M:%S.%f")
    if dtype == "DATE":
        return datetime.strptime(value, "%Y-%m-%d").date()
    if dtype == "TIME":
        return datetime.strptime(value[:-3], "%H:%M:%S.%f").time()
    raise ValueError(f"Not supported Amazon Timestream type: {dtype}")


def _process_row(schema: List[Dict[str, str]], row: Dict[str, Any]) -> List[Any]:
    row_processed: List[Any] = []
    for col_schema, col in zip(schema, row["Data"]):
        if col.get("NullValue", False):
            row_processed.append(None)
        elif "ScalarValue" in col:
            row_processed.append(_cast_value(value=col["ScalarValue"], dtype=col_schema["type"]))
        else:
            raise ValueError(
                f"Query with non ScalarType/NullValue for column {col_schema['name']}. "
                f"Expected {col_schema['type']} instead of {col}"
            )
    return row_processed


def _process_schema(page: Dict[str, Any]) -> List[Dict[str, str]]:
    schema: List[Dict[str, str]] = []
    for col in page["ColumnInfo"]:
        if "ScalarType" not in col["Type"]:
            raise ValueError(f"Query with non ScalarType for column {col['Name']}: {col['Type']}")
        schema.append({"name": col["Name"], "type": col["Type"]["ScalarType"]})
    return schema


def write(
    df: pd.DataFrame,
    database: str,
    table: str,
    time_col: str,
    measure_col: str,
    dimensions_cols: List[str],
    num_threads: int = 32,
    boto3_session: Optional[boto3.Session] = None,
) -> List[Dict[str, str]]:
    """Store a Pandas DataFrame into a Amazon Timestream table.

    Parameters
    ----------
    df: pandas.DataFrame
        Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    database : str
        Amazon Timestream database name.
    table : str
        Amazon Timestream table name.
    time_col : str
        DataFrame column name to be used as time. MUST be a timestamp column.
    measure_col : str
        DataFrame column name to be used as measure.
    dimensions_cols : List[str]
        List of DataFrame column names to be used as dimensions.
    num_threads : str
        Number of thread to be used for concurrent writing.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.

    Returns
    -------
    List[Dict[str, str]]
        Rejected records.

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

    """
    measure_type: str = _data_types.timestream_type_from_pandas(df[[measure_col]])
    _logger.debug("measure_type: %s", measure_type)
    cols_names: List[str] = [time_col, measure_col] + dimensions_cols
    _logger.debug("cols_names: %s", cols_names)
    batches: List[List[Any]] = _utils.chunkify(lst=_df2list(df=df[cols_names]), max_length=100)
    _logger.debug("len(batches): %s", len(batches))
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        res: List[List[Any]] = list(
            executor.map(
                _write_batch,
                itertools.repeat(database),
                itertools.repeat(table),
                itertools.repeat(cols_names),
                itertools.repeat(measure_type),
                batches,
                itertools.repeat(_utils.boto3_to_primitives(boto3_session=boto3_session)),
            )
        )
        return [item for sublist in res for item in sublist]


def query(sql: str, boto3_session: Optional[boto3.Session] = None) -> pd.DataFrame:
    """Run a query and retrieve the result as a Pandas DataFrame.

    Parameters
    ----------
    sql: str
        SQL query.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.

    Returns
    -------
    pd.DataFrame
        Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html

    Examples
    --------
    Running a query and storing the result as a Pandas DataFrame

    >>> import awswrangler as wr
    >>> df = wr.timestream.query('SELECT * FROM "sampleDB"."sampleTable" ORDER BY time DESC LIMIT 10')

    """
    client: boto3.client = _utils.client(
        service_name="timestream-query",
        session=boto3_session,
        botocore_config=Config(read_timeout=60, retries={"max_attempts": 10}),
    )
    paginator = client.get_paginator("query")
    rows: List[List[Any]] = []
    schema: List[Dict[str, str]] = []
    for page in paginator.paginate(QueryString=sql):
        if not schema:
            schema = _process_schema(page=page)
        for row in page["Rows"]:
            rows.append(_process_row(schema=schema, row=row))
    _logger.debug("schema: %s", schema)
    df = pd.DataFrame(data=rows, columns=[c["name"] for c in schema])
    for col in schema:
        if col["type"] == "VARCHAR":
            df[col["name"]] = df[col["name"]].astype("string")
    return df


def create_database(
    database: str,
    kms_key_id: Optional[str] = None,
    tags: Optional[Dict[str, str]] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> str:
    """Create a new Timestream database.

    Note
    ----
    If the KMS key is not specified, the database will be encrypted with a
    Timestream managed KMS key located in your account.

    Parameters
    ----------
    database: str
        Database name.
    kms_key_id: Optional[str]
        The KMS key for the database. If the KMS key is not specified,
        the database will be encrypted with a Timestream managed KMS key located in your account.
    tags: Optional[Dict[str, str]]
        Key/Value dict to put on the database.
        Tags enable you to categorize databases and/or tables, for example,
        by purpose, owner, or environment.
        e.g. {"foo": "boo", "bar": "xoo"})
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.

    Returns
    -------
    str
        The Amazon Resource Name that uniquely identifies this database. (ARN)

    Examples
    --------
    Creating a database.

    >>> import awswrangler as wr
    >>> arn = wr.timestream.create_database("MyDatabase")

    """
    client: boto3.client = _utils.client(service_name="timestream-write", session=boto3_session)
    args: Dict[str, Any] = {"DatabaseName": database}
    if kms_key_id is not None:
        args["KmsKeyId"] = kms_key_id
    if tags is not None:
        args["Tags"] = [{"Key": k, "Value": v} for k, v in tags.items()]
    response: Dict[str, Dict[str, Any]] = client.create_database(**args)
    return cast(str, response["Database"]["Arn"])


def delete_database(
    database: str,
    boto3_session: Optional[boto3.Session] = None,
) -> None:
    """Delete a given Timestream database. This is an irreversible operation.

    After a database is deleted, the time series data from its tables cannot be recovered.

    All tables in the database must be deleted first, or a ValidationException error will be thrown.

    Due to the nature of distributed retries,
    the operation can return either success or a ResourceNotFoundException.
    Clients should consider them equivalent.

    Parameters
    ----------
    database: str
        Database name.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.

    Returns
    -------
    None
        None.

    Examples
    --------
    Deleting a database

    >>> import awswrangler as wr
    >>> arn = wr.timestream.delete_database("MyDatabase")

    """
    client: boto3.client = _utils.client(service_name="timestream-write", session=boto3_session)
    client.delete_database(DatabaseName=database)


def create_table(
    database: str,
    table: str,
    memory_retention_hours: int,
    magnetic_retention_days: int,
    tags: Optional[Dict[str, str]] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> str:
    """Create a new Timestream database.

    Note
    ----
    If the KMS key is not specified, the database will be encrypted with a
    Timestream managed KMS key located in your account.

    Parameters
    ----------
    database: str
        Database name.
    table: str
        Table name.
    memory_retention_hours: int
        The duration for which data must be stored in the memory store.
    magnetic_retention_days: int
        The duration for which data must be stored in the magnetic store.
    tags: Optional[Dict[str, str]]
        Key/Value dict to put on the table.
        Tags enable you to categorize databases and/or tables, for example,
        by purpose, owner, or environment.
        e.g. {"foo": "boo", "bar": "xoo"})
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.

    Returns
    -------
    str
        The Amazon Resource Name that uniquely identifies this database. (ARN)

    Examples
    --------
    Creating a table.

    >>> import awswrangler as wr
    >>> arn = wr.timestream.create_table(
    ...     database="MyDatabase",
    ...     table="MyTable",
    ...     memory_retention_hours=3,
    ...     magnetic_retention_days=7
    ... )

    """
    client: boto3.client = _utils.client(service_name="timestream-write", session=boto3_session)
    args: Dict[str, Any] = {
        "DatabaseName": database,
        "TableName": table,
        "RetentionProperties": {
            "MemoryStoreRetentionPeriodInHours": memory_retention_hours,
            "MagneticStoreRetentionPeriodInDays": magnetic_retention_days,
        },
    }
    if tags is not None:
        args["Tags"] = [{"Key": k, "Value": v} for k, v in tags.items()]
    response: Dict[str, Dict[str, Any]] = client.create_table(**args)
    return cast(str, response["Table"]["Arn"])


def delete_table(
    database: str,
    table: str,
    boto3_session: Optional[boto3.Session] = None,
) -> None:
    """Delete a given Timestream table.

    This is an irreversible operation.

    After a Timestream database table is deleted, the time series data stored in the table cannot be recovered.

    Due to the nature of distributed retries,
    the operation can return either success or a ResourceNotFoundException.
    Clients should consider them equivalent.

    Parameters
    ----------
    database: str
        Database name.
    table: str
        Table name.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.

    Returns
    -------
    None
        None.

    Examples
    --------
    Deleting a table

    >>> import awswrangler as wr
    >>> arn = wr.timestream.delete_table("MyDatabase", "MyTable")

    """
    client: boto3.client = _utils.client(service_name="timestream-write", session=boto3_session)
    client.delete_table(DatabaseName=database, TableName=table)
