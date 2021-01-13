"""AWS Glue Catalog Module."""

import logging
from typing import Any, Dict, List, Optional, Tuple

import boto3

from awswrangler import _utils, exceptions
from awswrangler._config import apply_configs
from awswrangler.catalog._definitions import _csv_table_definition, _parquet_table_definition
from awswrangler.catalog._delete import delete_all_partitions, delete_table_if_exists
from awswrangler.catalog._get import _get_table_input
from awswrangler.catalog._utils import _catalog_id, sanitize_column_name, sanitize_table_name

_logger: logging.Logger = logging.getLogger(__name__)


def _update_if_necessary(dic: Dict[str, str], key: str, value: Optional[str], mode: str) -> str:
    if value is not None:
        if key not in dic or dic[key] != value:
            dic[key] = value
            if mode in ("append", "overwrite_partitions"):
                return "update"
    return mode


def _create_table(  # pylint: disable=too-many-branches,too-many-statements
    database: str,
    table: str,
    description: Optional[str],
    parameters: Optional[Dict[str, str]],
    mode: str,
    catalog_versioning: bool,
    boto3_session: Optional[boto3.Session],
    table_input: Dict[str, Any],
    table_exist: bool,
    projection_enabled: bool,
    partitions_types: Optional[Dict[str, str]],
    columns_comments: Optional[Dict[str, str]],
    projection_types: Optional[Dict[str, str]],
    projection_ranges: Optional[Dict[str, str]],
    projection_values: Optional[Dict[str, str]],
    projection_intervals: Optional[Dict[str, str]],
    projection_digits: Optional[Dict[str, str]],
    catalog_id: Optional[str],
) -> None:
    # Description
    mode = _update_if_necessary(dic=table_input, key="Description", value=description, mode=mode)

    # Parameters
    parameters = parameters if parameters else {}
    for k, v in parameters.items():
        mode = _update_if_necessary(dic=table_input["Parameters"], key=k, value=v, mode=mode)

    # Projection
    if projection_enabled is True:
        table_input["Parameters"]["projection.enabled"] = "true"
        partitions_types = partitions_types if partitions_types else {}
        projection_types = projection_types if projection_types else {}
        projection_ranges = projection_ranges if projection_ranges else {}
        projection_values = projection_values if projection_values else {}
        projection_intervals = projection_intervals if projection_intervals else {}
        projection_digits = projection_digits if projection_digits else {}
        projection_types = {sanitize_column_name(k): v for k, v in projection_types.items()}
        projection_ranges = {sanitize_column_name(k): v for k, v in projection_ranges.items()}
        projection_values = {sanitize_column_name(k): v for k, v in projection_values.items()}
        projection_intervals = {sanitize_column_name(k): v for k, v in projection_intervals.items()}
        projection_digits = {sanitize_column_name(k): v for k, v in projection_digits.items()}
        for k, v in projection_types.items():
            dtype: Optional[str] = partitions_types.get(k)
            if dtype is None:
                raise exceptions.InvalidArgumentCombination(
                    f"Column {k} appears as projected column but not as partitioned column."
                )
            if dtype == "date":
                table_input["Parameters"][f"projection.{k}.format"] = "yyyy-MM-dd"
            elif dtype == "timestamp":
                table_input["Parameters"][f"projection.{k}.format"] = "yyyy-MM-dd HH:mm:ss"
                table_input["Parameters"][f"projection.{k}.interval.unit"] = "SECONDS"
                table_input["Parameters"][f"projection.{k}.interval"] = "1"
        for k, v in projection_types.items():
            mode = _update_if_necessary(dic=table_input["Parameters"], key=f"projection.{k}.type", value=v, mode=mode)
        for k, v in projection_ranges.items():
            mode = _update_if_necessary(dic=table_input["Parameters"], key=f"projection.{k}.range", value=v, mode=mode)
        for k, v in projection_values.items():
            mode = _update_if_necessary(dic=table_input["Parameters"], key=f"projection.{k}.values", value=v, mode=mode)
        for k, v in projection_intervals.items():
            mode = _update_if_necessary(
                dic=table_input["Parameters"], key=f"projection.{k}.interval", value=str(v), mode=mode
            )
        for k, v in projection_digits.items():
            mode = _update_if_necessary(
                dic=table_input["Parameters"], key=f"projection.{k}.digits", value=str(v), mode=mode
            )
    else:
        table_input["Parameters"]["projection.enabled"] = "false"

    # Column comments
    columns_comments = columns_comments if columns_comments else {}
    columns_comments = {sanitize_column_name(k): v for k, v in columns_comments.items()}
    if columns_comments:
        for col in table_input["StorageDescriptor"]["Columns"]:
            name: str = col["Name"]
            if name in columns_comments:
                mode = _update_if_necessary(dic=col, key="Comment", value=columns_comments[name], mode=mode)
        for par in table_input["PartitionKeys"]:
            name = par["Name"]
            if name in columns_comments:
                mode = _update_if_necessary(dic=par, key="Comment", value=columns_comments[name], mode=mode)

    _logger.debug("table_input: %s", table_input)

    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    client_glue: boto3.client = _utils.client(service_name="glue", session=session)
    skip_archive: bool = not catalog_versioning
    if mode not in ("overwrite", "append", "overwrite_partitions", "update"):
        raise exceptions.InvalidArgument(
            f"{mode} is not a valid mode. It must be 'overwrite', 'append' or 'overwrite_partitions'."
        )
    if table_exist is True and mode == "overwrite":
        delete_all_partitions(table=table, database=database, catalog_id=catalog_id, boto3_session=session)
        _logger.debug("Updating table (%s)...", mode)
        client_glue.update_table(
            **_catalog_id(
                catalog_id=catalog_id, DatabaseName=database, TableInput=table_input, SkipArchive=skip_archive
            )
        )
    elif (table_exist is True) and (mode in ("append", "overwrite_partitions", "update")):
        if mode == "update":
            _logger.debug("Updating table (%s)...", mode)
            client_glue.update_table(
                **_catalog_id(
                    catalog_id=catalog_id, DatabaseName=database, TableInput=table_input, SkipArchive=skip_archive
                )
            )
    elif table_exist is False:
        try:
            _logger.debug("Creating table (%s)...", mode)
            client_glue.create_table(
                **_catalog_id(catalog_id=catalog_id, DatabaseName=database, TableInput=table_input)
            )
        except client_glue.exceptions.AlreadyExistsException:
            if mode == "overwrite":
                _utils.try_it(
                    f=_overwrite_table,
                    ex=client_glue.exceptions.AlreadyExistsException,
                    client_glue=client_glue,
                    catalog_id=catalog_id,
                    database=database,
                    table=table,
                    table_input=table_input,
                    boto3_session=boto3_session,
                )
    _logger.debug("Leaving table as is (%s)...", mode)


def _overwrite_table(
    client_glue: boto3.client,
    catalog_id: Optional[str],
    database: str,
    table: str,
    table_input: Dict[str, Any],
    boto3_session: boto3.Session,
) -> None:
    delete_table_if_exists(database=database, table=table, boto3_session=boto3_session, catalog_id=catalog_id)
    client_glue.create_table(**_catalog_id(catalog_id=catalog_id, DatabaseName=database, TableInput=table_input))


def _upsert_table_parameters(
    parameters: Dict[str, str],
    database: str,
    catalog_versioning: bool,
    catalog_id: Optional[str],
    table_input: Dict[str, Any],
    boto3_session: Optional[boto3.Session],
) -> Dict[str, str]:
    pars: Dict[str, str] = table_input["Parameters"]
    update: bool = False
    for k, v in parameters.items():
        if k not in pars or v != pars[k]:
            pars[k] = v
            update = True
    if update is True:
        _overwrite_table_parameters(
            parameters=pars,
            database=database,
            catalog_id=catalog_id,
            boto3_session=boto3_session,
            table_input=table_input,
            catalog_versioning=catalog_versioning,
        )
    return pars


def _overwrite_table_parameters(
    parameters: Dict[str, str],
    database: str,
    catalog_versioning: bool,
    catalog_id: Optional[str],
    table_input: Dict[str, Any],
    boto3_session: Optional[boto3.Session],
) -> Dict[str, str]:
    table_input["Parameters"] = parameters
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
    skip_archive: bool = not catalog_versioning
    client_glue.update_table(
        **_catalog_id(catalog_id=catalog_id, DatabaseName=database, TableInput=table_input, SkipArchive=skip_archive)
    )
    return parameters


def _create_parquet_table(
    database: str,
    table: str,
    path: str,
    columns_types: Dict[str, str],
    partitions_types: Optional[Dict[str, str]],
    bucketing_info: Optional[Tuple[List[str], int]],
    catalog_id: Optional[str],
    compression: Optional[str],
    description: Optional[str],
    parameters: Optional[Dict[str, str]],
    columns_comments: Optional[Dict[str, str]],
    mode: str,
    catalog_versioning: bool,
    projection_enabled: bool,
    projection_types: Optional[Dict[str, str]],
    projection_ranges: Optional[Dict[str, str]],
    projection_values: Optional[Dict[str, str]],
    projection_intervals: Optional[Dict[str, str]],
    projection_digits: Optional[Dict[str, str]],
    boto3_session: Optional[boto3.Session],
    catalog_table_input: Optional[Dict[str, Any]],
) -> None:
    table = sanitize_table_name(table=table)
    partitions_types = {} if partitions_types is None else partitions_types
    _logger.debug("catalog_table_input: %s", catalog_table_input)
    table_input: Dict[str, Any]
    if (catalog_table_input is not None) and (mode in ("append", "overwrite_partitions")):
        table_input = catalog_table_input
        catalog_cols: Dict[str, str] = {x["Name"]: x["Type"] for x in table_input["StorageDescriptor"]["Columns"]}
        for c, t in columns_types.items():
            if c not in catalog_cols:
                _logger.debug("New column %s with type %s.", c, t)
                table_input["StorageDescriptor"]["Columns"].append({"Name": c, "Type": t})
                mode = "update"
            elif t != catalog_cols[c]:  # Data type change detected!
                raise exceptions.InvalidArgumentValue(
                    f"Data type change detected on column {c} (Old type: {catalog_cols[c]} / New type {t})."
                )
    else:
        table_input = _parquet_table_definition(
            table=table,
            path=path,
            columns_types=columns_types,
            partitions_types=partitions_types,
            bucketing_info=bucketing_info,
            compression=compression,
        )
    table_exist: bool = catalog_table_input is not None
    _logger.debug("table_exist: %s", table_exist)
    _create_table(
        database=database,
        table=table,
        description=description,
        parameters=parameters,
        columns_comments=columns_comments,
        mode=mode,
        catalog_versioning=catalog_versioning,
        boto3_session=boto3_session,
        table_input=table_input,
        table_exist=table_exist,
        partitions_types=partitions_types,
        projection_enabled=projection_enabled,
        projection_types=projection_types,
        projection_ranges=projection_ranges,
        projection_values=projection_values,
        projection_intervals=projection_intervals,
        projection_digits=projection_digits,
        catalog_id=catalog_id,
    )


def _create_csv_table(
    database: str,
    table: str,
    path: str,
    columns_types: Dict[str, str],
    partitions_types: Optional[Dict[str, str]],
    bucketing_info: Optional[Tuple[List[str], int]],
    description: Optional[str],
    compression: Optional[str],
    parameters: Optional[Dict[str, str]],
    columns_comments: Optional[Dict[str, str]],
    mode: str,
    catalog_versioning: bool,
    sep: str,
    skip_header_line_count: Optional[int],
    boto3_session: Optional[boto3.Session],
    projection_enabled: bool,
    projection_types: Optional[Dict[str, str]],
    projection_ranges: Optional[Dict[str, str]],
    projection_values: Optional[Dict[str, str]],
    projection_intervals: Optional[Dict[str, str]],
    projection_digits: Optional[Dict[str, str]],
    catalog_table_input: Optional[Dict[str, Any]],
    catalog_id: Optional[str],
) -> None:
    table = sanitize_table_name(table=table)
    partitions_types = {} if partitions_types is None else partitions_types
    _logger.debug("catalog_table_input: %s", catalog_table_input)
    table_input: Dict[str, Any]
    if (catalog_table_input is not None) and (mode in ("append", "overwrite_partitions")):
        table_input = catalog_table_input
        catalog_cols: Dict[str, str] = {x["Name"]: x["Type"] for x in table_input["StorageDescriptor"]["Columns"]}
        for c, t in columns_types.items():
            if c not in catalog_cols:
                _logger.debug("New column %s with type %s.", c, t)
                raise exceptions.InvalidArgumentValue(
                    f"Schema change detected - New column {c}. Schema evolution is not supported for CSV tables."
                )
    else:
        table_input = _csv_table_definition(
            table=table,
            path=path,
            columns_types=columns_types,
            partitions_types=partitions_types,
            bucketing_info=bucketing_info,
            compression=compression,
            sep=sep,
            skip_header_line_count=skip_header_line_count,
        )
    table_exist: bool = catalog_table_input is not None
    _logger.debug("table_exist: %s", table_exist)
    _create_table(
        database=database,
        table=table,
        description=description,
        parameters=parameters,
        columns_comments=columns_comments,
        mode=mode,
        catalog_versioning=catalog_versioning,
        boto3_session=boto3_session,
        table_input=table_input,
        table_exist=table_exist,
        partitions_types=partitions_types,
        projection_enabled=projection_enabled,
        projection_types=projection_types,
        projection_ranges=projection_ranges,
        projection_values=projection_values,
        projection_intervals=projection_intervals,
        projection_digits=projection_digits,
        catalog_id=catalog_id,
    )


@apply_configs
def upsert_table_parameters(
    parameters: Dict[str, str],
    database: str,
    table: str,
    catalog_versioning: bool = False,
    catalog_id: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> Dict[str, str]:
    """Insert or Update the received parameters.

    Parameters
    ----------
    parameters : Dict[str, str]
        e.g. {"source": "mysql", "destination":  "datalake"}
    database : str
        Database name.
    table : str
        Table name.
    catalog_versioning : bool
        If True and `mode="overwrite"`, creates an archived version of the table catalog before updating it.
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Dict[str, str]
       All parameters after the upsert.

    Examples
    --------
    >>> import awswrangler as wr
    >>> pars = wr.catalog.upsert_table_parameters(
    ...     parameters={"source": "mysql", "destination":  "datalake"},
    ...     database="...",
    ...     table="...")

    """
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    table_input: Optional[Dict[str, str]] = _get_table_input(
        database=database, table=table, boto3_session=session, catalog_id=catalog_id
    )
    if table_input is None:
        raise exceptions.InvalidArgumentValue(f"Table {database}.{table} does not exist.")
    return _upsert_table_parameters(
        parameters=parameters,
        database=database,
        boto3_session=session,
        catalog_id=catalog_id,
        table_input=table_input,
        catalog_versioning=catalog_versioning,
    )


@apply_configs
def overwrite_table_parameters(
    parameters: Dict[str, str],
    database: str,
    table: str,
    catalog_versioning: bool = False,
    catalog_id: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> Dict[str, str]:
    """Overwrite all existing parameters.

    Parameters
    ----------
    parameters : Dict[str, str]
        e.g. {"source": "mysql", "destination":  "datalake"}
    database : str
        Database name.
    table : str
        Table name.
    catalog_versioning : bool
        If True and `mode="overwrite"`, creates an archived version of the table catalog before updating it.
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Dict[str, str]
       All parameters after the overwrite (The same received).

    Examples
    --------
    >>> import awswrangler as wr
    >>> pars = wr.catalog.overwrite_table_parameters(
    ...     parameters={"source": "mysql", "destination":  "datalake"},
    ...     database="...",
    ...     table="...")

    """
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    table_input: Optional[Dict[str, Any]] = _get_table_input(
        database=database, table=table, catalog_id=catalog_id, boto3_session=session
    )
    if table_input is None:
        raise exceptions.InvalidTable(f"Table {table} does not exist on database {database}.")
    return _overwrite_table_parameters(
        parameters=parameters,
        database=database,
        catalog_id=catalog_id,
        table_input=table_input,
        boto3_session=session,
        catalog_versioning=catalog_versioning,
    )


@apply_configs
def create_database(
    name: str,
    description: Optional[str] = None,
    catalog_id: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> None:
    """Create a database in AWS Glue Catalog.

    Parameters
    ----------
    name : str
        Database name.
    description : str, optional
        A Descrption for the Database.
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    None
        None.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.catalog.create_database(
    ...     name='awswrangler_test'
    ... )
    """
    args: Dict[str, str] = {}
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
    args["Name"] = name
    if description is not None:
        args["Description"] = description

    if catalog_id is not None:
        client_glue.create_database(CatalogId=catalog_id, DatabaseInput=args)
    else:
        client_glue.create_database(DatabaseInput=args)


@apply_configs
def create_parquet_table(
    database: str,
    table: str,
    path: str,
    columns_types: Dict[str, str],
    partitions_types: Optional[Dict[str, str]] = None,
    bucketing_info: Optional[Tuple[List[str], int]] = None,
    catalog_id: Optional[str] = None,
    compression: Optional[str] = None,
    description: Optional[str] = None,
    parameters: Optional[Dict[str, str]] = None,
    columns_comments: Optional[Dict[str, str]] = None,
    mode: str = "overwrite",
    catalog_versioning: bool = False,
    projection_enabled: bool = False,
    projection_types: Optional[Dict[str, str]] = None,
    projection_ranges: Optional[Dict[str, str]] = None,
    projection_values: Optional[Dict[str, str]] = None,
    projection_intervals: Optional[Dict[str, str]] = None,
    projection_digits: Optional[Dict[str, str]] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> None:
    """Create a Parquet Table (Metadata Only) in the AWS Glue Catalog.

    'https://docs.aws.amazon.com/athena/latest/ug/data-types.html'

    Parameters
    ----------
    database : str
        Database name.
    table : str
        Table name.
    path : str
        Amazon S3 path (e.g. s3://bucket/prefix/).
    columns_types: Dict[str, str]
        Dictionary with keys as column names and values as data types (e.g. {'col0': 'bigint', 'col1': 'double'}).
    partitions_types: Dict[str, str], optional
        Dictionary with keys as partition names and values as data types (e.g. {'col2': 'date'}).
    bucketing_info: Tuple[List[str], int], optional
        Tuple consisting of the column names used for bucketing as the first element and the number of buckets as the
        second element.
        Only `str`, `int` and `bool` are supported as column data types for bucketing.
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    compression: str, optional
        Compression style (``None``, ``snappy``, ``gzip``, etc).
    description: str, optional
        Table description
    parameters: Dict[str, str], optional
        Key/value pairs to tag the table.
    columns_comments: Dict[str, str], optional
        Columns names and the related comments (e.g. {'col0': 'Column 0.', 'col1': 'Column 1.', 'col2': 'Partition.'}).
    mode: str
        'overwrite' to recreate any possible existing table or 'append' to keep any possible existing table.
    catalog_versioning : bool
        If True and `mode="overwrite"`, creates an archived version of the table catalog before updating it.
    projection_enabled : bool
        Enable Partition Projection on Athena (https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html)
    projection_types : Optional[Dict[str, str]]
        Dictionary of partitions names and Athena projections types.
        Valid types: "enum", "integer", "date", "injected"
        https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
        (e.g. {'col_name': 'enum', 'col2_name': 'integer'})
    projection_ranges: Optional[Dict[str, str]]
        Dictionary of partitions names and Athena projections ranges.
        https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
        (e.g. {'col_name': '0,10', 'col2_name': '-1,8675309'})
    projection_values: Optional[Dict[str, str]]
        Dictionary of partitions names and Athena projections values.
        https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
        (e.g. {'col_name': 'A,B,Unknown', 'col2_name': 'foo,boo,bar'})
    projection_intervals: Optional[Dict[str, str]]
        Dictionary of partitions names and Athena projections intervals.
        https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
        (e.g. {'col_name': '1', 'col2_name': '5'})
    projection_digits: Optional[Dict[str, str]]
        Dictionary of partitions names and Athena projections digits.
        https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
        (e.g. {'col_name': '1', 'col2_name': '2'})
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    None
        None.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.catalog.create_parquet_table(
    ...     database='default',
    ...     table='my_table',
    ...     path='s3://bucket/prefix/',
    ...     columns_types={'col0': 'bigint', 'col1': 'double'},
    ...     partitions_types={'col2': 'date'},
    ...     compression='snappy',
    ...     description='My own table!',
    ...     parameters={'source': 'postgresql'},
    ...     columns_comments={'col0': 'Column 0.', 'col1': 'Column 1.', 'col2': 'Partition.'}
    ... )

    """
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    catalog_table_input: Optional[Dict[str, Any]] = _get_table_input(
        database=database, table=table, boto3_session=session, catalog_id=catalog_id
    )
    _create_parquet_table(
        database=database,
        table=table,
        path=path,
        columns_types=columns_types,
        partitions_types=partitions_types,
        bucketing_info=bucketing_info,
        catalog_id=catalog_id,
        compression=compression,
        description=description,
        parameters=parameters,
        columns_comments=columns_comments,
        mode=mode,
        catalog_versioning=catalog_versioning,
        projection_enabled=projection_enabled,
        projection_types=projection_types,
        projection_ranges=projection_ranges,
        projection_values=projection_values,
        projection_intervals=projection_intervals,
        projection_digits=projection_digits,
        boto3_session=boto3_session,
        catalog_table_input=catalog_table_input,
    )


@apply_configs
def create_csv_table(
    database: str,
    table: str,
    path: str,
    columns_types: Dict[str, str],
    partitions_types: Optional[Dict[str, str]] = None,
    bucketing_info: Optional[Tuple[List[str], int]] = None,
    compression: Optional[str] = None,
    description: Optional[str] = None,
    parameters: Optional[Dict[str, str]] = None,
    columns_comments: Optional[Dict[str, str]] = None,
    mode: str = "overwrite",
    catalog_versioning: bool = False,
    sep: str = ",",
    skip_header_line_count: Optional[int] = None,
    boto3_session: Optional[boto3.Session] = None,
    projection_enabled: bool = False,
    projection_types: Optional[Dict[str, str]] = None,
    projection_ranges: Optional[Dict[str, str]] = None,
    projection_values: Optional[Dict[str, str]] = None,
    projection_intervals: Optional[Dict[str, str]] = None,
    projection_digits: Optional[Dict[str, str]] = None,
    catalog_id: Optional[str] = None,
) -> None:
    """Create a CSV Table (Metadata Only) in the AWS Glue Catalog.

    'https://docs.aws.amazon.com/athena/latest/ug/data-types.html'

    Parameters
    ----------
    database : str
        Database name.
    table : str
        Table name.
    path : str
        Amazon S3 path (e.g. s3://bucket/prefix/).
    columns_types: Dict[str, str]
        Dictionary with keys as column names and values as data types (e.g. {'col0': 'bigint', 'col1': 'double'}).
    partitions_types: Dict[str, str], optional
        Dictionary with keys as partition names and values as data types (e.g. {'col2': 'date'}).
    bucketing_info: Tuple[List[str], int], optional
        Tuple consisting of the column names used for bucketing as the first element and the number of buckets as the
        second element.
        Only `str`, `int` and `bool` are supported as column data types for bucketing.
    compression : str, optional
        Compression style (``None``, ``gzip``, etc).
    description : str, optional
        Table description
    parameters : Dict[str, str], optional
        Key/value pairs to tag the table.
    columns_comments: Dict[str, str], optional
        Columns names and the related comments (e.g. {'col0': 'Column 0.', 'col1': 'Column 1.', 'col2': 'Partition.'}).
    mode : str
        'overwrite' to recreate any possible axisting table or 'append' to keep any possible axisting table.
    catalog_versioning : bool
        If True and `mode="overwrite"`, creates an archived version of the table catalog before updating it.
    sep : str
        String of length 1. Field delimiter for the output file.
    skip_header_line_count : Optional[int]
        Number of Lines to skip regarding to the header.
    projection_enabled : bool
        Enable Partition Projection on Athena (https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html)
    projection_types : Optional[Dict[str, str]]
        Dictionary of partitions names and Athena projections types.
        Valid types: "enum", "integer", "date", "injected"
        https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
        (e.g. {'col_name': 'enum', 'col2_name': 'integer'})
    projection_ranges: Optional[Dict[str, str]]
        Dictionary of partitions names and Athena projections ranges.
        https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
        (e.g. {'col_name': '0,10', 'col2_name': '-1,8675309'})
    projection_values: Optional[Dict[str, str]]
        Dictionary of partitions names and Athena projections values.
        https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
        (e.g. {'col_name': 'A,B,Unknown', 'col2_name': 'foo,boo,bar'})
    projection_intervals: Optional[Dict[str, str]]
        Dictionary of partitions names and Athena projections intervals.
        https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
        (e.g. {'col_name': '1', 'col2_name': '5'})
    projection_digits: Optional[Dict[str, str]]
        Dictionary of partitions names and Athena projections digits.
        https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
        (e.g. {'col_name': '1', 'col2_name': '2'})
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.

    Returns
    -------
    None
        None.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.catalog.create_csv_table(
    ...     database='default',
    ...     table='my_table',
    ...     path='s3://bucket/prefix/',
    ...     columns_types={'col0': 'bigint', 'col1': 'double'},
    ...     partitions_types={'col2': 'date'},
    ...     compression='gzip',
    ...     description='My own table!',
    ...     parameters={'source': 'postgresql'},
    ...     columns_comments={'col0': 'Column 0.', 'col1': 'Column 1.', 'col2': 'Partition.'}
    ... )

    """
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    catalog_table_input: Optional[Dict[str, Any]] = _get_table_input(
        database=database, table=table, boto3_session=session, catalog_id=catalog_id
    )
    _create_csv_table(
        database=database,
        table=table,
        path=path,
        columns_types=columns_types,
        partitions_types=partitions_types,
        bucketing_info=bucketing_info,
        catalog_id=catalog_id,
        compression=compression,
        description=description,
        parameters=parameters,
        columns_comments=columns_comments,
        mode=mode,
        catalog_versioning=catalog_versioning,
        projection_enabled=projection_enabled,
        projection_types=projection_types,
        projection_ranges=projection_ranges,
        projection_values=projection_values,
        projection_intervals=projection_intervals,
        projection_digits=projection_digits,
        boto3_session=boto3_session,
        catalog_table_input=catalog_table_input,
        sep=sep,
        skip_header_line_count=skip_header_line_count,
    )
