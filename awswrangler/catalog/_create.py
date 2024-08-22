"""AWS Glue Catalog Module."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Literal

import boto3

from awswrangler import _utils, exceptions, typing
from awswrangler._config import apply_configs
from awswrangler.catalog._definitions import (
    _csv_table_definition,
    _json_table_definition,
    _orc_table_definition,
    _parquet_table_definition,
)
from awswrangler.catalog._delete import delete_all_partitions, delete_table_if_exists
from awswrangler.catalog._get import _get_table_input
from awswrangler.catalog._utils import _catalog_id, sanitize_column_name, sanitize_table_name

if TYPE_CHECKING:
    from mypy_boto3_glue import GlueClient

_logger: logging.Logger = logging.getLogger(__name__)


def _update_if_necessary(
    dic: dict[str, str | dict[str, str]], key: str, value: str | dict[str, str] | None, mode: str
) -> str:
    if value is not None:
        if key not in dic or dic[key] != value:
            dic[key] = value
            if mode in ("append", "overwrite_partitions"):
                return "update"
    return mode


def _create_table(  # noqa: PLR0912,PLR0915
    database: str,
    table: str,
    description: str | None,
    parameters: dict[str, str] | None,
    mode: str,
    catalog_versioning: bool,
    boto3_session: boto3.Session | None,
    table_input: dict[str, Any],
    table_exist: bool,
    partitions_types: dict[str, str] | None,
    columns_comments: dict[str, str] | None,
    columns_parameters: dict[str, dict[str, str]] | None,
    athena_partition_projection_settings: typing.AthenaPartitionProjectionSettings | None,
    catalog_id: str | None,
) -> None:
    # Description
    mode = _update_if_necessary(dic=table_input, key="Description", value=description, mode=mode)

    if "Parameters" not in table_input:
        table_input["Parameters"] = {}

    # Parameters
    parameters = parameters if parameters else {}
    for k, v in parameters.items():
        mode = _update_if_necessary(dic=table_input["Parameters"], key=k, value=v, mode=mode)

    # Projection
    projection_params = athena_partition_projection_settings if athena_partition_projection_settings else {}
    if athena_partition_projection_settings:
        table_input["Parameters"]["projection.enabled"] = "true"
        partitions_types = partitions_types if partitions_types else {}
        projection_types = projection_params.get("projection_types", {})
        projection_ranges = projection_params.get("projection_ranges", {})
        projection_values = projection_params.get("projection_values", {})
        projection_intervals = projection_params.get("projection_intervals", {})
        projection_digits = projection_params.get("projection_digits", {})
        projection_formats = projection_params.get("projection_formats", {})
        projection_types = {sanitize_column_name(k): v for k, v in projection_types.items()}
        projection_ranges = {sanitize_column_name(k): v for k, v in projection_ranges.items()}
        projection_values = {sanitize_column_name(k): v for k, v in projection_values.items()}
        projection_intervals = {sanitize_column_name(k): v for k, v in projection_intervals.items()}
        projection_digits = {sanitize_column_name(k): v for k, v in projection_digits.items()}
        projection_formats = {sanitize_column_name(k): v for k, v in projection_formats.items()}
        projection_storage_location_template = projection_params.get("projection_storage_location_template")
        for k, v in projection_types.items():
            dtype: str | None = partitions_types.get(k)
            if dtype is None and projection_storage_location_template is None:
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
        for k, v in projection_formats.items():
            mode = _update_if_necessary(
                dic=table_input["Parameters"], key=f"projection.{k}.format", value=str(v), mode=mode
            )
        mode = _update_if_necessary(
            table_input["Parameters"],
            key="storage.location.template",
            value=projection_storage_location_template,
            mode=mode,
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

    # Column parameters
    columns_parameters = columns_parameters if columns_parameters else {}
    columns_parameters = {sanitize_column_name(k): v for k, v in columns_parameters.items()}
    if columns_parameters:
        for col in table_input["StorageDescriptor"]["Columns"]:
            name: str = col["Name"]  # type: ignore[no-redef]
            if name in columns_parameters:
                mode = _update_if_necessary(dic=col, key="Parameters", value=columns_parameters[name], mode=mode)
        for par in table_input["PartitionKeys"]:
            name = par["Name"]
            if name in columns_parameters:
                mode = _update_if_necessary(dic=par, key="Parameters", value=columns_parameters[name], mode=mode)

    _logger.debug("table_input: %s", table_input)

    client_glue = _utils.client(service_name="glue", session=boto3_session)
    skip_archive: bool = not catalog_versioning
    if mode not in ("overwrite", "append", "overwrite_partitions", "update"):
        raise exceptions.InvalidArgument(
            f"{mode} is not a valid mode. It must be 'overwrite', 'append' or 'overwrite_partitions'."
        )
    args: dict[str, Any] = _catalog_id(
        catalog_id=catalog_id,
        DatabaseName=database,
        TableInput=table_input,
    )
    if table_exist:
        _logger.debug("Updating table (%s)...", mode)
        args["SkipArchive"] = skip_archive
        if mode == "overwrite":
            delete_all_partitions(table=table, database=database, catalog_id=catalog_id, boto3_session=boto3_session)
        if mode in ["overwrite", "update"]:
            client_glue.update_table(**args)
    else:
        try:
            _logger.debug("Creating table (%s)...", mode)
            client_glue.create_table(**args)
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
    client_glue: "GlueClient",
    catalog_id: str | None,
    database: str,
    table: str,
    table_input: dict[str, Any],
    boto3_session: boto3.Session,
) -> None:
    delete_table_if_exists(
        database=database,
        table=table,
        boto3_session=boto3_session,
        catalog_id=catalog_id,
    )
    args: dict[str, Any] = _catalog_id(
        catalog_id=catalog_id,
        DatabaseName=database,
        TableInput=table_input,
    )
    client_glue.create_table(**args)


def _upsert_table_parameters(
    parameters: dict[str, str],
    database: str,
    catalog_versioning: bool,
    catalog_id: str | None,
    table_input: dict[str, Any],
    boto3_session: boto3.Session | None,
) -> dict[str, str]:
    pars: dict[str, str] = table_input["Parameters"]
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
    parameters: dict[str, str],
    database: str,
    catalog_versioning: bool,
    catalog_id: str | None,
    table_input: dict[str, Any],
    boto3_session: boto3.Session | None,
) -> dict[str, str]:
    table_input["Parameters"] = parameters
    client_glue = _utils.client(service_name="glue", session=boto3_session)
    skip_archive: bool = not catalog_versioning
    client_glue.update_table(
        **_catalog_id(
            catalog_id=catalog_id,
            DatabaseName=database,
            TableInput=table_input,
            SkipArchive=skip_archive,
        )
    )
    return parameters


def _update_table_input(table_input: dict[str, Any], columns_types: dict[str, str], allow_reorder: bool = True) -> bool:
    column_updated = False

    catalog_cols: dict[str, str] = {x["Name"]: x["Type"] for x in table_input["StorageDescriptor"]["Columns"]}

    if not allow_reorder:
        for catalog_key, frame_key in zip(catalog_cols, columns_types):
            if catalog_key != frame_key:
                raise exceptions.InvalidArgumentValue(f"Column {frame_key} is out of order.")

    for c, t in columns_types.items():
        if c not in catalog_cols:
            _logger.debug("New column %s with type %s.", c, t)
            table_input["StorageDescriptor"]["Columns"].append({"Name": c, "Type": t})
            column_updated = True
        elif t != catalog_cols[c]:  # Data type change detected!
            raise exceptions.InvalidArgumentValue(
                f"Data type change detected on column {c} (Old type: {catalog_cols[c]} / New type {t})."
            )

    return column_updated


def _create_parquet_table(
    database: str,
    table: str,
    path: str,
    columns_types: dict[str, str],
    table_type: str | None,
    partitions_types: dict[str, str] | None,
    bucketing_info: typing.BucketingInfoTuple | None,
    catalog_id: str | None,
    compression: str | None,
    description: str | None,
    parameters: dict[str, str] | None,
    columns_comments: dict[str, str] | None,
    columns_parameters: dict[str, dict[str, str]] | None,
    mode: str,
    catalog_versioning: bool,
    athena_partition_projection_settings: typing.AthenaPartitionProjectionSettings | None,
    boto3_session: boto3.Session | None,
    catalog_table_input: dict[str, Any] | None,
) -> None:
    table = sanitize_table_name(table=table)
    columns_types = {sanitize_column_name(k): v for k, v in columns_types.items()}
    partitions_types = {} if partitions_types is None else partitions_types
    _logger.debug("catalog_table_input: %s", catalog_table_input)

    table_input: dict[str, Any]
    if (catalog_table_input is not None) and (mode in ("append", "overwrite_partitions")):
        table_input = catalog_table_input

        is_table_updated = _update_table_input(table_input, columns_types)
        if is_table_updated:
            mode = "update"
    else:
        table_input = _parquet_table_definition(
            table=table,
            path=path,
            columns_types=columns_types,
            table_type=table_type,
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
        columns_parameters=columns_parameters,
        mode=mode,
        catalog_versioning=catalog_versioning,
        boto3_session=boto3_session,
        table_input=table_input,
        table_exist=table_exist,
        partitions_types=partitions_types,
        athena_partition_projection_settings=athena_partition_projection_settings,
        catalog_id=catalog_id,
    )


def _create_orc_table(
    database: str,
    table: str,
    path: str,
    columns_types: dict[str, str],
    table_type: str | None,
    partitions_types: dict[str, str] | None,
    bucketing_info: typing.BucketingInfoTuple | None,
    catalog_id: str | None,
    compression: str | None,
    description: str | None,
    parameters: dict[str, str] | None,
    columns_comments: dict[str, str] | None,
    columns_parameters: dict[str, dict[str, str]] | None,
    mode: str,
    catalog_versioning: bool,
    athena_partition_projection_settings: typing.AthenaPartitionProjectionSettings | None,
    boto3_session: boto3.Session | None,
    catalog_table_input: dict[str, Any] | None,
) -> None:
    table = sanitize_table_name(table=table)
    columns_types = {sanitize_column_name(k): v for k, v in columns_types.items()}
    partitions_types = {} if partitions_types is None else partitions_types
    _logger.debug("catalog_table_input: %s", catalog_table_input)

    table_input: dict[str, Any]
    if (catalog_table_input is not None) and (mode in ("append", "overwrite_partitions")):
        table_input = catalog_table_input

        is_table_updated = _update_table_input(table_input, columns_types)
        if is_table_updated:
            mode = "update"
    else:
        table_input = _orc_table_definition(
            table=table,
            path=path,
            columns_types=columns_types,
            table_type=table_type,
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
        columns_parameters=columns_parameters,
        mode=mode,
        catalog_versioning=catalog_versioning,
        boto3_session=boto3_session,
        table_input=table_input,
        table_exist=table_exist,
        partitions_types=partitions_types,
        athena_partition_projection_settings=athena_partition_projection_settings,
        catalog_id=catalog_id,
    )


def _create_csv_table(
    database: str,
    table: str,
    path: str | None,
    columns_types: dict[str, str],
    table_type: str | None,
    partitions_types: dict[str, str] | None,
    bucketing_info: typing.BucketingInfoTuple | None,
    description: str | None,
    compression: str | None,
    parameters: dict[str, str] | None,
    columns_comments: dict[str, str] | None,
    columns_parameters: dict[str, dict[str, str]] | None,
    mode: str,
    catalog_versioning: bool,
    schema_evolution: bool,
    sep: str,
    skip_header_line_count: int | None,
    serde_library: str | None,
    serde_parameters: dict[str, str] | None,
    boto3_session: boto3.Session | None,
    athena_partition_projection_settings: typing.AthenaPartitionProjectionSettings | None,
    catalog_table_input: dict[str, Any] | None,
    catalog_id: str | None,
) -> None:
    table = sanitize_table_name(table=table)
    columns_types = {sanitize_column_name(k): v for k, v in columns_types.items()}
    partitions_types = {} if partitions_types is None else partitions_types
    _logger.debug("catalog_table_input: %s", catalog_table_input)

    if schema_evolution is False:
        _utils.check_schema_changes(columns_types=columns_types, table_input=catalog_table_input, mode=mode)

    table_input: dict[str, Any]
    if (catalog_table_input is not None) and (mode in ("append", "overwrite_partitions")):
        table_input = catalog_table_input

        is_table_updated = _update_table_input(table_input, columns_types, allow_reorder=False)
        if is_table_updated:
            mode = "update"

    else:
        table_input = _csv_table_definition(
            table=table,
            path=path,
            columns_types=columns_types,
            table_type=table_type,
            partitions_types=partitions_types,
            bucketing_info=bucketing_info,
            compression=compression,
            sep=sep,
            skip_header_line_count=skip_header_line_count,
            serde_library=serde_library,
            serde_parameters=serde_parameters,
        )
    table_exist: bool = catalog_table_input is not None
    _logger.debug("table_exist: %s", table_exist)
    _create_table(
        database=database,
        table=table,
        description=description,
        parameters=parameters,
        columns_comments=columns_comments,
        columns_parameters=columns_parameters,
        mode=mode,
        catalog_versioning=catalog_versioning,
        boto3_session=boto3_session,
        table_input=table_input,
        table_exist=table_exist,
        partitions_types=partitions_types,
        athena_partition_projection_settings=athena_partition_projection_settings,
        catalog_id=catalog_id,
    )


def _create_json_table(
    database: str,
    table: str,
    path: str,
    columns_types: dict[str, str],
    table_type: str | None,
    partitions_types: dict[str, str] | None,
    bucketing_info: typing.BucketingInfoTuple | None,
    description: str | None,
    compression: str | None,
    parameters: dict[str, str] | None,
    columns_comments: dict[str, str] | None,
    columns_parameters: dict[str, dict[str, str]] | None,
    mode: str,
    catalog_versioning: bool,
    schema_evolution: bool,
    serde_library: str | None,
    serde_parameters: dict[str, str] | None,
    boto3_session: boto3.Session | None,
    athena_partition_projection_settings: typing.AthenaPartitionProjectionSettings | None,
    catalog_table_input: dict[str, Any] | None,
    catalog_id: str | None,
) -> None:
    table = sanitize_table_name(table=table)
    columns_types = {sanitize_column_name(k): v for k, v in columns_types.items()}
    partitions_types = {} if partitions_types is None else partitions_types
    _logger.debug("catalog_table_input: %s", catalog_table_input)

    table_input: dict[str, Any]
    if schema_evolution is False:
        _utils.check_schema_changes(columns_types=columns_types, table_input=catalog_table_input, mode=mode)
    if (catalog_table_input is not None) and (mode in ("append", "overwrite_partitions")):
        table_input = catalog_table_input

        is_table_updated = _update_table_input(table_input, columns_types)
        if is_table_updated:
            mode = "update"

    else:
        table_input = _json_table_definition(
            table=table,
            path=path,
            columns_types=columns_types,
            table_type=table_type,
            partitions_types=partitions_types,
            bucketing_info=bucketing_info,
            compression=compression,
            serde_library=serde_library,
            serde_parameters=serde_parameters,
        )
    table_exist: bool = catalog_table_input is not None
    _logger.debug("table_exist: %s", table_exist)
    _create_table(
        database=database,
        table=table,
        description=description,
        parameters=parameters,
        columns_comments=columns_comments,
        columns_parameters=columns_parameters,
        mode=mode,
        catalog_versioning=catalog_versioning,
        boto3_session=boto3_session,
        table_input=table_input,
        table_exist=table_exist,
        partitions_types=partitions_types,
        athena_partition_projection_settings=athena_partition_projection_settings,
        catalog_id=catalog_id,
    )


@apply_configs
def upsert_table_parameters(
    parameters: dict[str, str],
    database: str,
    table: str,
    catalog_versioning: bool = False,
    catalog_id: str | None = None,
    boto3_session: boto3.Session | None = None,
) -> dict[str, str]:
    """Insert or Update the received parameters.

    Parameters
    ----------
    parameters
        e.g. {"source": "mysql", "destination":  "datalake"}
    database
        Database name.
    table
        Table name.
    catalog_versioning
        If True and `mode="overwrite"`, creates an archived version of the table catalog before updating it.
    catalog_id
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    boto3_session
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
       All parameters after the upsert.

    Examples
    --------
    >>> import awswrangler as wr
    >>> pars = wr.catalog.upsert_table_parameters(
    ...     parameters={"source": "mysql", "destination":  "datalake"},
    ...     database="...",
    ...     table="...",
    ... )

    """
    table_input: dict[str, str] | None = _get_table_input(
        database=database,
        table=table,
        boto3_session=boto3_session,
        catalog_id=catalog_id,
    )
    if table_input is None:
        raise exceptions.InvalidArgumentValue(f"Table {database}.{table} does not exist.")
    return _upsert_table_parameters(
        parameters=parameters,
        database=database,
        boto3_session=boto3_session,
        catalog_id=catalog_id,
        table_input=table_input,
        catalog_versioning=catalog_versioning,
    )


@apply_configs
def overwrite_table_parameters(
    parameters: dict[str, str],
    database: str,
    table: str,
    catalog_versioning: bool = False,
    catalog_id: str | None = None,
    boto3_session: boto3.Session | None = None,
) -> dict[str, str]:
    """Overwrite all existing parameters.

    Parameters
    ----------
    parameters
        e.g. {"source": "mysql", "destination":  "datalake"}
    database
        Database name.
    table
        Table name.
    catalog_versioning
        If True and `mode="overwrite"`, creates an archived version of the table catalog before updating it.
    catalog_id
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
       All parameters after the overwrite (The same received).

    Examples
    --------
    >>> import awswrangler as wr
    >>> pars = wr.catalog.overwrite_table_parameters(
    ...     parameters={"source": "mysql", "destination":  "datalake"},
    ...     database="...",
    ...     table="...")

    """
    table_input: dict[str, Any] | None = _get_table_input(
        database=database,
        table=table,
        catalog_id=catalog_id,
        boto3_session=boto3_session,
    )
    if table_input is None:
        raise exceptions.InvalidTable(f"Table {table} does not exist on database {database}.")
    return _overwrite_table_parameters(
        parameters=parameters,
        database=database,
        catalog_id=catalog_id,
        table_input=table_input,
        boto3_session=boto3_session,
        catalog_versioning=catalog_versioning,
    )


@apply_configs
def create_database(
    name: str,
    description: str | None = None,
    catalog_id: str | None = None,
    exist_ok: bool = False,
    database_input_args: dict[str, Any] | None = None,
    boto3_session: boto3.Session | None = None,
) -> None:
    """Create a database in AWS Glue Catalog.

    Parameters
    ----------
    name
        Database name.
    description
        A description for the Database.
    catalog_id
        The ID of the Data Catalog from which to retrieve Databases.
        If ``None`` is provided, the AWS account ID is used by default.
    exist_ok
        If set to ``True`` will not raise an Exception if a Database with the same already exists.
        In this case the description will be updated if it is different from the current one.
    database_input_args
        Additional metadata to pass to database creation. Supported arguments listed here:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.create_database
    boto3_session
        The default boto3 session will be used if **boto3_session** receive ``None``.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.catalog.create_database(
    ...     name='awswrangler_test'
    ... )
    """
    client_glue = _utils.client(service_name="glue", session=boto3_session)
    args: dict[str, Any] = {"Name": name, **database_input_args} if database_input_args else {"Name": name}
    if description is not None:
        args["Description"] = description

    try:
        r = client_glue.get_database(**_catalog_id(catalog_id=catalog_id, Name=name))
        if not exist_ok:
            raise exceptions.AlreadyExists(f"Database {name} already exists and <exist_ok> is set to False.")
        for k, v in args.items():
            if v != r["Database"].get(k, ""):
                client_glue.update_database(**_catalog_id(catalog_id=catalog_id, Name=name, DatabaseInput=args))
                break
    except client_glue.exceptions.EntityNotFoundException:
        client_glue.create_database(**_catalog_id(catalog_id=catalog_id, DatabaseInput=args))


@apply_configs
def create_parquet_table(
    database: str,
    table: str,
    path: str,
    columns_types: dict[str, str],
    table_type: str | None = None,
    partitions_types: dict[str, str] | None = None,
    bucketing_info: typing.BucketingInfoTuple | None = None,
    catalog_id: str | None = None,
    compression: str | None = None,
    description: str | None = None,
    parameters: dict[str, str] | None = None,
    columns_comments: dict[str, str] | None = None,
    columns_parameters: dict[str, dict[str, str]] | None = None,
    mode: Literal["overwrite", "append"] = "overwrite",
    catalog_versioning: bool = False,
    athena_partition_projection_settings: typing.AthenaPartitionProjectionSettings | None = None,
    boto3_session: boto3.Session | None = None,
) -> None:
    """Create a Parquet Table (Metadata Only) in the AWS Glue Catalog.

    'https://docs.aws.amazon.com/athena/latest/ug/data-types.html'

    Parameters
    ----------
    database
        Database name.
    table
        Table name.
    path
        Amazon S3 path (e.g. s3://bucket/prefix/).
    columns_types
        Dictionary with keys as column names and values as data types (e.g. {'col0': 'bigint', 'col1': 'double'}).
    table_type
        The type of the Glue Table. Set to ``EXTERNAL_TABLE`` if ``None``.
    partitions_types
        Dictionary with keys as partition names and values as data types (e.g. {'col2': 'date'}).
    bucketing_info
        Tuple consisting of the column names used for bucketing as the first element and the number of buckets as the
        second element.
        Only `str`, `int` and `bool` are supported as column data types for bucketing.
    catalog_id
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    compression
        Compression style (``None``, ``snappy``, ``gzip``, etc).
    description
        Table description
    parameters
        Key/value pairs to tag the table.
    columns_comments
        Columns names and the related comments (e.g. {'col0': 'Column 0.', 'col1': 'Column 1.', 'col2': 'Partition.'}).
    columns_parameters
        Columns names and the related parameters (e.g. {'col0': {'par0': 'Param 0', 'par1': 'Param 1'}}).
    mode
        'overwrite' to recreate any possible existing table or 'append' to keep any possible existing table.
    catalog_versioning
        If True and `mode="overwrite"`, creates an archived version of the table catalog before updating it.
    athena_partition_projection_settings
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
    boto3_session
        The default boto3 session will be used if **boto3_session** receive ``None``.

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
    catalog_table_input: dict[str, Any] | None = _get_table_input(
        database=database,
        table=table,
        boto3_session=boto3_session,
        catalog_id=catalog_id,
    )
    _create_parquet_table(
        database=database,
        table=table,
        path=path,
        columns_types=columns_types,
        table_type=table_type,
        partitions_types=partitions_types,
        bucketing_info=bucketing_info,
        catalog_id=catalog_id,
        compression=compression,
        description=description,
        parameters=parameters,
        columns_comments=columns_comments,
        columns_parameters=columns_parameters,
        mode=mode,
        catalog_versioning=catalog_versioning,
        athena_partition_projection_settings=athena_partition_projection_settings,
        boto3_session=boto3_session,
        catalog_table_input=catalog_table_input,
    )


@apply_configs
def create_orc_table(
    database: str,
    table: str,
    path: str,
    columns_types: dict[str, str],
    table_type: str | None = None,
    partitions_types: dict[str, str] | None = None,
    bucketing_info: typing.BucketingInfoTuple | None = None,
    catalog_id: str | None = None,
    compression: str | None = None,
    description: str | None = None,
    parameters: dict[str, str] | None = None,
    columns_comments: dict[str, str] | None = None,
    columns_parameters: dict[str, dict[str, str]] | None = None,
    mode: Literal["overwrite", "append"] = "overwrite",
    catalog_versioning: bool = False,
    athena_partition_projection_settings: typing.AthenaPartitionProjectionSettings | None = None,
    boto3_session: boto3.Session | None = None,
) -> None:
    """Create a ORC Table (Metadata Only) in the AWS Glue Catalog.

    'https://docs.aws.amazon.com/athena/latest/ug/data-types.html'

    Parameters
    ----------
    database
        Database name.
    table
        Table name.
    path
        Amazon S3 path (e.g. s3://bucket/prefix/).
    columns_types
        Dictionary with keys as column names and values as data types (e.g. {'col0': 'bigint', 'col1': 'double'}).
    table_type
        The type of the Glue Table. Set to EXTERNAL_TABLE if None.
    partitions_types
        Dictionary with keys as partition names and values as data types (e.g. {'col2': 'date'}).
    bucketing_info
        Tuple consisting of the column names used for bucketing as the first element and the number of buckets as the
        second element.
        Only `str`, `int` and `bool` are supported as column data types for bucketing.
    catalog_id
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    compression
        Compression style (``None``, ``snappy``, ``gzip``, etc).
    description
        Table description
    parameters
        Key/value pairs to tag the table.
    columns_comments
        Columns names and the related comments (e.g. {'col0': 'Column 0.', 'col1': 'Column 1.', 'col2': 'Partition.'}).
    columns_parameters
        Columns names and the related parameters (e.g. {'col0': {'par0': 'Param 0', 'par1': 'Param 1'}}).
    mode
        'overwrite' to recreate any possible existing table or 'append' to keep any possible existing table.
    catalog_versioning
        If True and `mode="overwrite"`, creates an archived version of the table catalog before updating it.
    athena_partition_projection_settings
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
    boto3_session
        The default boto3 session will be used if **boto3_session** receive ``None``.

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
    catalog_table_input: dict[str, Any] | None = _get_table_input(
        database=database,
        table=table,
        boto3_session=boto3_session,
        catalog_id=catalog_id,
    )
    _create_orc_table(
        database=database,
        table=table,
        path=path,
        columns_types=columns_types,
        table_type=table_type,
        partitions_types=partitions_types,
        bucketing_info=bucketing_info,
        catalog_id=catalog_id,
        compression=compression,
        description=description,
        parameters=parameters,
        columns_comments=columns_comments,
        columns_parameters=columns_parameters,
        mode=mode,
        catalog_versioning=catalog_versioning,
        athena_partition_projection_settings=athena_partition_projection_settings,
        boto3_session=boto3_session,
        catalog_table_input=catalog_table_input,
    )


@apply_configs
def create_csv_table(
    database: str,
    table: str,
    path: str,
    columns_types: dict[str, str],
    table_type: str | None = None,
    partitions_types: dict[str, str] | None = None,
    bucketing_info: typing.BucketingInfoTuple | None = None,
    compression: str | None = None,
    description: str | None = None,
    parameters: dict[str, str] | None = None,
    columns_comments: dict[str, str] | None = None,
    columns_parameters: dict[str, dict[str, str]] | None = None,
    mode: Literal["overwrite", "append"] = "overwrite",
    catalog_versioning: bool = False,
    schema_evolution: bool = False,
    sep: str = ",",
    skip_header_line_count: int | None = None,
    serde_library: str | None = None,
    serde_parameters: dict[str, str] | None = None,
    boto3_session: boto3.Session | None = None,
    athena_partition_projection_settings: typing.AthenaPartitionProjectionSettings | None = None,
    catalog_id: str | None = None,
) -> None:
    r"""Create a CSV Table (Metadata Only) in the AWS Glue Catalog.

    'https://docs.aws.amazon.com/athena/latest/ug/data-types.html'

    Note
    ----
    Athena requires the columns in the underlying CSV files in S3 to be in the same order
    as the columns in the Glue data catalog.

    Parameters
    ----------
    database
        Database name.
    table
        Table name.
    path
        Amazon S3 path (e.g. s3://bucket/prefix/).
    columns_types
        Dictionary with keys as column names and values as data types (e.g. {'col0': 'bigint', 'col1': 'double'}).
    table_type
        The type of the Glue Table. Set to EXTERNAL_TABLE if None.
    partitions_types
        Dictionary with keys as partition names and values as data types (e.g. {'col2': 'date'}).
    bucketing_info
        Tuple consisting of the column names used for bucketing as the first element and the number of buckets as the
        second element.
        Only `str`, `int` and `bool` are supported as column data types for bucketing.
    compression
        Compression style (``None``, ``gzip``, etc).
    description
        Table description
    parameters
        Key/value pairs to tag the table.
    columns_comments
        Columns names and the related comments (e.g. {'col0': 'Column 0.', 'col1': 'Column 1.', 'col2': 'Partition.'}).
    columns_parameters
        Columns names and the related parameters (e.g. {'col0': {'par0': 'Param 0', 'par1': 'Param 1'}}).
    mode
        'overwrite' to recreate any possible existing table or 'append' to keep any possible existing table.
    catalog_versioning
        If True and `mode="overwrite"`, creates an archived version of the table catalog before updating it.
    schema_evolution
        If True allows schema evolution (new or missing columns), otherwise a exception will be raised.
        (Only considered if dataset=True and mode in ("append", "overwrite_partitions"))
        Related tutorial:
        https://aws-sdk-pandas.readthedocs.io/en/3.9.1/tutorials/014%20-%20Schema%20Evolution.html
    sep
        String of length 1. Field delimiter for the output file.
    skip_header_line_count
        Number of Lines to skip regarding to the header.
    serde_library
        Specifies the SerDe Serialization library which will be used. You need to provide the Class library name
        as a string.
        If no library is provided the default is `org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe`.
    serde_parameters
        Dictionary of initialization parameters for the SerDe.
        The default is `{"field.delim": sep, "escape.delim": "\\"}`.
    athena_partition_projection_settings
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
    boto3_session
        The default boto3 session will be used if **boto3_session** receive ``None``.
    catalog_id
        The ID of the Data Catalog from which to retrieve Databases.
        If ``None`` is provided, the AWS account ID is used by default.

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
    catalog_table_input: dict[str, Any] | None = _get_table_input(
        database=database,
        table=table,
        boto3_session=boto3_session,
        catalog_id=catalog_id,
    )
    _create_csv_table(
        database=database,
        table=table,
        path=path,
        columns_types=columns_types,
        table_type=table_type,
        partitions_types=partitions_types,
        bucketing_info=bucketing_info,
        catalog_id=catalog_id,
        compression=compression,
        description=description,
        parameters=parameters,
        columns_comments=columns_comments,
        columns_parameters=columns_parameters,
        mode=mode,
        catalog_versioning=catalog_versioning,
        schema_evolution=schema_evolution,
        athena_partition_projection_settings=athena_partition_projection_settings,
        boto3_session=boto3_session,
        catalog_table_input=catalog_table_input,
        sep=sep,
        skip_header_line_count=skip_header_line_count,
        serde_library=serde_library,
        serde_parameters=serde_parameters,
    )


@apply_configs
def create_json_table(
    database: str,
    table: str,
    path: str,
    columns_types: dict[str, str],
    table_type: str | None = None,
    partitions_types: dict[str, str] | None = None,
    bucketing_info: typing.BucketingInfoTuple | None = None,
    compression: str | None = None,
    description: str | None = None,
    parameters: dict[str, str] | None = None,
    columns_comments: dict[str, str] | None = None,
    columns_parameters: dict[str, dict[str, str]] | None = None,
    mode: Literal["overwrite", "append"] = "overwrite",
    catalog_versioning: bool = False,
    schema_evolution: bool = False,
    serde_library: str | None = None,
    serde_parameters: dict[str, str] | None = None,
    boto3_session: boto3.Session | None = None,
    athena_partition_projection_settings: typing.AthenaPartitionProjectionSettings | None = None,
    catalog_id: str | None = None,
) -> None:
    r"""Create a JSON Table (Metadata Only) in the AWS Glue Catalog.

    'https://docs.aws.amazon.com/athena/latest/ug/data-types.html'

    Parameters
    ----------
    database
        Database name.
    table
        Table name.
    path
        Amazon S3 path (e.g. s3://bucket/prefix/).
    columns_types
        Dictionary with keys as column names and values as data types (e.g. {'col0': 'bigint', 'col1': 'double'}).
    table_type
        The type of the Glue Table. Set to EXTERNAL_TABLE if None.
    partitions_types
        Dictionary with keys as partition names and values as data types (e.g. {'col2': 'date'}).
    bucketing_info
        Tuple consisting of the column names used for bucketing as the first element and the number of buckets as the
        second element.
        Only `str`, `int` and `bool` are supported as column data types for bucketing.
    compression
        Compression style (``None``, ``gzip``, etc).
    description
        Table description
    parameters
        Key/value pairs to tag the table.
    columns_comments
        Columns names and the related comments (e.g. {'col0': 'Column 0.', 'col1': 'Column 1.', 'col2': 'Partition.'}).
    columns_parameters
        Columns names and the related parameters (e.g. {'col0': {'par0': 'Param 0', 'par1': 'Param 1'}}).
    mode
        'overwrite' to recreate any possible existing table or 'append' to keep any possible existing table.
    catalog_versioning
        If True and `mode="overwrite"`, creates an archived version of the table catalog before updating it.
    schema_evolution
        If True allows schema evolution (new or missing columns), otherwise a exception will be raised.
        (Only considered if dataset=True and mode in ("append", "overwrite_partitions"))
        Related tutorial:
        https://aws-sdk-pandas.readthedocs.io/en/3.9.1/tutorials/014%20-%20Schema%20Evolution.html
    serde_library
        Specifies the SerDe Serialization library which will be used. You need to provide the Class library name
        as a string.
        If no library is provided the default is `org.openx.data.jsonserde.JsonSerDe`.
    serde_parameters
        Dictionary of initialization parameters for the SerDe.
        The default is `{"field.delim": sep, "escape.delim": "\\"}`.
    athena_partition_projection_settings
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
    boto3_session
        The default boto3 session will be used if **boto3_session** receive ``None``.
    catalog_id
        The ID of the Data Catalog from which to retrieve Databases.
        If ``None`` is provided, the AWS account ID is used by default.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.catalog.create_json_table(
    ...     database='default',
    ...     table='my_table',
    ...     path='s3://bucket/prefix/',
    ...     columns_types={'col0': 'bigint', 'col1': 'double'},
    ...     partitions_types={'col2': 'date'},
    ...     description='My very own JSON table!',
    ...     parameters={'source': 'postgresql'},
    ...     columns_comments={'col0': 'Column 0.', 'col1': 'Column 1.', 'col2': 'Partition.'}
    ... )

    """
    catalog_table_input: dict[str, Any] | None = _get_table_input(
        database=database, table=table, boto3_session=boto3_session, catalog_id=catalog_id
    )
    _create_json_table(
        database=database,
        table=table,
        path=path,
        columns_types=columns_types,
        table_type=table_type,
        partitions_types=partitions_types,
        bucketing_info=bucketing_info,
        catalog_id=catalog_id,
        compression=compression,
        description=description,
        parameters=parameters,
        columns_comments=columns_comments,
        columns_parameters=columns_parameters,
        mode=mode,
        catalog_versioning=catalog_versioning,
        schema_evolution=schema_evolution,
        athena_partition_projection_settings=athena_partition_projection_settings,
        boto3_session=boto3_session,
        catalog_table_input=catalog_table_input,
        serde_library=serde_library,
        serde_parameters=serde_parameters,
    )
