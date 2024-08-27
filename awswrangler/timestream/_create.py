"""Amazon Timestream Create Module."""

from __future__ import annotations

import logging
from typing import Any

import boto3

from awswrangler import _utils

_logger: logging.Logger = logging.getLogger(__name__)


def create_database(
    database: str,
    kms_key_id: str | None = None,
    tags: dict[str, str] | None = None,
    boto3_session: boto3.Session | None = None,
) -> str:
    """Create a new Timestream database.

    Note
    ----
    If the KMS key is not specified, the database will be encrypted with a
    Timestream managed KMS key located in your account.

    Parameters
    ----------
    database
        Database name.
    kms_key_id
        The KMS key for the database. If the KMS key is not specified,
        the database will be encrypted with a Timestream managed KMS key located in your account.
    tags
        Key/Value dict to put on the database.
        Tags enable you to categorize databases and/or tables, for example,
        by purpose, owner, or environment.
        e.g. {"foo": "boo", "bar": "xoo"})
    boto3_session
        The default boto3 session will be used if **boto3_session** is ``None``.

    Returns
    -------
        The Amazon Resource Name that uniquely identifies this database. (ARN)

    Examples
    --------
    Creating a database.

    >>> import awswrangler as wr
    >>> arn = wr.timestream.create_database("MyDatabase")

    """
    _logger.info("Creating Timestream database %s", database)
    client = _utils.client(service_name="timestream-write", session=boto3_session)
    args: dict[str, Any] = {"DatabaseName": database}
    if kms_key_id is not None:
        args["KmsKeyId"] = kms_key_id
    if tags is not None:
        args["Tags"] = [{"Key": k, "Value": v} for k, v in tags.items()]
    response = client.create_database(**args)
    return response["Database"]["Arn"]


def create_table(
    database: str,
    table: str,
    memory_retention_hours: int,
    magnetic_retention_days: int,
    tags: dict[str, str] | None = None,
    timestream_additional_kwargs: dict[str, Any] | None = None,
    boto3_session: boto3.Session | None = None,
) -> str:
    """Create a new Timestream database.

    Note
    ----
    If the KMS key is not specified, the database will be encrypted with a
    Timestream managed KMS key located in your account.

    Parameters
    ----------
    database
        Database name.
    table
        Table name.
    memory_retention_hours
        The duration for which data must be stored in the memory store.
    magnetic_retention_days
        The duration for which data must be stored in the magnetic store.
    tags
        Key/Value dict to put on the table.
        Tags enable you to categorize databases and/or tables, for example,
        by purpose, owner, or environment.
        e.g. {"foo": "boo", "bar": "xoo"})
    timestream_additional_kwargs
        Forwarded to botocore requests.
        e.g. timestream_additional_kwargs={'MagneticStoreWriteProperties': {'EnableMagneticStoreWrites': True}}
    boto3_session
        The default boto3 session will be used if **boto3_session** is ``None``.

    Returns
    -------
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
    _logger.info("Creating Timestream table %s in database %s", table, database)
    client = _utils.client(service_name="timestream-write", session=boto3_session)
    timestream_additional_kwargs = {} if timestream_additional_kwargs is None else timestream_additional_kwargs
    args: dict[str, Any] = {
        "DatabaseName": database,
        "TableName": table,
        "RetentionProperties": {
            "MemoryStoreRetentionPeriodInHours": memory_retention_hours,
            "MagneticStoreRetentionPeriodInDays": magnetic_retention_days,
        },
        **timestream_additional_kwargs,
    }
    if tags is not None:
        args["Tags"] = [{"Key": k, "Value": v} for k, v in tags.items()]
    response = client.create_table(**args)
    return response["Table"]["Arn"]
