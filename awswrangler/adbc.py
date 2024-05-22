"""Amazon ADBC Module."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from urllib.parse import urlencode

import boto3

from awswrangler import _databases as _db_utils
from awswrangler import _utils, exceptions

if TYPE_CHECKING:
    try:
        import adbc_driver_postgresql.dbapi as pg_dbapi
        from adbc_driver_manager import dbapi
    except ImportError:
        pass
else:
    pg_dbapi = _utils.import_optional_dependency("adbc_driver_postgresql.dbapi")
    db_api = _utils.import_optional_dependency("adbc_driver_manager.dbapi")


_logger: logging.Logger = logging.getLogger(__name__)


def _validate_connection(con: "dbapi.Connection") -> None:
    if not isinstance(con, pg_dbapi.Connection):
        raise exceptions.InvalidConnection(
            "Invalid 'con' argument, please pass a "
            "adbc_driver_postgresql.dbapi.Connection object. "
            "Use adbc_driver_postgresql.dbapi.connect() to use "
            "credentials directly or wr.adbc.connect() to fetch it from the Glue Catalog."
        )


@_utils.check_optional_dependency(pg_dbapi, "pg_abapi")
def connect(
    connection: str | None = None,
    secret_id: str | None = None,
    catalog_id: str | None = None,
    dbname: str | None = None,
    timeout: int | None = None,
    boto3_session: boto3.Session | None = None,
) -> "dbapi.Connection":
    """
    Connect to a database using the ArrowDBC connector.

    Parameters
    ----------
    connection: str, optional
        Glue Catalog Connection name.
    secret_id: str, optional
        Specifies the secret containing the credentials that are used to connect to the database.
        You can specify either the Amazon Resource Name (ARN) or the friendly name of the secret.
    catalog_id: str, optional
        The ID of the Data Catalog.
        If none is provided, the AWS account ID is used by default.
    dbname: str, optional
        The name of a database.
    timeout : int, optional
        Timeout in seconds.
    boto3_session: boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    adbc_driver_manager.dbapi.Connection
        Connection object.
    """
    attrs: _db_utils.ConnectionAttributes = _db_utils.get_connection_attributes(
        connection=connection, secret_id=secret_id, catalog_id=catalog_id, dbname=dbname, boto3_session=boto3_session
    )
    if attrs.kind not in ("postgresql", "postgres"):
        raise exceptions.InvalidDatabaseType(
            f"Invalid connection type ({attrs.kind}. It must be a postgresql connection.)"
        )

    connection_arguments = {
        "host": attrs.host,
        "port": attrs.port,
        "user": attrs.user,
        "password": attrs.password,
    }
    if timeout:
        connection_arguments["connect_timeout"] = timeout

    return pg_dbapi.connect(uri=f"postgresql:///{attrs.database}?{urlencode(connection_arguments)}")  # type: ignore[no-any-return]
