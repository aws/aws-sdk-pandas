"""Amazon Redshift Connect Module (PRIVATE)."""

from __future__ import annotations

from typing import Any

import boto3

from awswrangler import _databases as _db_utils
from awswrangler import _utils, exceptions

redshift_connector = _utils.import_optional_dependency("redshift_connector")


def _validate_connection(con: "redshift_connector.Connection") -> None:  # type: ignore[name-defined]
    if not isinstance(con, redshift_connector.Connection):
        raise exceptions.InvalidConnection(
            "Invalid 'conn' argument, please pass a "
            "redshift_connector.Connection object. Use redshift_connector.connect() to use "
            "credentials directly or wr.redshift.connect() to fetch it from the Glue Catalog."
        )


@_utils.check_optional_dependency(redshift_connector, "redshift_connector")
def connect(
    connection: str | None = None,
    secret_id: str | None = None,
    catalog_id: str | None = None,
    dbname: str | None = None,
    boto3_session: boto3.Session | None = None,
    ssl: bool = True,
    timeout: int | None = None,
    max_prepared_statements: int = 1000,
    tcp_keepalive: bool = True,
    **kwargs: Any,
) -> "redshift_connector.Connection":  # type: ignore[name-defined]
    """Return a redshift_connector connection from a Glue Catalog or Secret Manager.

    Note
    ----
    You MUST pass a `connection` OR `secret_id`.
    Here is an example of the secret structure in Secrets Manager:
    {
    "host":"my-host.us-east-1.redshift.amazonaws.com",
    "username":"test",
    "password":"test",
    "engine":"redshift",
    "port":"5439",
    "dbname": "mydb"
    }


    https://github.com/aws/amazon-redshift-python-driver

    Parameters
    ----------
    connection : str, optional
        Glue Catalog Connection name.
    secret_id : str, optional
        Specifies the secret containing the connection details that you want to retrieve.
        You can specify either the Amazon Resource Name (ARN) or the friendly name of the secret.
    catalog_id : str, optional
        The ID of the Data Catalog.
        If none is provided, the AWS account ID is used by default.
    dbname : str, optional
        Optional database name to overwrite the stored one.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    ssl : bool
        This governs SSL encryption for TCP/IP sockets.
        This parameter is forward to redshift_connector.
        https://github.com/aws/amazon-redshift-python-driver
    timeout : int, optional
        This is the time in seconds before the connection to the server will time out.
        The default is None which means no timeout.
        This parameter is forward to redshift_connector.
        https://github.com/aws/amazon-redshift-python-driver
    max_prepared_statements : int
        This parameter is forward to redshift_connector.
        https://github.com/aws/amazon-redshift-python-driver
    tcp_keepalive : bool
        If True then use TCP keepalive. The default is True.
        This parameter is forward to redshift_connector.
        https://github.com/aws/amazon-redshift-python-driver
    **kwargs : Any
        Forwarded to redshift_connector.connect.
        e.g. is_serverless=True, serverless_acct_id='...', serverless_work_group='...'

    Returns
    -------
    redshift_connector.Connection
        redshift_connector connection.

    Examples
    --------
    Fetching Redshift connection from Glue Catalog

    >>> import awswrangler as wr
    >>> con = wr.redshift.connect("MY_GLUE_CONNECTION")
    >>> with con.cursor() as cursor:
    >>>     cursor.execute("SELECT 1")
    >>>     print(cursor.fetchall())
    >>> con.close()

    Fetching Redshift connection from Secrets Manager

    >>> import awswrangler as wr
    >>> con = wr.redshift.connect(secret_id="MY_SECRET")
    >>> with con.cursor() as cursor:
    >>>     cursor.execute("SELECT 1")
    >>>     print(cursor.fetchall())
    >>> con.close()

    """
    attrs: _db_utils.ConnectionAttributes = _db_utils.get_connection_attributes(
        connection=connection, secret_id=secret_id, catalog_id=catalog_id, dbname=dbname, boto3_session=boto3_session
    )
    if attrs.kind != "redshift":
        raise exceptions.InvalidDatabaseType(
            f"Invalid connection type ({attrs.kind}. It must be a redshift connection.)"
        )
    return redshift_connector.connect(
        user=attrs.user,
        database=attrs.database,
        password=attrs.password,
        port=int(attrs.port),
        host=attrs.host,
        ssl=ssl,
        timeout=timeout,
        max_prepared_statements=max_prepared_statements,
        tcp_keepalive=tcp_keepalive,
        **kwargs,
    )


@_utils.check_optional_dependency(redshift_connector, "redshift_connector")
def connect_temp(
    cluster_identifier: str,
    user: str,
    database: str | None = None,
    duration: int = 900,
    auto_create: bool = True,
    db_groups: list[str] | None = None,
    boto3_session: boto3.Session | None = None,
    ssl: bool = True,
    timeout: int | None = None,
    max_prepared_statements: int = 1000,
    tcp_keepalive: bool = True,
    **kwargs: Any,
) -> "redshift_connector.Connection":  # type: ignore[name-defined]
    """Return a redshift_connector temporary connection (No password required).

    https://github.com/aws/amazon-redshift-python-driver

    Parameters
    ----------
    cluster_identifier : str
        The unique identifier of a cluster.
        This parameter is case sensitive.
    user : str, optional
        The name of a database user.
    database : str, optional
        Database name. If None, the default Database is used.
    duration : int, optional
        The number of seconds until the returned temporary password expires.
        Constraint: minimum 900, maximum 3600.
        Default: 900
    auto_create : bool
        Create a database user with the name specified for the user named in user if one does not exist.
    db_groups : List[str], optional
        A list of the names of existing database groups that the user named in user will join for the current session,
        in addition to any group memberships for an existing user. If not specified, a new user is added only to PUBLIC.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    ssl : bool
        This governs SSL encryption for TCP/IP sockets.
        This parameter is forward to redshift_connector.
        https://github.com/aws/amazon-redshift-python-driver
    timeout : int, optional
        This is the time in seconds before the connection to the server will time out.
        The default is None which means no timeout.
        This parameter is forward to redshift_connector.
        https://github.com/aws/amazon-redshift-python-driver
    max_prepared_statements : int
        This parameter is forward to redshift_connector.
        https://github.com/aws/amazon-redshift-python-driver
    tcp_keepalive : bool
        If True then use TCP keepalive. The default is True.
        This parameter is forward to redshift_connector.
        https://github.com/aws/amazon-redshift-python-driver
    **kwargs : Any
        Forwarded to redshift_connector.connect.
        e.g. is_serverless=True, serverless_acct_id='...', serverless_work_group='...'

    Returns
    -------
    redshift_connector.Connection
        redshift_connector connection.

    Examples
    --------
    >>> import awswrangler as wr
    >>> con = wr.redshift.connect_temp(cluster_identifier="my-cluster", user="test")
    >>> with con.cursor() as cursor:
    >>>     cursor.execute("SELECT 1")
    >>>     print(cursor.fetchall())
    >>> con.close()

    """
    client_redshift = _utils.client(service_name="redshift", session=boto3_session)
    args: dict[str, Any] = {
        "DbUser": user,
        "ClusterIdentifier": cluster_identifier,
        "DurationSeconds": duration,
        "AutoCreate": auto_create,
    }
    if db_groups is not None:
        args["DbGroups"] = db_groups
    else:
        db_groups = []
    res = client_redshift.get_cluster_credentials(**args)
    cluster = client_redshift.describe_clusters(ClusterIdentifier=cluster_identifier)["Clusters"][0]
    return redshift_connector.connect(
        user=res["DbUser"],
        database=database if database else cluster["DBName"],
        password=res["DbPassword"],
        port=cluster["Endpoint"]["Port"],
        host=cluster["Endpoint"]["Address"],
        ssl=ssl,
        timeout=timeout,
        max_prepared_statements=max_prepared_statements,
        tcp_keepalive=tcp_keepalive,
        db_groups=db_groups,
        **kwargs,
    )
