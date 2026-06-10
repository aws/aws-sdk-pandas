"""Amazon Redshift Connect Module."""

from __future__ import annotations

import importlib.util
import logging
from typing import Any

import boto3

import awswrangler.pandas as pd
from awswrangler import _utils, exceptions
from awswrangler._databases import ConnectionAttributes, _get_connection_attributes

_redshift_connector_found = importlib.util.find_spec("redshift_connector")
if _redshift_connector_found:
    import redshift_connector

_logger: logging.Logger = logging.getLogger(__name__)


@_utils.check_optional_dependency(_redshift_connector_found, "redshift_connector")
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
    idp_token: str | None = None,
    idp_token_type: str | None = None,
    idc_region: str | None = None,
    issuer_url: str | None = None,
    listen_port: int | None = None,
    idc_client_display_name: str | None = None,
    idp_response_timeout: int | None = None,
    **kwargs: Any,
) -> "redshift_connector.Connection":
    """Return a redshift_connector connection from a Glue Catalog or Secret Manager.

    Note
    ----
    You MUST pass a connection OR secret_id.
    Here is an example of the secret structure in Secrets Manager:
    { "host":"my-host.us-east-1.redshift.amazonaws.com", "username":"test",
      "password":"test", "engine":"redshift", "port":"5439", "dbname": "mydb" }

    https://github.com/aws/amazon-redshift-python-driver

    Parameters
    ----------
    connection : str, optional
        Glue Catalog Connection name.
    secret_id : str, optional
        Specifies the secret containing the connection details that you want to retrieve.
    catalog_id : str, optional
        The ID of the Data Catalog. If none is provided, the AWS account ID is used by default.
    dbname : str, optional
        Optional database name to override the one found in the connection details.
    boto3_session : boto3.Session, optional
        The default boto3 session will be used if boto3_session is None.
    ssl : bool
        True to enable ssl, False to disable ssl. Default is True.
    timeout : int, optional
        Connection timeout in seconds.
    max_prepared_statements : int
        The maximum number of prepared statements. Default is 1000.
    tcp_keepalive : bool
        If True then use TCP keepalive. The default is True.
        This parameter is forward to redshift_connector.
        https://github.com/aws/amazon-redshift-python-driver
    idp_token : str, optional
        An AWS IAM Identity Center vended access token or an OpenID Connect (OIDC) JSON
        Web Token (JWT) from a web identity provider connected with AWS IAM Identity Center.
        When provided, ``IdpTokenAuthPlugin`` is used for Trusted Identity Propagation
        so that Redshift applies per-user permissions instead of the IAM role of the runtime.
        Requires ``idp_token_type`` to also be set.
        See: https://docs.aws.amazon.com/redshift/latest/mgmt/redshift-iam-access-control-idp-connect-oauth.html
    idp_token_type : str, optional
        The type of token supplied in ``idp_token``. Accepted values:

        * ``"ACCESS_TOKEN"`` – Use when ``idp_token`` is an AWS IAM Identity Center
          vended access token.
        * ``"EXT_JWT"`` – Use when ``idp_token`` is an OpenID Connect (OIDC) JSON Web
          Token (JWT) from an external web identity provider connected to IAM Identity Center.

        Required when ``idp_token`` is set.
    idc_region : str, optional
        The AWS Region where the AWS IAM Identity Center instance is located
        (e.g. ``"us-east-1"``). Used when ``BrowserIdcAuthPlugin`` authentication is
        requested via ``credentials_provider="BrowserIdcAuthPlugin"`` in **kwargs.
    issuer_url : str, optional
        The AWS IAM Identity Center server instance endpoint URL. Required when using
        ``BrowserIdcAuthPlugin`` (i.e. ``credentials_provider="BrowserIdcAuthPlugin"``
        in **kwargs).
        Example: ``"https://identitycenter.amazonaws.com/ssoins-g5j2k70sn4yc5nsc"``
    listen_port : int, optional
        The port that the Redshift driver uses to receive the ``auth_code`` response from
        AWS IAM Identity Center through the browser redirect. Only relevant for
        ``BrowserIdcAuthPlugin``.
    idc_client_display_name : str, optional
        The display name the IAM Identity Center client uses for the application in the
        IAM Identity Center single sign-on consent popup. Only relevant for
        ``BrowserIdcAuthPlugin``.
    idp_response_timeout : int, optional
        The amount of time in seconds that the Redshift driver waits for the
        authentication flow to complete. Only relevant for ``BrowserIdcAuthPlugin``.
    **kwargs : Any
        Forwarded to redshift_connector.connect.
        e.g. is_serverless=True, serverless_acct_id='...', serverless_work_group='...'

    Returns
    -------
    redshift_connector.Connection
        Connection to Redshift.

    Examples
    --------
    Fetching Redshift connection from Glue Catalog

    >>> import awswrangler as wr
    >>> with wr.redshift.connect("MY_GLUE_CONNECTION") as con:
    ...     with con.cursor() as cursor:
    ...         cursor.execute("SELECT 1")
    ...         print(cursor.fetchall())

    Fetching Redshift connection from Secrets Manager

    >>> import awswrangler as wr
    >>> with wr.redshift.connect(secret_id="MY_SECRET") as con:
    ...     with con.cursor() as cursor:
    ...         cursor.execute("SELECT 1")

    Using IAM Trusted Identity Propagation via IdpTokenAuthPlugin

    >>> import awswrangler as wr
    >>> con = wr.redshift.connect(
    ...     connection="MY_GLUE_CONNECTION",
    ...     idp_token="<iam-idc-access-token-or-oidc-jwt>",
    ...     idp_token_type="ACCESS_TOKEN",
    ... )

    Using BrowserIdcAuthPlugin for interactive SSO

    >>> import awswrangler as wr
    >>> con = wr.redshift.connect(
    ...     connection="MY_GLUE_CONNECTION",
    ...     credentials_provider="BrowserIdcAuthPlugin",
    ...     idc_region="us-east-1",
    ...     issuer_url="https://identitycenter.amazonaws.com/ssoins-xxx",
    ... )

    """
    attrs: ConnectionAttributes = _get_connection_attributes(
        connection=connection,
        secret_id=secret_id,
        catalog_id=catalog_id,
        dbname=dbname,
        boto3_session=boto3_session,
    )
    connect_kwargs: dict[str, Any] = {
        "host": attrs.host,
        "database": attrs.database,
        "port": attrs.port,
        "ssl": ssl,
        "timeout": timeout,
        "max_prepared_statements": max_prepared_statements,
        "tcp_keepalive": tcp_keepalive,
        **kwargs,
    }

    if idp_token is not None:
        # --- Trusted Identity Propagation via IdpTokenAuthPlugin ---
        if idp_token_type is None:
            raise exceptions.InvalidArgumentCombination(
                "idp_token_type must be set when idp_token is provided. "
                "Accepted values: 'ACCESS_TOKEN' or 'EXT_JWT'."
            )
        _valid_token_types = {"ACCESS_TOKEN", "EXT_JWT"}
        if idp_token_type.upper() not in _valid_token_types:
            raise exceptions.InvalidArgumentValue(
                f"idp_token_type must be one of {_valid_token_types}, got '{idp_token_type}'."
            )
        _logger.debug("Using IdpTokenAuthPlugin for Trusted Identity Propagation.")
        connect_kwargs["credentials_provider"] = "IdpTokenAuthPlugin"
        connect_kwargs["token"] = idp_token
        connect_kwargs["token_type"] = idp_token_type.upper()
        connect_kwargs.setdefault("user", "")
        connect_kwargs.setdefault("password", "")
    elif kwargs.get("credentials_provider") == "BrowserIdcAuthPlugin":
        # --- BrowserIdcAuthPlugin: inject supplemental IDC parameters ---
        _logger.debug("Enriching BrowserIdcAuthPlugin parameters for Trusted Identity Propagation.")
        if idc_region is not None:
            connect_kwargs["idc_region"] = idc_region
        if issuer_url is not None:
            connect_kwargs["issuer_url"] = issuer_url
        if listen_port is not None:
            connect_kwargs["listen_port"] = listen_port
        if idc_client_display_name is not None:
            connect_kwargs["idc_client_display_name"] = idc_client_display_name
        if idp_response_timeout is not None:
            connect_kwargs["idp_response_timeout"] = idp_response_timeout
        connect_kwargs.setdefault("user", "")
        connect_kwargs.setdefault("password", "")
    else:
        # --- Standard authentication path (username + password) ---
        connect_kwargs["user"] = attrs.user
        connect_kwargs["password"] = attrs.password

    if attrs.ssl_context is not None:
        connect_kwargs["ssl_context"] = attrs.ssl_context
    _logger.debug("connect_kwargs: %s", {k: v for k, v in connect_kwargs.items() if k != "password"})
    return redshift_connector.connect(**connect_kwargs)
