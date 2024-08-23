"""Amazon QuickSight Describe Module."""

from __future__ import annotations

import logging
from typing import Any, cast

import boto3

from awswrangler import _utils, exceptions, sts
from awswrangler.quicksight._get_list import get_dashboard_id, get_data_source_id, get_dataset_id

_logger: logging.Logger = logging.getLogger(__name__)


def describe_dashboard(
    name: str | None = None,
    dashboard_id: str | None = None,
    account_id: str | None = None,
    boto3_session: boto3.Session | None = None,
) -> dict[str, Any]:
    """Describe a QuickSight dashboard by name or ID.

    Note
    ----
    You must pass a not None ``name`` or ``dashboard_id`` argument.

    Parameters
    ----------
    name
        Dashboard name.
    dashboard_id
        Dashboard ID.
    account_id
        If None, the account ID will be inferred from your boto3 session.
    boto3_session
        The default boto3 session will be used if **boto3_session** is ``None``.

    Returns
    -------
        Dashboard Description.

    Examples
    --------
    >>> import awswrangler as wr
    >>> description = wr.quicksight.describe_dashboard(name="my-dashboard")
    """
    if (name is None) and (dashboard_id is None):
        raise exceptions.InvalidArgument("You must pass a not None name or dashboard_id argument.")
    if account_id is None:
        account_id = sts.get_account_id(boto3_session=boto3_session)
    if (dashboard_id is None) and (name is not None):
        dashboard_id = get_dashboard_id(name=name, account_id=account_id, boto3_session=boto3_session)

    client = _utils.client(service_name="quicksight", session=boto3_session)
    dashboard_id = cast(str, dashboard_id)

    response = client.describe_dashboard(AwsAccountId=account_id, DashboardId=dashboard_id)
    return response["Dashboard"]  # type: ignore[return-value]


def describe_data_source(
    name: str | None = None,
    data_source_id: str | None = None,
    account_id: str | None = None,
    boto3_session: boto3.Session | None = None,
) -> dict[str, Any]:
    """Describe a QuickSight data source by name or ID.

    Note
    ----
    You must pass a not None ``name`` or ``data_source_id`` argument.

    Parameters
    ----------
    name
        Data source name.
    data_source_id
        Data source ID.
    account_id
        If None, the account ID will be inferred from your boto3 session.
    boto3_session
        The default boto3 session will be used if **boto3_session** is ``None``.

    Returns
    -------
        Data source Description.

    Examples
    --------
    >>> import awswrangler as wr
    >>> description = wr.quicksight.describe_data_source("...")
    """
    if (name is None) and (data_source_id is None):
        raise exceptions.InvalidArgument("You must pass a not None name or data_source_id argument.")
    if account_id is None:
        account_id = sts.get_account_id(boto3_session=boto3_session)
    if (data_source_id is None) and (name is not None):
        data_source_id = get_data_source_id(name=name, account_id=account_id, boto3_session=boto3_session)

    client = _utils.client(service_name="quicksight", session=boto3_session)
    data_source_id = cast(str, data_source_id)

    response = client.describe_data_source(AwsAccountId=account_id, DataSourceId=data_source_id)
    return response["DataSource"]  # type: ignore[return-value]


def describe_data_source_permissions(
    name: str | None = None,
    data_source_id: str | None = None,
    account_id: str | None = None,
    boto3_session: boto3.Session | None = None,
) -> dict[str, Any]:
    """Describe a QuickSight data source permissions by name or ID.

    Note
    ----
    You must pass a not None ``name`` or ``data_source_id`` argument.

    Parameters
    ----------
    name
        Data source name.
    data_source_id
        Data source ID.
    account_id
        If None, the account ID will be inferred from your boto3 session.
    boto3_session
        The default boto3 session will be used if **boto3_session** is ``None``.

    Returns
    -------
        Data source Permissions Description.

    Examples
    --------
    >>> import awswrangler as wr
    >>> description = wr.quicksight.describe_data_source_permissions("my-data-source")
    """
    if (name is None) and (data_source_id is None):
        raise exceptions.InvalidArgument("You must pass a not None name or data_source_id argument.")
    if account_id is None:
        account_id = sts.get_account_id(boto3_session=boto3_session)
    if (data_source_id is None) and (name is not None):
        data_source_id = get_data_source_id(name=name, account_id=account_id, boto3_session=boto3_session)

    client = _utils.client(service_name="quicksight", session=boto3_session)
    data_source_id = cast(str, data_source_id)

    response = client.describe_data_source_permissions(AwsAccountId=account_id, DataSourceId=data_source_id)
    return response["Permissions"]  # type: ignore[return-value]


def describe_dataset(
    name: str | None = None,
    dataset_id: str | None = None,
    account_id: str | None = None,
    boto3_session: boto3.Session | None = None,
) -> dict[str, Any]:
    """Describe a QuickSight dataset by name or ID.

    Note
    ----
    You must pass a not None ``name`` or ``dataset_id`` argument.

    Parameters
    ----------
    name
        Dataset name.
    dataset_id
        Dataset ID.
    account_id
        If None, the account ID will be inferred from your boto3 session.
    boto3_session
        The default boto3 session will be used if **boto3_session** is ``None``.

    Returns
    -------
        Dataset Description.

    Examples
    --------
    >>> import awswrangler as wr
    >>> description = wr.quicksight.describe_dataset("my-dataset")
    """
    if (name is None) and (dataset_id is None):
        raise exceptions.InvalidArgument("You must pass a not None name or dataset_id argument.")
    if account_id is None:
        account_id = sts.get_account_id(boto3_session=boto3_session)
    if (dataset_id is None) and (name is not None):
        dataset_id = get_dataset_id(name=name, account_id=account_id, boto3_session=boto3_session)

    client = _utils.client(service_name="quicksight", session=boto3_session)
    dataset_id = cast(str, dataset_id)

    response = client.describe_data_set(AwsAccountId=account_id, DataSetId=dataset_id)
    return response["DataSet"]  # type: ignore[return-value]


def describe_ingestion(
    ingestion_id: str,
    dataset_name: str | None = None,
    dataset_id: str | None = None,
    account_id: str | None = None,
    boto3_session: boto3.Session | None = None,
) -> dict[str, Any]:
    """Describe a QuickSight ingestion by ID.

    Note
    ----
    You must pass a not None value for ``dataset_name`` or ``dataset_id`` argument.

    Parameters
    ----------
    ingestion_id
        Ingestion ID.
    dataset_name
        Dataset name.
    dataset_id
        Dataset ID.
    account_id
        If None, the account ID will be inferred from your boto3 session.
    boto3_session
        The default boto3 session will be used if **boto3_session** is ``None``.

    Returns
    -------
        Ingestion Description.

    Examples
    --------
    >>> import awswrangler as wr
    >>> description = wr.quicksight.describe_dataset(ingestion_id="...", dataset_name="...")
    """
    if (dataset_name is None) and (dataset_id is None):
        raise exceptions.InvalidArgument("You must pass a not None name or dataset_id argument.")
    if account_id is None:
        account_id = sts.get_account_id(boto3_session=boto3_session)
    if (dataset_id is None) and (dataset_name is not None):
        dataset_id = get_dataset_id(name=dataset_name, account_id=account_id, boto3_session=boto3_session)

    client = _utils.client(service_name="quicksight", session=boto3_session)
    dataset_id = cast(str, dataset_id)

    response = client.describe_ingestion(IngestionId=ingestion_id, AwsAccountId=account_id, DataSetId=dataset_id)
    return response["Ingestion"]  # type: ignore[return-value]
