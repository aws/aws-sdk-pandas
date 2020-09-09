"""Amazon QuickSight Describe Module."""

import logging
from typing import Any, Dict, Optional, cast

import boto3

from awswrangler import _utils, exceptions, sts
from awswrangler.quicksight._get_list import get_dashboard_id, get_data_source_id, get_dataset_id

_logger: logging.Logger = logging.getLogger(__name__)


def describe_dashboard(
    name: Optional[str] = None,
    dashboard_id: Optional[str] = None,
    account_id: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> Dict[str, Any]:
    """Describe a QuickSight dashboard by name or ID.

    Note
    ----
    You must pass a not None ``name`` or ``dashboard_id`` argument.

    Parameters
    ----------
    name : str, optional
        Dashboard name.
    dashboard_id : str, optional
        Dashboard ID.
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Dict[str, Any]
        Dashboad Description.

    Examples
    --------
    >>> import awswrangler as wr
    >>> description = wr.quicksight.describe_dashboard(name="my-dashboard")
    """
    if (name is None) and (dashboard_id is None):
        raise exceptions.InvalidArgument("You must pass a not None name or dashboard_id argument.")
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    if account_id is None:
        account_id = sts.get_account_id(boto3_session=session)
    if (dashboard_id is None) and (name is not None):
        dashboard_id = get_dashboard_id(name=name, account_id=account_id, boto3_session=session)
    client: boto3.client = _utils.client(service_name="quicksight", session=session)
    return cast(
        Dict[str, Any], client.describe_dashboard(AwsAccountId=account_id, DashboardId=dashboard_id)["Dashboard"]
    )


def describe_data_source(
    name: Optional[str] = None,
    data_source_id: Optional[str] = None,
    account_id: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> Dict[str, Any]:
    """Describe a QuickSight data source by name or ID.

    Note
    ----
    You must pass a not None ``name`` or ``data_source_id`` argument.

    Parameters
    ----------
    name : str, optional
        Data source name.
    data_source_id : str, optional
        Data source ID.
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Dict[str, Any]
        Data source Description.

    Examples
    --------
    >>> import awswrangler as wr
    >>> description = wr.quicksight.describe_data_source("...")
    """
    if (name is None) and (data_source_id is None):
        raise exceptions.InvalidArgument("You must pass a not None name or data_source_id argument.")
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    if account_id is None:
        account_id = sts.get_account_id(boto3_session=session)
    if (data_source_id is None) and (name is not None):
        data_source_id = get_data_source_id(name=name, account_id=account_id, boto3_session=session)
    client: boto3.client = _utils.client(service_name="quicksight", session=session)
    return cast(
        Dict[str, Any], client.describe_data_source(AwsAccountId=account_id, DataSourceId=data_source_id)["DataSource"]
    )


def describe_data_source_permissions(
    name: Optional[str] = None,
    data_source_id: Optional[str] = None,
    account_id: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> Dict[str, Any]:
    """Describe a QuickSight data source permissions by name or ID.

    Note
    ----
    You must pass a not None ``name`` or ``data_source_id`` argument.

    Parameters
    ----------
    name : str, optional
        Data source name.
    data_source_id : str, optional
        Data source ID.
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Dict[str, Any]
        Data source Permissions Description.

    Examples
    --------
    >>> import awswrangler as wr
    >>> description = wr.quicksight.describe_data_source_permissions("my-data-source")
    """
    if (name is None) and (data_source_id is None):
        raise exceptions.InvalidArgument("You must pass a not None name or data_source_id argument.")
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    if account_id is None:
        account_id = sts.get_account_id(boto3_session=session)
    if (data_source_id is None) and (name is not None):
        data_source_id = get_data_source_id(name=name, account_id=account_id, boto3_session=session)
    client: boto3.client = _utils.client(service_name="quicksight", session=session)
    return cast(
        Dict[str, Any],
        client.describe_data_source_permissions(AwsAccountId=account_id, DataSourceId=data_source_id)["Permissions"],
    )


def describe_dataset(
    name: Optional[str] = None,
    dataset_id: Optional[str] = None,
    account_id: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> Dict[str, Any]:
    """Describe a QuickSight dataset by name or ID.

    Note
    ----
    You must pass a not None ``name`` or ``dataset_id`` argument.

    Parameters
    ----------
    name : str, optional
        Dataset name.
    dataset_id : str, optional
        Dataset ID.
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Dict[str, Any]
        Dataset Description.

    Examples
    --------
    >>> import awswrangler as wr
    >>> description = wr.quicksight.describe_dataset("my-dataset")
    """
    if (name is None) and (dataset_id is None):
        raise exceptions.InvalidArgument("You must pass a not None name or dataset_id argument.")
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    if account_id is None:
        account_id = sts.get_account_id(boto3_session=session)
    if (dataset_id is None) and (name is not None):
        dataset_id = get_dataset_id(name=name, account_id=account_id, boto3_session=session)
    client: boto3.client = _utils.client(service_name="quicksight", session=session)
    return cast(Dict[str, Any], client.describe_data_set(AwsAccountId=account_id, DataSetId=dataset_id)["DataSet"])


def describe_ingestion(
    ingestion_id: str,
    dataset_name: Optional[str] = None,
    dataset_id: Optional[str] = None,
    account_id: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> Dict[str, Any]:
    """Describe a QuickSight ingestion by ID.

    Note
    ----
    You must pass a not None value for ``dataset_name`` or ``dataset_id`` argument.

    Parameters
    ----------
    ingestion_id : str
        Ingestion ID.
    dataset_name : str, optional
        Dataset name.
    dataset_id : str, optional
        Dataset ID.
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Dict[str, Any]
        Ingestion Description.

    Examples
    --------
    >>> import awswrangler as wr
    >>> description = wr.quicksight.describe_dataset(ingestion_id="...", dataset_name="...")
    """
    if (dataset_name is None) and (dataset_id is None):
        raise exceptions.InvalidArgument("You must pass a not None name or dataset_id argument.")
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    if account_id is None:
        account_id = sts.get_account_id(boto3_session=session)
    if (dataset_id is None) and (dataset_name is not None):
        dataset_id = get_dataset_id(name=dataset_name, account_id=account_id, boto3_session=session)
    client: boto3.client = _utils.client(service_name="quicksight", session=session)
    return cast(
        Dict[str, Any],
        client.describe_ingestion(IngestionId=ingestion_id, AwsAccountId=account_id, DataSetId=dataset_id)["Ingestion"],
    )
