"""Amazon QuickSight Delete Module."""

import logging
from typing import Any, Callable, Dict, Optional

import boto3

from awswrangler import _utils, exceptions, sts
from awswrangler.quicksight._get_list import (
    get_dashboard_id,
    get_data_source_id,
    get_dataset_id,
    get_template_id,
    list_dashboards,
    list_data_sources,
    list_datasets,
    list_templates,
)

_logger: logging.Logger = logging.getLogger(__name__)


def _delete(
    func_name: str, account_id: Optional[str] = None, boto3_session: Optional[boto3.Session] = None, **kwargs: Any
) -> None:
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    if account_id is None:
        account_id = sts.get_account_id(boto3_session=session)
    client: boto3.client = _utils.client(service_name="quicksight", session=session)
    func: Callable[..., None] = getattr(client, func_name)
    func(AwsAccountId=account_id, **kwargs)


def delete_dashboard(
    name: Optional[str] = None,
    dashboard_id: Optional[str] = None,
    version_number: Optional[int] = None,
    account_id: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> None:
    """Delete a dashboard.

    Note
    ----
    You must pass a not None ``name`` or ``dashboard_id`` argument.

    Parameters
    ----------
    name : str, optional
        Dashboard name.
    dashboard_id : str, optional
        The ID for the dashboard.
    version_number : int, optional
        The version number of the dashboard. If the version number property is provided,
        only the specified version of the dashboard is deleted.
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    None
        None.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.quicksight.delete_dashboard(name="...")
    """
    if (name is None) and (dashboard_id is None):
        raise exceptions.InvalidArgument("You must pass a not None name or dashboard_id argument.")
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    if (dashboard_id is None) and (name is not None):
        dashboard_id = get_dashboard_id(name=name, account_id=account_id, boto3_session=session)
    args: Dict[str, Any] = {
        "func_name": "delete_dashboard",
        "account_id": account_id,
        "boto3_session": session,
        "DashboardId": dashboard_id,
    }
    if version_number is not None:
        args["VersionNumber"] = version_number
    _delete(**args)


def delete_dataset(
    name: Optional[str] = None,
    dataset_id: Optional[str] = None,
    account_id: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> None:
    """Delete a dataset.

    Note
    ----
    You must pass a not None ``name`` or ``dataset_id`` argument.

    Parameters
    ----------
    name : str, optional
        Dashboard name.
    dataset_id : str, optional
        The ID for the dataset.
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    None
        None.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.quicksight.delete_dataset(name="...")
    """
    if (name is None) and (dataset_id is None):
        raise exceptions.InvalidArgument("You must pass a not None name or dataset_id argument.")
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    if (dataset_id is None) and (name is not None):
        dataset_id = get_dataset_id(name=name, account_id=account_id, boto3_session=session)
    args: Dict[str, Any] = {
        "func_name": "delete_data_set",
        "account_id": account_id,
        "boto3_session": session,
        "DataSetId": dataset_id,
    }
    _delete(**args)


def delete_data_source(
    name: Optional[str] = None,
    data_source_id: Optional[str] = None,
    account_id: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> None:
    """Delete a data source.

    Note
    ----
    You must pass a not None ``name`` or ``data_source_id`` argument.

    Parameters
    ----------
    name : str, optional
        Dashboard name.
    data_source_id : str, optional
        The ID for the data source.
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    None
        None.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.quicksight.delete_data_source(name="...")
    """
    if (name is None) and (data_source_id is None):
        raise exceptions.InvalidArgument("You must pass a not None name or data_source_id argument.")
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    if (data_source_id is None) and (name is not None):
        data_source_id = get_data_source_id(name=name, account_id=account_id, boto3_session=session)
    args: Dict[str, Any] = {
        "func_name": "delete_data_source",
        "account_id": account_id,
        "boto3_session": session,
        "DataSourceId": data_source_id,
    }
    _delete(**args)


def delete_template(
    name: Optional[str] = None,
    template_id: Optional[str] = None,
    version_number: Optional[int] = None,
    account_id: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> None:
    """Delete a tamplate.

    Note
    ----
    You must pass a not None ``name`` or ``template_id`` argument.

    Parameters
    ----------
    name : str, optional
        Dashboard name.
    template_id : str, optional
        The ID for the dashboard.
    version_number : int, optional
        Specifies the version of the template that you want to delete.
        If you don't provide a version number, it deletes all versions of the template.
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    None
        None.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.quicksight.delete_template(name="...")
    """
    if (name is None) and (template_id is None):
        raise exceptions.InvalidArgument("You must pass a not None name or template_id argument.")
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    if (template_id is None) and (name is not None):
        template_id = get_template_id(name=name, account_id=account_id, boto3_session=session)
    args: Dict[str, Any] = {
        "func_name": "delete_template",
        "account_id": account_id,
        "boto3_session": session,
        "TemplateId": template_id,
    }
    if version_number is not None:
        args["VersionNumber"] = version_number
    _delete(**args)


def delete_all_dashboards(account_id: Optional[str] = None, boto3_session: Optional[boto3.Session] = None) -> None:
    """Delete all dashboards.

    Parameters
    ----------
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    None
        None.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.quicksight.delete_all_dashboards()
    """
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    if account_id is None:
        account_id = sts.get_account_id(boto3_session=session)
    for dashboard in list_dashboards(account_id=account_id, boto3_session=session):
        delete_dashboard(dashboard_id=dashboard["DashboardId"], account_id=account_id, boto3_session=session)


def delete_all_datasets(account_id: Optional[str] = None, boto3_session: Optional[boto3.Session] = None) -> None:
    """Delete all datasets.

    Parameters
    ----------
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    None
        None.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.quicksight.delete_all_datasets()
    """
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    if account_id is None:
        account_id = sts.get_account_id(boto3_session=session)
    for dataset in list_datasets(account_id=account_id, boto3_session=session):
        delete_dataset(dataset_id=dataset["DataSetId"], account_id=account_id, boto3_session=session)


def delete_all_data_sources(account_id: Optional[str] = None, boto3_session: Optional[boto3.Session] = None) -> None:
    """Delete all data sources.

    Parameters
    ----------
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    None
        None.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.quicksight.delete_all_data_sources()
    """
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    if account_id is None:
        account_id = sts.get_account_id(boto3_session=session)
    for data_source in list_data_sources(account_id=account_id, boto3_session=session):
        delete_data_source(data_source_id=data_source["DataSourceId"], account_id=account_id, boto3_session=session)


def delete_all_templates(account_id: Optional[str] = None, boto3_session: Optional[boto3.Session] = None) -> None:
    """Delete all templates.

    Parameters
    ----------
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    None
        None.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.quicksight.delete_all_templates()
    """
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    if account_id is None:
        account_id = sts.get_account_id(boto3_session=session)
    for template in list_templates(account_id=account_id, boto3_session=session):
        delete_template(template_id=template["TemplateId"], account_id=account_id, boto3_session=session)
