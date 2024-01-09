"""Amazon QuickSight Delete Module."""

from __future__ import annotations

import logging
import re
from typing import Any, Callable

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
    func_name: str, account_id: str | None = None, boto3_session: boto3.Session | None = None, **kwargs: Any
) -> None:
    if account_id is None:
        account_id = sts.get_account_id(boto3_session=boto3_session)
    client = _utils.client(service_name="quicksight", session=boto3_session)
    func: Callable[..., None] = getattr(client, func_name)
    func(AwsAccountId=account_id, **kwargs)


def delete_dashboard(
    name: str | None = None,
    dashboard_id: str | None = None,
    version_number: int | None = None,
    account_id: str | None = None,
    boto3_session: boto3.Session | None = None,
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
    if (dashboard_id is None) and (name is not None):
        dashboard_id = get_dashboard_id(name=name, account_id=account_id, boto3_session=boto3_session)
    args: dict[str, Any] = {
        "func_name": "delete_dashboard",
        "account_id": account_id,
        "boto3_session": boto3_session,
        "DashboardId": dashboard_id,
    }
    if version_number is not None:
        args["VersionNumber"] = version_number
    _delete(**args)


def delete_dataset(
    name: str | None = None,
    dataset_id: str | None = None,
    account_id: str | None = None,
    boto3_session: boto3.Session | None = None,
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
    if (dataset_id is None) and (name is not None):
        dataset_id = get_dataset_id(name=name, account_id=account_id, boto3_session=boto3_session)
    args: dict[str, Any] = {
        "func_name": "delete_data_set",
        "account_id": account_id,
        "boto3_session": boto3_session,
        "DataSetId": dataset_id,
    }
    _delete(**args)


def delete_data_source(
    name: str | None = None,
    data_source_id: str | None = None,
    account_id: str | None = None,
    boto3_session: boto3.Session | None = None,
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
    if (data_source_id is None) and (name is not None):
        data_source_id = get_data_source_id(name=name, account_id=account_id, boto3_session=boto3_session)
    args: dict[str, Any] = {
        "func_name": "delete_data_source",
        "account_id": account_id,
        "boto3_session": boto3_session,
        "DataSourceId": data_source_id,
    }
    _delete(**args)


def delete_template(
    name: str | None = None,
    template_id: str | None = None,
    version_number: int | None = None,
    account_id: str | None = None,
    boto3_session: boto3.Session | None = None,
) -> None:
    """Delete a template.

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
    if (template_id is None) and (name is not None):
        template_id = get_template_id(name=name, account_id=account_id, boto3_session=boto3_session)
    args: dict[str, Any] = {
        "func_name": "delete_template",
        "account_id": account_id,
        "boto3_session": boto3_session,
        "TemplateId": template_id,
    }
    if version_number is not None:
        args["VersionNumber"] = version_number
    _delete(**args)


def delete_all_dashboards(
    account_id: str | None = None, regex_filter: str | None = None, boto3_session: boto3.Session | None = None
) -> None:
    """Delete all dashboards.

    Parameters
    ----------
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    regex_filter : str, optional
        Regex regex_filter that will delete all dashboards with a match in their ``Name``
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
    if account_id is None:
        account_id = sts.get_account_id(boto3_session=boto3_session)
    for dashboard in list_dashboards(account_id=account_id, boto3_session=boto3_session):
        if regex_filter:
            if not re.search(f"^{regex_filter}$", dashboard["Name"]):
                continue
        delete_dashboard(dashboard_id=dashboard["DashboardId"], account_id=account_id, boto3_session=boto3_session)


def delete_all_datasets(
    account_id: str | None = None, regex_filter: str | None = None, boto3_session: boto3.Session | None = None
) -> None:
    """Delete all datasets.

    Parameters
    ----------
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    regex_filter : str, optional
        Regex regex_filter that will delete all datasets with a match in their ``Name``
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
    if account_id is None:
        account_id = sts.get_account_id(boto3_session=boto3_session)
    for dataset in list_datasets(account_id=account_id, boto3_session=boto3_session):
        if regex_filter:
            if not re.search(f"^{regex_filter}$", dataset["Name"]):
                continue
        delete_dataset(dataset_id=dataset["DataSetId"], account_id=account_id, boto3_session=boto3_session)


def delete_all_data_sources(
    account_id: str | None = None, regex_filter: str | None = None, boto3_session: boto3.Session | None = None
) -> None:
    """Delete all data sources.

    Parameters
    ----------
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    regex_filter : str, optional
        Regex regex_filter that will delete all data sources with a match in their ``Name``
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
    if account_id is None:
        account_id = sts.get_account_id(boto3_session=boto3_session)
    for data_source in list_data_sources(account_id=account_id, boto3_session=boto3_session):
        if regex_filter:
            if not re.search(f"^{regex_filter}$", data_source["Name"]):
                continue
        delete_data_source(
            data_source_id=data_source["DataSourceId"], account_id=account_id, boto3_session=boto3_session
        )


def delete_all_templates(
    account_id: str | None = None, regex_filter: str | None = None, boto3_session: boto3.Session | None = None
) -> None:
    """Delete all templates.

    Parameters
    ----------
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    regex_filter : str, optional
        Regex regex_filter that will delete all templates with a match in their ``Name``
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
    if account_id is None:
        account_id = sts.get_account_id(boto3_session=boto3_session)
    for template in list_templates(account_id=account_id, boto3_session=boto3_session):
        if regex_filter:
            if not re.search(f"^{regex_filter}$", template["Name"]):
                continue
        delete_template(template_id=template["TemplateId"], account_id=account_id, boto3_session=boto3_session)
