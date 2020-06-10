"""Amazon QuickSight Get Module."""

import logging
from typing import Callable, List, Optional

import boto3  # type: ignore

from awswrangler import exceptions
from awswrangler.quicksight import _list

_logger: logging.Logger = logging.getLogger(__name__)


def _get_ids(
    name: str,
    func: Callable,
    attr_name: str,
    account_id: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> List[str]:
    ids: List[str] = []
    for item in func(account_id=account_id, boto3_session=boto3_session):
        if item["Name"] == name:
            ids.append(item[attr_name])
    return ids


def _get_id(
    name: str,
    func: Callable,
    attr_name: str,
    account_id: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> str:
    ids: List[str] = _get_ids(
        name=name, func=func, attr_name=attr_name, account_id=account_id, boto3_session=boto3_session
    )
    if len(ids) == 0:
        raise exceptions.InvalidArgument(f"There is no {attr_name} related with name {name}")
    if len(ids) > 1:
        raise exceptions.InvalidArgument(
            f"There is {len(ids)} {attr_name} with name {name}. "
            f"Please pass the id argument to specify "
            f"which one you would like to describe."
        )
    return ids[0]


def get_dashboard_ids(
    name: str, account_id: Optional[str] = None, boto3_session: Optional[boto3.Session] = None
) -> List[str]:
    """Get QuickSight dashboard IDs given a name.

    Note
    ----
    This function returns a list of ID because Quicksight accepts duplicated dashboard names,
    so you may have more than 1 ID for a given name.

    Parameters
    ----------
    name : str
        Dashboard name.
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    List[str]
        Dashboad IDs.

    Examples
    --------
    >>> import awswrangler as wr
    >>> ids = wr.quicksight.get_dashboard_ids(name="...")
    """
    return _get_ids(
        name=name,
        func=_list.list_dashboards,
        attr_name="DashboardId",
        account_id=account_id,
        boto3_session=boto3_session,
    )


def get_dashboard_id(name: str, account_id: Optional[str] = None, boto3_session: Optional[boto3.Session] = None) -> str:
    """Get QuickSight dashboard ID given a name and fails if there is more than 1 ID associated with this name.

    Parameters
    ----------
    name : str
        Dashboard name.
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    str
        Dashboad ID.

    Examples
    --------
    >>> import awswrangler as wr
    >>> my_id = wr.quicksight.get_dashboard_id(name="...")
    """
    return _get_id(
        name=name,
        func=_list.list_dashboards,
        attr_name="DashboardId",
        account_id=account_id,
        boto3_session=boto3_session,
    )


def get_dataset_ids(
    name: str, account_id: Optional[str] = None, boto3_session: Optional[boto3.Session] = None
) -> List[str]:
    """Get QuickSight dataset IDs given a name.

    Note
    ----
    This function returns a list of ID because Quicksight accepts duplicated datasets names,
    so you may have more than 1 ID for a given name.

    Parameters
    ----------
    name : str
        Dataset name.
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    List[str]
        Datasets IDs.

    Examples
    --------
    >>> import awswrangler as wr
    >>> ids = wr.quicksight.get_dataset_ids(name="...")
    """
    return _get_ids(
        name=name, func=_list.list_datasets, attr_name="DataSetId", account_id=account_id, boto3_session=boto3_session
    )


def get_dataset_id(name: str, account_id: Optional[str] = None, boto3_session: Optional[boto3.Session] = None) -> str:
    """Get QuickSight Dataset ID given a name and fails if there is more than 1 ID associated with this name.

    Parameters
    ----------
    name : str
        Dataset name.
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    str
        Dataset ID.

    Examples
    --------
    >>> import awswrangler as wr
    >>> my_id = wr.quicksight.get_dataset_id(name="...")
    """
    return _get_id(
        name=name, func=_list.list_datasets, attr_name="DataSetId", account_id=account_id, boto3_session=boto3_session
    )


def get_data_source_ids(
    name: str, account_id: Optional[str] = None, boto3_session: Optional[boto3.Session] = None
) -> List[str]:
    """Get QuickSight data source IDs given a name.

    Note
    ----
    This function returns a list of ID because Quicksight accepts duplicated data source names,
    so you may have more than 1 ID for a given name.

    Parameters
    ----------
    name : str
        Data source name.
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    List[str]
        Data source IDs.

    Examples
    --------
    >>> import awswrangler as wr
    >>> ids = wr.quicksight.get_data_source_ids(name="...")
    """
    return _get_ids(
        name=name,
        func=_list.list_data_sources,
        attr_name="DataSourceId",
        account_id=account_id,
        boto3_session=boto3_session,
    )


def get_data_source_id(
    name: str, account_id: Optional[str] = None, boto3_session: Optional[boto3.Session] = None
) -> str:
    """Get QuickSight data source ID given a name and fails if there is more than 1 ID associated with this name.

    Parameters
    ----------
    name : str
        Data source name.
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    str
        Dataset ID.

    Examples
    --------
    >>> import awswrangler as wr
    >>> my_id = wr.quicksight.get_data_source_id(name="...")
    """
    return _get_id(
        name=name,
        func=_list.list_data_sources,
        attr_name="DataSourceId",
        account_id=account_id,
        boto3_session=boto3_session,
    )


def get_template_ids(
    name: str, account_id: Optional[str] = None, boto3_session: Optional[boto3.Session] = None
) -> List[str]:
    """Get QuickSight template IDs given a name.

    Note
    ----
    This function returns a list of ID because Quicksight accepts duplicated templates names,
    so you may have more than 1 ID for a given name.

    Parameters
    ----------
    name : str
        Template name.
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    List[str]
        Tamplate IDs.

    Examples
    --------
    >>> import awswrangler as wr
    >>> ids = wr.quicksight.get_template_ids(name="...")
    """
    return _get_ids(
        name=name, func=_list.list_templates, attr_name="TemplateId", account_id=account_id, boto3_session=boto3_session
    )


def get_template_id(name: str, account_id: Optional[str] = None, boto3_session: Optional[boto3.Session] = None) -> str:
    """Get QuickSight template ID given a name and fails if there is more than 1 ID associated with this name.

    Parameters
    ----------
    name : str
        Template name.
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    str
        Template ID.

    Examples
    --------
    >>> import awswrangler as wr
    >>> my_id = wr.quicksight.get_template_id(name="...")
    """
    return _get_id(
        name=name, func=_list.list_templates, attr_name="TemplateId", account_id=account_id, boto3_session=boto3_session
    )


def get_data_source_arns(
    name: str, account_id: Optional[str] = None, boto3_session: Optional[boto3.Session] = None
) -> List[str]:
    """Get QuickSight Data source ARNs given a name.

    Note
    ----
    This function returns a list of ARNs because Quicksight accepts duplicated data source names,
    so you may have more than 1 ARN for a given name.

    Parameters
    ----------
    name : str
        Data source name.
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    List[str]
        Data source ARNs.

    Examples
    --------
    >>> import awswrangler as wr
    >>> arns = wr.quicksight.get_data_source_arns(name="...")
    """
    arns: List[str] = []
    for source in _list.list_data_sources(account_id=account_id, boto3_session=boto3_session):
        if source["Name"] == name:
            arns.append(source["Arn"])
    return arns


def get_data_source_arn(
    name: str, account_id: Optional[str] = None, boto3_session: Optional[boto3.Session] = None
) -> str:
    """Get QuickSight data source ARN given a name and fails if there is more than 1 ARN associated with this name.

    Note
    ----
    This function returns a list of ARNs because Quicksight accepts duplicated data source names,
    so you may have more than 1 ARN for a given name.

    Parameters
    ----------
    name : str
        Data source name.
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    str
        Data source ARN.

    Examples
    --------
    >>> import awswrangler as wr
    >>> arn = wr.quicksight.get_data_source_arn("...")
    """
    arns: List[str] = get_data_source_arns(name=name, account_id=account_id, boto3_session=boto3_session)
    if len(arns) == 0:
        raise exceptions.InvalidArgument(f"There is not data source with name {name}")
    if len(arns) > 1:
        raise exceptions.InvalidArgument(
            f"There is more than 1 data source with name {name}. "
            f"Please pass the data_source_arn argument to specify "
            f"which one you would like to describe."
        )
    return arns[0]
