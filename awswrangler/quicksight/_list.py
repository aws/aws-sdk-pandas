"""Amazon QuickSight List Module."""

import logging
from typing import Any, Callable, Dict, List, Optional

import boto3  # type: ignore

from awswrangler import _utils

_logger: logging.Logger = logging.getLogger(__name__)


def _list(
    func_name: str,
    attr_name: str,
    account_id: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
    **kwargs,
) -> List[Dict[str, Any]]:
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    if account_id is None:
        account_id = _utils.get_account_id(boto3_session=session)
    client: boto3.client = _utils.client(service_name="quicksight", session=session)
    func: Callable = getattr(client, func_name)
    response = func(AwsAccountId=account_id, **kwargs)
    next_token: str = response.get("NextToken", None)
    result: List[Dict[str, Any]] = response[attr_name]
    while next_token is not None:
        response = func(AwsAccountId=account_id, NextToken=next_token, **kwargs)
        next_token = response.get("NextToken", None)
        result += response[attr_name]
    return result


def list_dashboards(
    account_id: Optional[str] = None, boto3_session: Optional[boto3.Session] = None
) -> List[Dict[str, Any]]:
    """List dashboards in an AWS account.

    Parameters
    ----------
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    List[Dict[str, Any]]
        Dashboards.

    Examples
    --------
    >>> import awswrangler as wr
    >>> dashboards = wr.quicksight.list_dashboards()
    """
    return _list(
        func_name="list_dashboards",
        attr_name="DashboardSummaryList",
        account_id=account_id,
        boto3_session=boto3_session,
    )


def list_datasets(
    account_id: Optional[str] = None, boto3_session: Optional[boto3.Session] = None
) -> List[Dict[str, Any]]:
    """List all QuickSight datasets summaries.

    Parameters
    ----------
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    List[Dict[str, Any]]
        Datasets summaries.

    Examples
    --------
    >>> import awswrangler as wr
    >>> datasets = wr.quicksight.list_datasets()
    """
    return _list(
        func_name="list_data_sets", attr_name="DataSetSummaries", account_id=account_id, boto3_session=boto3_session
    )


def list_data_sources(
    account_id: Optional[str] = None, boto3_session: Optional[boto3.Session] = None
) -> List[Dict[str, Any]]:
    """List all QuickSight Data sources summaries.

    Parameters
    ----------
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    List[Dict[str, Any]]
        Data sources summaries.

    Examples
    --------
    >>> import awswrangler as wr
    >>> sources = wr.quicksight.list_data_sources()
    """
    return _list(
        func_name="list_data_sources", attr_name="DataSources", account_id=account_id, boto3_session=boto3_session
    )


def list_templates(
    account_id: Optional[str] = None, boto3_session: Optional[boto3.Session] = None
) -> List[Dict[str, Any]]:
    """List all QuickSight templates.

    Parameters
    ----------
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    List[Dict[str, Any]]
        Templates summaries.

    Examples
    --------
    >>> import awswrangler as wr
    >>> templates = wr.quicksight.list_templates()
    """
    return _list(
        func_name="list_templates", attr_name="TemplateSummaryList", account_id=account_id, boto3_session=boto3_session
    )


def list_group_memberships(
    group_name: str,
    namespace: str = "default",
    account_id: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> List[Dict[str, Any]]:
    """List all QuickSight Group memberships.

    Parameters
    ----------
    group_name : str
        The name of the group that you want to see a membership list of.
    namespace : str
        The namespace. Currently, you should set this to default .
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    List[Dict[str, Any]]
        Group memberships.

    Examples
    --------
    >>> import awswrangler as wr
    >>> memberships = wr.quicksight.list_group_memberships()
    """
    return _list(
        func_name="list_group_memberships",
        attr_name="GroupMemberList",
        account_id=account_id,
        boto3_session=boto3_session,
        GroupName=group_name,
        Namespace=namespace,
    )


def list_groups(
    namespace: str = "default", account_id: Optional[str] = None, boto3_session: Optional[boto3.Session] = None
) -> List[Dict[str, Any]]:
    """List all QuickSight Groups.

    Parameters
    ----------
    namespace : str
        The namespace. Currently, you should set this to default .
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    List[Dict[str, Any]]
        Groups.

    Examples
    --------
    >>> import awswrangler as wr
    >>> groups = wr.quicksight.list_groups()
    """
    return _list(
        func_name="list_groups",
        attr_name="GroupList",
        account_id=account_id,
        boto3_session=boto3_session,
        Namespace=namespace,
    )


def list_iam_policy_assignments(
    status: Optional[str] = None,
    namespace: str = "default",
    account_id: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> List[Dict[str, Any]]:
    """List IAM policy assignments in the current Amazon QuickSight account.

    Parameters
    ----------
    status : str, optional
        The status of the assignments.
        'ENABLED'|'DRAFT'|'DISABLED'
    namespace : str
        The namespace. Currently, you should set this to default .
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    List[Dict[str, Any]]
        IAM policy assignments.

    Examples
    --------
    >>> import awswrangler as wr
    >>> assigns = wr.quicksight.list_iam_policy_assignments()
    """
    args: Dict[str, Any] = {
        "func_name": "list_iam_policy_assignments",
        "attr_name": "IAMPolicyAssignments",
        "account_id": account_id,
        "boto3_session": boto3_session,
        "Namespace": namespace,
    }
    if status is not None:
        args["AssignmentStatus"] = status
    return _list(**args)


def list_iam_policy_assignments_for_user(
    user_name: str,
    namespace: str = "default",
    account_id: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> List[Dict[str, Any]]:
    """List all the IAM policy assignments.

    Including the Amazon Resource Names (ARNs) for the IAM policies assigned
    to the specified user and group or groups that the user belongs to.

    Parameters
    ----------
    user_name : str
        The name of the user.
    namespace : str
        The namespace. Currently, you should set this to default .
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    List[Dict[str, Any]]
        IAM policy assignments.

    Examples
    --------
    >>> import awswrangler as wr
    >>> assigns = wr.quicksight.list_iam_policy_assignments_for_user()
    """
    return _list(
        func_name="list_iam_policy_assignments_for_user",
        attr_name="ActiveAssignments",
        account_id=account_id,
        boto3_session=boto3_session,
        UserName=user_name,
        Namespace=namespace,
    )


def list_user_groups(
    user_name: str,
    namespace: str = "default",
    account_id: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> List[Dict[str, Any]]:
    """List the Amazon QuickSight groups that an Amazon QuickSight user is a member of.

    Parameters
    ----------
    user_name: str:
        The Amazon QuickSight user name that you want to list group memberships for.
    namespace : str
        The namespace. Currently, you should set this to default .
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    List[Dict[str, Any]]
        Groups.

    Examples
    --------
    >>> import awswrangler as wr
    >>> groups = wr.quicksight.list_user_groups()
    """
    return _list(
        func_name="list_user_groups",
        attr_name="GroupList",
        account_id=account_id,
        boto3_session=boto3_session,
        UserName=user_name,
        Namespace=namespace,
    )


def list_users(
    namespace: str = "default", account_id: Optional[str] = None, boto3_session: Optional[boto3.Session] = None
) -> List[Dict[str, Any]]:
    """Return a list of all of the Amazon QuickSight users belonging to this account.

    Parameters
    ----------
    namespace : str
        The namespace. Currently, you should set this to default .
    account_id : str, optional
        If None, the account ID will be inferred from your boto3 session.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    List[Dict[str, Any]]
        Groups.

    Examples
    --------
    >>> import awswrangler as wr
    >>> users = wr.quicksight.list_users()
    """
    return _list(
        func_name="list_users",
        attr_name="UserList",
        account_id=account_id,
        boto3_session=boto3_session,
        Namespace=namespace,
    )
