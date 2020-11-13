"""
Amazon QuickSight List and Get Module.

List and Get MUST be together to avoid circular dependency.
"""

import logging
from typing import Any, Callable, Dict, List, Optional

import boto3

from awswrangler import _utils, exceptions, sts

_logger: logging.Logger = logging.getLogger(__name__)


def _list(
    func_name: str,
    attr_name: str,
    account_id: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
    **kwargs: Any,
) -> List[Dict[str, Any]]:
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    if account_id is None:
        account_id = sts.get_account_id(boto3_session=session)
    client: boto3.client = _utils.client(service_name="quicksight", session=session)
    func: Callable[..., Dict[str, Any]] = getattr(client, func_name)
    response: Dict[str, Any] = func(AwsAccountId=account_id, **kwargs)
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
        The namespace. Currently, you should set this to default.
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


def list_ingestions(
    dataset_name: Optional[str] = None,
    dataset_id: Optional[str] = None,
    account_id: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> List[Dict[str, Any]]:
    """List the history of SPICE ingestions for a dataset.

    Parameters
    ----------
    dataset_name : str, optional
        Dataset name.
    dataset_id : str, optional
        The ID of the dataset used in the ingestion.
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
    >>> ingestions = wr.quicksight.list_ingestions()
    """
    if (dataset_name is None) and (dataset_id is None):
        raise exceptions.InvalidArgument("You must pass a not None name or dataset_id argument.")
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    if account_id is None:
        account_id = sts.get_account_id(boto3_session=session)
    if (dataset_id is None) and (dataset_name is not None):
        dataset_id = get_dataset_id(name=dataset_name, account_id=account_id, boto3_session=session)
    return _list(
        func_name="list_ingestions",
        attr_name="Ingestions",
        account_id=account_id,
        boto3_session=boto3_session,
        DataSetId=dataset_id,
    )


def _get_ids(
    name: str,
    func: Callable[..., List[Dict[str, Any]]],
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
    func: Callable[..., List[Dict[str, Any]]],
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
        name=name, func=list_dashboards, attr_name="DashboardId", account_id=account_id, boto3_session=boto3_session
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
        name=name, func=list_dashboards, attr_name="DashboardId", account_id=account_id, boto3_session=boto3_session
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
        name=name, func=list_datasets, attr_name="DataSetId", account_id=account_id, boto3_session=boto3_session
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
        name=name, func=list_datasets, attr_name="DataSetId", account_id=account_id, boto3_session=boto3_session
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
        name=name, func=list_data_sources, attr_name="DataSourceId", account_id=account_id, boto3_session=boto3_session
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
        name=name, func=list_data_sources, attr_name="DataSourceId", account_id=account_id, boto3_session=boto3_session
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
        name=name, func=list_templates, attr_name="TemplateId", account_id=account_id, boto3_session=boto3_session
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
        name=name, func=list_templates, attr_name="TemplateId", account_id=account_id, boto3_session=boto3_session
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
    for source in list_data_sources(account_id=account_id, boto3_session=boto3_session):
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
