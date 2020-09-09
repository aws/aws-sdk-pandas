"""Amazon QuickSight Cancel Module."""

import logging
from typing import Optional

import boto3

from awswrangler import _utils, exceptions, sts
from awswrangler.quicksight._get_list import get_dataset_id

_logger: logging.Logger = logging.getLogger(__name__)


def cancel_ingestion(
    ingestion_id: str,
    dataset_name: Optional[str] = None,
    dataset_id: Optional[str] = None,
    account_id: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> None:
    """Cancel an ongoing ingestion of data into SPICE.

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
    None
        None.

    Examples
    --------
    >>> import awswrangler as wr
    >>>  wr.quicksight.cancel_ingestion(ingestion_id="...", dataset_name="...")
    """
    if (dataset_name is None) and (dataset_id is None):
        raise exceptions.InvalidArgument("You must pass a not None name or dataset_id argument.")
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    if account_id is None:
        account_id = sts.get_account_id(boto3_session=session)
    if (dataset_id is None) and (dataset_name is not None):
        dataset_id = get_dataset_id(name=dataset_name, account_id=account_id, boto3_session=session)
    client: boto3.client = _utils.client(service_name="quicksight", session=session)
    client.cancel_ingestion(IngestionId=ingestion_id, AwsAccountId=account_id, DataSetId=dataset_id)
