"""Amazon S3 Excel Read Module (PRIVATE)."""

import logging
from typing import Any, Dict, Optional

import boto3
import pandas as pd

from awswrangler import _utils, exceptions
from awswrangler.s3._fs import open_s3_object

_logger: logging.Logger = logging.getLogger(__name__)


def read_excel(
    path: str,
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, Any]] = None,
    **pandas_kwargs: Any,
) -> pd.DataFrame:
    """Read EXCEL file(s) from from a received S3 path.

    Note
    ----
    This function accepts any Pandas's read_excel() argument.
    https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_excel.html

    Note
    ----
    In case of `use_threads=True` the number of threads
    that will be spawned will be gotten from os.cpu_count().

    Parameters
    ----------
    path : Union[str, List[str]]
        S3 path (e.g. ``s3://bucket/key.xlsx``).
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    s3_additional_kwargs : Optional[Dict[str, Any]]
        Forward to botocore requests, only "SSECustomerAlgorithm" and "SSECustomerKey" arguments will be considered.
    pandas_kwargs:
        KEYWORD arguments forwarded to pandas.read_excel(). You can NOT pass `pandas_kwargs` explicit, just add valid
        Pandas arguments in the function call and Wrangler will accept it.
        e.g. wr.s3.read_excel("s3://bucket/key.xlsx", na_rep="", verbose=True)
        https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_excel.html

    Returns
    -------
    pandas.DataFrame
        Pandas DataFrame.

    Examples
    --------
    Reading an EXCEL file

    >>> import awswrangler as wr
    >>> df = wr.s3.read_excel('s3://bucket/key.xlsx')

    """
    if "pandas_kwargs" in pandas_kwargs:
        raise exceptions.InvalidArgument(
            "You can NOT pass `pandas_kwargs` explicit, just add valid "
            "Pandas arguments in the function call and Wrangler will accept it."
            "e.g. wr.s3.read_excel('s3://bucket/key.xlsx', na_rep='', verbose=True)"
        )
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    with open_s3_object(
        path=path,
        mode="rb",
        use_threads=use_threads,
        s3_block_size=-1,  # One shot download
        s3_additional_kwargs=s3_additional_kwargs,
        boto3_session=session,
    ) as f:
        pandas_kwargs["engine"] = "openpyxl"
        _logger.debug("pandas_kwargs: %s", pandas_kwargs)
        return pd.read_excel(f, **pandas_kwargs)
