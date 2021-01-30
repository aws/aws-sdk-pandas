"""Amazon S3 Excel Write Module (PRIVATE)."""

import logging
from typing import Any, Dict, Optional

import boto3
import pandas as pd

from awswrangler import _utils, exceptions
from awswrangler.s3._fs import open_s3_object

_logger: logging.Logger = logging.getLogger(__name__)


def to_excel(
    df: pd.DataFrame,
    path: str,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, Any]] = None,
    use_threads: bool = True,
    **pandas_kwargs: Any,
) -> str:
    """Write EXCEL file on Amazon S3.

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
    df: pandas.DataFrame
        Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    path : str
        Amazon S3 path (e.g. s3://bucket/filename.xlsx).
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.
    s3_additional_kwargs : Optional[Dict[str, Any]]
        Forward to botocore requests. Valid parameters: "ACL", "Metadata", "ServerSideEncryption", "StorageClass",
        "SSECustomerAlgorithm", "SSECustomerKey", "SSEKMSKeyId", "SSEKMSEncryptionContext", "Tagging", "RequestPayer".
        e.g. s3_additional_kwargs={'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'}
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
    pandas_kwargs:
        KEYWORD arguments forwarded to pandas.DataFrame.to_excel(). You can NOT pass `pandas_kwargs` explicit, just add
        valid Pandas arguments in the function call and Wrangler will accept it.
        e.g. wr.s3.to_excel(df, path, na_rep="", index=False)
        https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_excel.html

    Returns
    -------
    str
        Written S3 path.

    Examples
    --------
    Writing EXCEL file

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_excel(df, 's3://bucket/filename.xlsx')

    """
    if "pandas_kwargs" in pandas_kwargs:
        raise exceptions.InvalidArgument(
            "You can NOT pass `pandas_kwargs` explicit, just add valid "
            "Pandas arguments in the function call and Wrangler will accept it."
            "e.g. wr.s3.to_excel(df, path, na_rep="
            ", index=False)"
        )
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    with open_s3_object(
        path=path,
        mode="wb",
        use_threads=use_threads,
        s3_additional_kwargs=s3_additional_kwargs,
        boto3_session=session,
    ) as f:
        pandas_kwargs["engine"] = "openpyxl"
        _logger.debug("pandas_kwargs: %s", pandas_kwargs)
        df.to_excel(f, **pandas_kwargs)
    return path
