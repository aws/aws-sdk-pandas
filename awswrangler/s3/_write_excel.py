"""Amazon S3 Excel Write Module (PRIVATE)."""

from __future__ import annotations

import logging
from typing import Any

import boto3
import pandas as pd

from awswrangler import exceptions
from awswrangler.s3._fs import open_s3_object

_logger: logging.Logger = logging.getLogger(__name__)


def to_excel(
    df: pd.DataFrame,
    path: str,
    boto3_session: boto3.Session | None = None,
    s3_additional_kwargs: dict[str, Any] | None = None,
    use_threads: bool | int = True,
    **pandas_kwargs: Any,
) -> str:
    """Write EXCEL file on Amazon S3.

    Note
    ----
    This function accepts any Pandas's read_excel() argument.
    https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_excel.html

    Note
    ----
    Depending on the file extension ('xlsx', 'xls', 'odf'...), an additional library
    might have to be installed first.

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
    pyarrow_additional_kwargs: dict[str, Any], optional
        Forwarded to botocore requests.
        e.g. s3_additional_kwargs={'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'}
    use_threads : bool, int
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
        If integer is provided, specified number is used.
    pandas_kwargs:
        KEYWORD arguments forwarded to pandas.DataFrame.to_excel(). You can NOT pass `pandas_kwargs` explicit, just add
        valid Pandas arguments in the function call and awswrangler will accept it.
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
            "Pandas arguments in the function call and awswrangler will accept it."
            "e.g. wr.s3.to_excel(df, path, na_rep="
            ", index=False)"
        )
    with open_s3_object(
        path=path,
        mode="wb",
        use_threads=use_threads,
        s3_additional_kwargs=s3_additional_kwargs,
        boto3_session=boto3_session,
    ) as f:
        _logger.debug("pandas_kwargs: %s", pandas_kwargs)
        df.to_excel(f, **pandas_kwargs)
    return path
