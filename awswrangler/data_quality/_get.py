"""AWS Glue Data Quality Get Module."""

from typing import Optional, cast

import boto3
import pandas as pd

from awswrangler.data_quality._utils import _get_ruleset, _rules_to_df


def get_ruleset(
    name: str,
    boto3_session: Optional[boto3.Session] = None,
) -> pd.DataFrame:
    """Get a Data Quality ruleset.

    Parameters
    ----------
    name : str
        Ruleset name.
    boto3_session : boto3.Session, optional
        Boto3 Session. If none, the default boto3 session is used.

    Returns
    -------
    pd.DataFrame
        Data frame with ruleset details.

    Examples
    --------
    >>> import awswrangler as wr

    >>> df_ruleset = wr.data_quality.get_ruleset(name="my_ruleset")
    """
    rules = cast(str, _get_ruleset(ruleset_name=name, boto3_session=boto3_session)["Ruleset"])
    return _rules_to_df(rules=rules)
