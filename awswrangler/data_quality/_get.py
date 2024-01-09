"""AWS Glue Data Quality Get Module."""

from __future__ import annotations

from typing import cast

import boto3

import awswrangler.pandas as pd
from awswrangler.data_quality._utils import _get_ruleset, _rules_to_df


def get_ruleset(
    name: str | list[str],
    boto3_session: boto3.Session | None = None,
) -> pd.DataFrame:
    """Get a Data Quality ruleset.

    Parameters
    ----------
    name : str or list[str]
        Ruleset name or list of names.
    boto3_session : boto3.Session, optional
        Boto3 Session. If none, the default boto3 session is used.

    Returns
    -------
    pd.DataFrame
        Data frame with ruleset(s) details.

    Examples
    --------
    Get single ruleset
    >>> import awswrangler as wr

    >>> df_ruleset = wr.data_quality.get_ruleset(name="my_ruleset")

    Get multiple rulesets. A column with the ruleset name is added to the data frame
    >>> df_rulesets = wr.data_quality.get_ruleset(name=["ruleset_1", "ruleset_2"])
    """
    ruleset_names: list[str] = name if isinstance(name, list) else [name]
    dfs: list[pd.DataFrame] = []
    for ruleset_name in ruleset_names:
        rules = cast(str, _get_ruleset(ruleset_name=ruleset_name, boto3_session=boto3_session)["Ruleset"])
        df = _rules_to_df(rules=rules)
        if len(ruleset_names) > 1:
            df["ruleset"] = ruleset_name
        dfs.append(df)
    return pd.concat(dfs)
