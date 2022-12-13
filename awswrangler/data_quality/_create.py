"""Data Quality module."""

import logging
import uuid
from typing import Optional

import boto3
import pandas as pd

from awswrangler import _utils, exceptions

_logger: logging.Logger = logging.getLogger(__name__)


def _create_dqdl(
    df_rules: pd.DataFrame,
):
    """Create DQDL from pandas data frame."""
    rules = []
    for rule_type, parameter, expression in df_rules.itertuples(index=False):
        parameter_str = f' "{parameter}" ' if parameter else " "
        expression_str = expression if expression else ""
        rules.append(f"{rule_type}{parameter_str}{expression_str}")

    rules_str = "Rules = [ " + ", ".join(rules) + " ]"
    return rules_str


def create_ruleset(
    name: str,
    database: str,
    table: str,
    df_rules: Optional[pd.DataFrame] = None,
    dqdl_rules: Optional[str] = None,
    description: str = "",
    client_token: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> None:
    """Create Data Quality ruleset.

    Parameters
    ----------
    name : str
        Ruleset name.
    database : str
        Glue database name.
    table : str
        Glue table name.
    df_rules : str, optional
        Data frame with `rule_type`, `parameter`, and `expression` columns.
    dqdl_rules : str, optional
        Data Quality Definition Language definition.
    description : str
        Ruleset description.
    client_token : str, optional
        Random id used for idempotency. Will be automatically generated if not provided.
    boto3_session : boto3.Session, optional
        Ruleset description.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.data_quality.create_ruleset(
    >>>     name="ruleset",
    >>>     database="db",
    >>>     table="table",
    >>>     dqdl_rules=dqdl_rules,
    >>>)
    """
    if df_rules is not None and dqdl_rules:
        raise exceptions.InvalidArgumentCombination("You must pass either ruleset `df_rules` or `dqdl_rules`.")

    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
    dqdl_rules = _create_dqdl(df_rules) if df_rules is not None else dqdl_rules

    try:
        client_glue.create_data_quality_ruleset(
            Name=name,
            Description=description,
            Ruleset=dqdl_rules,
            TargetTable={
                "TableName": table,
                "DatabaseName": database,
            },
            ClientToken=client_token if client_token else uuid.uuid4().hex,
        )
    except client_glue.exceptions.AlreadyExistsException:
        raise exceptions.AlreadyExists(f"Ruleset {name} already exists.")
