"""AWS Glue Data Quality package."""

from awswrangler.data_quality._create import create_ruleset, evaluate_ruleset

__all__ = [
    "create_ruleset",
    "evaluate_ruleset",
]
