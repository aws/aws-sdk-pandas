"""Unit tests for DQDL ruleset parsing (no AWS resources required)."""

from awswrangler.data_quality._utils import _rules_to_df, _split_dqdl_rules


def test_rules_to_df_plain() -> None:
    rules = 'Rules = [RowCount > 0, IsComplete "id", Uniqueness "id" > 0.95]'
    df = _rules_to_df(rules)
    assert df["rule_type"].tolist() == ["RowCount", "IsComplete", "Uniqueness"]
    assert df["parameter"].tolist()[1] == '"id"'


def test_rules_to_df_with_comments_and_newlines() -> None:
    """Glue DQ recommendations now emit multi-line rulesets with # comment lines."""
    rules = """Rules = [
        # Primary identifiers
        IsComplete "order_id",
        Uniqueness "order_id" > 0.95,  # inline comment
        # Value checks
        ColumnValues "kind" in ["a", "b,c"],
        RowCount > 0
    ]"""
    df = _rules_to_df(rules)
    assert len(df) == 4
    assert df["rule_type"].tolist() == ["IsComplete", "Uniqueness", "ColumnValues", "RowCount"]


def test_split_dqdl_handles_commas_inside_brackets_and_quotes() -> None:
    rules = _split_dqdl_rules('ColumnValues "k" in ["x,y", "z"], IsComplete "a, b", RowCount > 0')
    assert rules == ['ColumnValues "k" in ["x,y", "z"]', 'IsComplete "a, b"', "RowCount > 0"]


def test_split_dqdl_handles_composite_rules() -> None:
    rules = _split_dqdl_rules('(RowCount > 0) and (IsComplete "c0"), Uniqueness "c1" > 0.5')
    assert rules == ['(RowCount > 0) and (IsComplete "c0")', 'Uniqueness "c1" > 0.5']


def test_split_dqdl_hash_inside_string_is_not_a_comment() -> None:
    rules = _split_dqdl_rules('ColumnValues "code" in ["#1", "#2"], RowCount > 0')
    assert rules == ['ColumnValues "code" in ["#1", "#2"]', "RowCount > 0"]


def test_split_dqdl_trailing_comment_without_newline() -> None:
    assert _split_dqdl_rules("RowCount > 0 # trailing comment") == ["RowCount > 0"]


def test_split_dqdl_unbalanced_bracket_fails_safe() -> None:
    # A stray closing bracket must not disable the top-level comma split.
    assert _split_dqdl_rules('IsComplete "a"], RowCount > 0') == ['IsComplete "a"]', "RowCount > 0"]
