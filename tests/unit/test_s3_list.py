"""Unit tests for awswrangler.s3._list module."""

from awswrangler.s3._list import _path2list


class TestPath2ListSuffixIgnoreSuffix:
    """Test suffix + ignore_suffix interaction in _path2list (list input path)."""

    def test_suffix_only(self):
        paths = ["a.parquet", "b.csv", "c.json"]
        result = _path2list(paths, None, None, suffix=".parquet")
        assert result == ["a.parquet"]

    def test_ignore_suffix_only(self):
        paths = [
            "s3://bucket/a.parquet",
            "s3://bucket/b.parquet",
            "s3://bucket/c.bak",
        ]
        result = _path2list(paths, None, None, ignore_suffix=".bak")
        assert result == [
            "s3://bucket/a.parquet",
            "s3://bucket/b.parquet",
        ]

    def test_both_suffix_and_ignore_suffix(self):
        # This is the regression test: the old code would overwrite
        # suffix-filtered results with the original `path` list when
        # ignore_suffix was also set.
        paths = [
            "s3://bucket/a.parquet",
            "s3://bucket/b.parquet",
            "s3://bucket/c.parquet.bak",
            "s3://bucket/d.csv",
        ]
        result = _path2list(paths, None, None, suffix=".parquet", ignore_suffix=".bak")
        # Should: suffix filters to parquet files, then ignore removes .bak ones
        expected = ["s3://bucket/a.parquet", "s3://bucket/b.parquet"]
        assert result == expected

    def test_both_suffix_and_ignore_suffix_as_lists(self):
        paths = [
            "s3://bucket/a.parquet",
            "s3://bucket/b.parquet",
            "s3://bucket/c.bak",
        ]
        result = _path2list(paths, None, None, suffix=[".parquet"], ignore_suffix=[".bak"])
        assert result == ["s3://bucket/a.parquet", "s3://bucket/b.parquet"]

    def test_no_filters_returns_original(self):
        paths = ["s3://bucket/a.parquet", "s3://bucket/b.csv"]
        result = _path2list(paths, None, None)
        assert result == ["s3://bucket/a.parquet", "s3://bucket/b.csv"]

    def test_suffix_no_matches_returns_empty(self):
        paths = ["s3://bucket/a.csv", "s3://bucket/b.json"]
        result = _path2list(paths, None, None, suffix=".parquet")
        assert result == []

    def test_suffix_only_list_path(self):
        # When path is a list and suffix matches some items
        paths = ["s3://bucket/a.parquet", "s3://bucket/b.csv"]
        result = _path2list(paths, None, None, suffix=".parquet")
        assert result == ["s3://bucket/a.parquet"]

    def test_suffix_and_ignore_empty_result(self):
        # When suffix filters everything away, ignore should not re-add them
        paths = ["s3://bucket/a.csv", "s3://bucket/b.json"]
        result = _path2list(paths, None, None, suffix=".parquet", ignore_suffix=".bak")
        assert result == []
