import awswrangler as wr


def test_metadata():
    assert wr.__version__ == "3.12.1"
    assert wr.__title__ == "awswrangler"
    assert wr.__description__ == "Pandas on AWS."
    assert wr.__license__ == "Apache License 2.0"
