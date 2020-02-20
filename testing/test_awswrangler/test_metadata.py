import awswrangler as wr


def test_metadata():
    assert wr.__version__ == "1.0.0"
    assert wr.__metadata__.__title__ == "awswrangler"
    assert wr.__metadata__.__description__ == "Pandas on AWS."
    assert wr.__metadata__.__license__ == "Apache License 2.0"
