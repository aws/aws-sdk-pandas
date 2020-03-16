import logging

import awswrangler as wr

logging.basicConfig(level=logging.INFO, format="[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)


def test_normalize_column_name():
    assert wr.athena.normalize_column_name("foo()__Boo))))____BAR") == "foo_boo_bar"
    assert wr.athena.normalize_column_name("foo()__Boo))))_{}{}{{}{}{}{___BAR[][][][]") == "foo_boo_bar"
