import logging

import pytest

from awswrangler._utils import get_even_chunks_sizes

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


@pytest.mark.parametrize(
    "total_size,chunk_size,upper_bound,result",
    [
        (10, 4, True, (4, 3, 3)),
        (2, 3, True, (2,)),
        (1, 1, True, (1,)),
        (2, 1, True, (1, 1)),
        (11, 4, True, (4, 4, 3)),
        (1_001, 500, True, (334, 334, 333)),
        (1_002, 500, True, (334, 334, 334)),
        (10, 4, False, (5, 5)),
        (1, 1, False, (1,)),
        (2, 1, False, (1, 1)),
        (11, 4, False, (6, 5)),
        (1_001, 500, False, (501, 500)),
        (1_002, 500, False, (501, 501)),
    ],
)
def test_get_even_chunks_sizes(total_size, chunk_size, upper_bound, result):
    assert get_even_chunks_sizes(total_size, chunk_size, upper_bound) == result
