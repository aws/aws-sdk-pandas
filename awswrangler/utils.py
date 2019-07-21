from math import ceil
from math import gcd
import logging

from awswrangler.exceptions import InvalidArguments

logger = logging.getLogger(__name__)


def calculate_bounders(num_items, num_groups=None, max_size=None):
    if num_groups or max_size:
        if max_size:
            num_groups = int(ceil(float(num_items) / float(max_size)))
        else:
            num_groups = num_items if num_items < num_groups else num_groups
        size = int(num_items / num_groups)
        rest = num_items % num_groups
        bounders = []
        end = 0
        for _ in range(num_groups):
            start = end
            end += size
            if rest:
                end += 1
                rest -= 1
            bounders.append((start, end))
        return bounders
    else:
        raise InvalidArguments("You must give num_groups or max_size!")


def lcm(a, b):
    """
    Least Common Multiple
    """
    return abs(a * b) // gcd(a, b)
