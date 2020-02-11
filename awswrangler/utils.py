"""Utilities Module."""

from logging import Logger, getLogger
from math import ceil, gcd
from time import sleep

from awswrangler.exceptions import InvalidArguments

logger: Logger = getLogger(__name__)


def calculate_bounders(num_items, num_groups=None, max_size=None):
    """
    Calculate bounders to split a list.

    Use num_groups or max_size.

    :param num_items: Total number os items to be splitted
    :param num_groups: Number of chunks
    :param max_size: Max size per chunk
    :return: List of tuples with the indexes to split
    """
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


def wait_process_release(processes, target_number=None):
    """
    Wait one of the processes releases.

    :param processes: List of processes
    :param target_number: Wait for a target number of running processes
    :return: None
    """
    n = len(processes)
    i = 0
    while True:
        if target_number is None:
            if processes[i].is_alive() is False:
                del processes[i]
                return None
            i += 1
            if i == n:
                i = 0
        else:
            count = 0
            for p in processes:
                if p.is_alive():
                    count += 1
            if count <= target_number:
                return count
        sleep(0.25)


def lcm(a: int, b: int) -> int:
    """Least Common Multiple."""
    return int(abs(a * b) // gcd(a, b))
