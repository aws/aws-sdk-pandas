"""Ray utilities (PRIVATE)."""

from __future__ import annotations

import ray
from ray.util.placement_group import PlacementGroup


# https://github.com/ray-project/ray/blob/master/python/ray/data/_internal/util.py#L87
def _estimate_avail_cpus(cur_pg: PlacementGroup | None) -> int:
    """
    Estimates the available CPU parallelism for this Dataset in the cluster.

    If we aren't in a placement group, this is trivially the number of CPUs in the
    cluster. Otherwise, we try to calculate how large the placement group is relative
    to the size of the cluster.

    Args:
        cur_pg: The current placement group, if any.
    """
    cluster_cpus = int(ray.cluster_resources().get("CPU", 1))
    cluster_gpus = int(ray.cluster_resources().get("GPU", 0))

    # If we're in a placement group, we shouldn't assume the entire cluster's
    # resources are available for us to use. Estimate an upper bound on what's
    # reasonable to assume is available for datasets to use.
    if cur_pg:
        pg_cpus = 0
        for bundle in cur_pg.bundle_specs:
            # Calculate the proportion of the cluster this placement group "takes up".
            # Then scale our cluster_cpus proportionally to avoid over-parallelizing
            # if there are many parallel Tune trials using the cluster.
            cpu_fraction = bundle.get("CPU", 0) / max(1, cluster_cpus)
            gpu_fraction = bundle.get("GPU", 0) / max(1, cluster_gpus)
            max_fraction = max(cpu_fraction, gpu_fraction)
            # Over-parallelize by up to a factor of 2, but no more than that. It's
            # preferable to over-estimate than under-estimate.
            pg_cpus += 2 * int(max_fraction * cluster_cpus)

        return min(cluster_cpus, pg_cpus)

    return cluster_cpus


def _estimate_available_parallelism() -> int:
    """
    Estimates the available CPU parallelism for this Dataset in the cluster.

    If we are currently in a placement group, take that into account.
    """
    cur_pg = ray.util.get_current_placement_group()
    return _estimate_avail_cpus(cur_pg)


def ensure_worker_count(use_threads: bool | int = True) -> int:
    if type(use_threads) == int:  # noqa: E721
        if use_threads < 1:
            return 1
        return use_threads

    if use_threads is False:
        return 1

    parallelism = _estimate_available_parallelism()
    return max(parallelism, 1)
