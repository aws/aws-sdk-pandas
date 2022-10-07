"""Ray utilities module."""

from typing import List

from awswrangler._utils import chunkify


def _batch_paths_distributed(paths: List[str], parallelism: int) -> List[List[str]]:
    if len(paths) > parallelism:
        return chunkify(paths, parallelism)
    return [[path] for path in paths]
