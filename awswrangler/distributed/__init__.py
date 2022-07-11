"""Distributed Module."""

from awswrangler.distributed._distributed import initialize_ray, ray_remote  # noqa

__all__ = [
    "ray_remote",
    "initialize_ray",
]
