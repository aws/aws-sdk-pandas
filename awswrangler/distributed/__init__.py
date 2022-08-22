"""Distributed Module."""

from awswrangler.distributed._distributed import initialize_ray, ray_get, ray_remote  # noqa

__all__ = [
    "initialize_ray",
    "ray_get",
    "ray_remote",
]
