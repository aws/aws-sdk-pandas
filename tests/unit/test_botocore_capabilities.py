"""Offline check that the installed botocore knows the AWS services and operations awswrangler uses.

Package versions only identify an SDK release. The service models bundled with botocore describe what that
release can actually do, and they can be inspected without credentials or network access. Running this module
against the oldest boto3/botocore that ``pyproject.toml`` allows shows which features that SDK cannot serve.
"""

from __future__ import annotations

import re
from pathlib import Path

import botocore
import pytest
from botocore.exceptions import UnknownServiceError
from botocore.loaders import Loader
from botocore.model import ServiceModel

import awswrangler

_PACKAGE_DIR = Path(awswrangler.__file__).parent
_SERVICE_NAME = re.compile(r"""service_name\s*=\s*["']([a-z0-9-]+)["']""")


def _services_used_by_awswrangler() -> list[str]:
    names: set[str] = set()
    for path in _PACKAGE_DIR.rglob("*.py"):
        names.update(_SERVICE_NAME.findall(path.read_text(encoding="utf-8")))
    return sorted(names)


def _service_model(service_name: str) -> ServiceModel:
    try:
        return ServiceModel(Loader().load_service_model(service_name, "service-2"), service_name=service_name)
    except UnknownServiceError:
        # Without pytrace the output would repeat botocore's list of every service it does know.
        pytest.fail(f"botocore {botocore.__version__} has no service model for '{service_name}'", pytrace=False)


def test_services_are_discovered():
    # Guards the discovery itself: an empty list would make the test below vacuous.
    assert {"s3", "athena", "glue"} <= set(_services_used_by_awswrangler())


@pytest.mark.parametrize("service_name", _services_used_by_awswrangler())
def test_botocore_knows_service(service_name):
    _service_model(service_name)


# Operations that awswrangler calls and that were added to an existing or new service after the declared minimum.
@pytest.mark.parametrize(
    "service_name,operation_name",
    [
        ("s3", "GetObject"),
        ("s3vectors", "CreateVectorBucket"),
        ("bedrock", "CreateModelInvocationJob"),
    ],
)
def test_botocore_knows_operation(service_name, operation_name):
    model = _service_model(service_name)
    assert operation_name in model.operation_names, (
        f"botocore {botocore.__version__} does not know {service_name}.{operation_name}"
    )
