# tests/unit/test_emr_bootstraps.py

from __future__ import annotations

import pytest
from unittest import mock

import awswrangler as wr


class _FakeEmrClient:
    def __init__(self):
        self.run_job_flow_args = None

    def run_job_flow(self, **kwargs):
        self.run_job_flow_args = kwargs
        return {"JobFlowId": "j-TEST"}


def _create_cluster_with_bootstraps(bootstraps_paths):
    fake_emr_client = _FakeEmrClient()
    with mock.patch("awswrangler.emr._utils.client", return_value=fake_emr_client):
        with mock.patch("awswrangler.emr.sts.get_account_id", return_value="123456789012"):
            with mock.patch("awswrangler.emr._utils.get_region_from_session", return_value="us-east-1"):
                wr.emr.create_cluster(
                    subnet_id="subnet-12345678",
                    bootstraps_paths=bootstraps_paths,
                )
    return fake_emr_client.run_job_flow_args


def test_create_cluster_bootstraps_legacy_paths() -> None:
    args = _create_cluster_with_bootstraps(["s3://bucket/bootstrap.sh"])

    assert args is not None
    assert args["BootstrapActions"] == [
        {"Name": "s3://bucket/bootstrap.sh", "ScriptBootstrapAction": {"Path": "s3://bucket/bootstrap.sh"}}
    ]


def test_create_cluster_bootstraps_script_bootstrap_action_dict() -> None:
    args = _create_cluster_with_bootstraps(
        [
            {
                "Name": "install-dependencies",
                "ScriptBootstrapAction": {
                    "Path": "s3://bucket/bootstrap.sh",
                    "Args": ["--package", "numpy"],
                },
            }
        ]
    )

    assert args is not None
    assert args["BootstrapActions"] == [
        {
            "Name": "install-dependencies",
            "ScriptBootstrapAction": {
                "Path": "s3://bucket/bootstrap.sh",
                "Args": ["--package", "numpy"],
            },
        }
    ]


def test_create_cluster_bootstraps_path_args_dict() -> None:
    args = _create_cluster_with_bootstraps(
        [
            {
                "Path": "s3://bucket/bootstrap.sh",
                "Args": ["--arg", "value"],
            }
        ]
    )

    assert args is not None
    assert args["BootstrapActions"] == [
        {
            "Name": "s3://bucket/bootstrap.sh",
            "ScriptBootstrapAction": {
                "Path": "s3://bucket/bootstrap.sh",
                "Args": ["--arg", "value"],
            },
        }
    ]


def test_create_cluster_bootstraps_invalid_dict_raises() -> None:
    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        _create_cluster_with_bootstraps([{"Name": "broken"}])
