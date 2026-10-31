from __future__ import annotations

from unittest import mock

import boto3

import awswrangler as wr


def _session_client(session: mock.Mock) -> mock.Mock:
    client = mock.Mock()
    session.client.return_value = client
    return client


def test_emr_serverless_run_job_waits_with_the_given_session() -> None:
    session = mock.Mock(spec=boto3.Session)
    client = _session_client(session)
    client.start_job_run.return_value = {"jobRunId": "job-1"}
    client.get_job_run.return_value = {"jobRun": {"state": "SUCCESS"}}

    with mock.patch("awswrangler._utils.ensure_session", side_effect=lambda session=None: session) as ensure:
        wr.emr_serverless.run_job(
            application_id="app",
            execution_role_arn="arn:aws:iam::123456789012:role/test",
            job_driver_args={},
            boto3_session=session,
        )

    assert all(call.kwargs.get("session") is session for call in ensure.call_args_list)
    client.get_job_run.assert_called_once()


def test_catalog_get_tables_lists_databases_with_the_given_session() -> None:
    session = mock.Mock(spec=boto3.Session)
    client = _session_client(session)
    paginator = client.get_paginator.return_value
    paginator.paginate.side_effect = [
        [{"DatabaseList": [{"Name": "db"}]}],
        [{"TableList": [{"Name": "table"}]}],
    ]

    with mock.patch("awswrangler._utils.ensure_session", side_effect=lambda session=None: session):
        tables = list(wr.catalog.get_tables(boto3_session=session))

    assert tables == [{"Name": "table"}]
