"""Unit tests for awswrangler.bedrock entry points.

Bedrock is not currently supported by moto, so these tests mock the boto3 clients returned by
``awswrangler._utils.client`` and assert on call arguments and result shaping.
"""

from __future__ import annotations

import io
import json
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import awswrangler as wr
from awswrangler import exceptions
from awswrangler.bedrock import _invoke


def _patch_client(mock_client: MagicMock) -> Any:
    """Patch awswrangler._utils.client so any service request returns mock_client."""
    return patch("awswrangler._utils.client", return_value=mock_client)


def _patch_clients(mapping: dict[str, MagicMock]) -> Any:
    """Patch awswrangler._utils.client to dispatch by service name."""

    def fake_client(service_name: str, **_: Any) -> MagicMock:
        if service_name not in mapping:
            raise AssertionError(f"Unexpected service requested: {service_name}")
        return mapping[service_name]

    return patch("awswrangler._utils.client", side_effect=fake_client)


def _bedrock_response(payload: dict[str, Any]) -> dict[str, Any]:
    return {"body": io.BytesIO(json.dumps(payload).encode())}


# ---------------------------------------------------------------------------
# invoke_batch
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _lower_min_records(monkeypatch: pytest.MonkeyPatch) -> None:
    """Bedrock requires >=100 records per job. Drop the floor so test fixtures stay small."""
    monkeypatch.setattr(_invoke, "_MIN_RECORDS", 1)


def _build_batch_clients(
    output_records: list[dict[str, Any]],
    statuses: list[str] | None = None,
) -> tuple[MagicMock, MagicMock, list[dict[str, Any]]]:
    """Wire up bedrock + s3 mocks and capture the JSONL payload uploaded to S3."""
    bedrock = MagicMock()
    s3 = MagicMock()

    captured_input: list[dict[str, Any]] = []

    def fake_put_object(**kwargs: Any) -> dict[str, Any]:
        body = kwargs["Body"]
        for line in body.splitlines():
            if line.strip():
                captured_input.append(json.loads(line))
        return {}

    s3.put_object.side_effect = fake_put_object

    bedrock.create_model_invocation_job.return_value = {
        "jobArn": "arn:aws:bedrock:us-east-1:123:model-invocation-job/abc123def456",
    }

    statuses = statuses or ["Completed"]
    bedrock.get_model_invocation_job.side_effect = [{"status": s} for s in statuses]

    s3.list_objects_v2.return_value = {
        "Contents": [{"Key": "out/abc123def456/awswrangler-job.jsonl.out"}],
    }
    s3.get_object.return_value = {
        "Body": io.BytesIO(b"\n".join(json.dumps(r).encode() for r in output_records)),
    }
    return bedrock, s3, captured_input


def test_invoke_batch_converse_happy_path() -> None:
    output_records = [
        {
            "recordId": "REC000000000",
            "modelInput": {"messages": []},
            "modelOutput": {
                "output": {"message": {"role": "assistant", "content": [{"text": "hi"}]}},
                "stopReason": "end_turn",
            },
        },
        {
            "recordId": "REC000000001",
            "modelInput": {"messages": []},
            "modelOutput": {
                "output": {"message": {"role": "assistant", "content": [{"text": "bye"}]}},
                "stopReason": "end_turn",
            },
        },
    ]
    bedrock, s3, captured = _build_batch_clients(output_records)

    df = pd.DataFrame({"prompt": ["say hi", "say bye"], "label": ["greet", "farewell"]})
    with _patch_clients({"bedrock": bedrock, "s3": s3}):
        out = wr.bedrock.invoke_batch(
            df=df,
            model_id="anthropic.claude-3-haiku-20240307-v1:0",
            role_arn="arn:aws:iam::123:role/BatchRole",
            prompt_column="prompt",
            input_s3_uri="s3://my-bucket/in/",
            output_s3_uri="s3://my-bucket/out/",
            inference_config={"maxTokens": 256},
            wait=True,
            poll_interval_seconds=0,
        )

    # Input JSONL was built correctly.
    assert [r["recordId"] for r in captured] == ["REC000000000", "REC000000001"]
    assert captured[0]["modelInput"]["messages"][0]["content"][0]["text"] == "say hi"
    assert captured[0]["modelInput"]["inferenceConfig"] == {"maxTokens": 256}

    # CreateModelInvocationJob was called with the right shape.
    create = bedrock.create_model_invocation_job.call_args.kwargs
    assert create["modelInvocationType"] == "Converse"
    assert create["modelId"] == "anthropic.claude-3-haiku-20240307-v1:0"
    assert create["roleArn"] == "arn:aws:iam::123:role/BatchRole"
    assert create["inputDataConfig"]["s3InputDataConfig"]["s3Uri"].startswith("s3://my-bucket/in/")
    assert create["outputDataConfig"]["s3OutputDataConfig"]["s3Uri"] == "s3://my-bucket/out/"

    # Output joined back to the original frame.
    assert list(out.columns) == ["prompt", "label", "record_id", "model_output", "error", "output_text"]
    assert out["output_text"].tolist() == ["hi", "bye"]
    assert out["label"].tolist() == ["greet", "farewell"]
    assert out.attrs["job_arn"].endswith("abc123def456")
    assert out.attrs["status"] == "Completed"


def test_invoke_batch_uses_record_id_column() -> None:
    output_records = [
        {
            "recordId": "alpha",
            "modelOutput": {"output": {"message": {"content": [{"text": "A"}]}}},
        },
        {
            "recordId": "beta",
            "modelOutput": {"output": {"message": {"content": [{"text": "B"}]}}},
        },
    ]
    bedrock, s3, captured = _build_batch_clients(output_records)
    df = pd.DataFrame({"id": ["alpha", "beta"], "prompt": ["x", "y"]})

    with _patch_clients({"bedrock": bedrock, "s3": s3}):
        out = wr.bedrock.invoke_batch(
            df=df,
            model_id="anthropic.claude-3-haiku-20240307-v1:0",
            role_arn="arn:aws:iam::123:role/R",
            prompt_column="prompt",
            record_id_column="id",
            input_s3_uri="s3://b/in/",
            output_s3_uri="s3://b/out/",
            wait=True,
            poll_interval_seconds=0,
        )

    assert [r["recordId"] for r in captured] == ["alpha", "beta"]
    # Order may not match when joining; sort by id for stability.
    out_sorted = out.sort_values("id").reset_index(drop=True)
    assert out_sorted["output_text"].tolist() == ["A", "B"]


def test_invoke_batch_invoke_model_requires_model_input_column() -> None:
    df = pd.DataFrame({"prompt": ["x"]})
    with pytest.raises(exceptions.InvalidArgumentCombination):
        wr.bedrock.invoke_batch(
            df=df,
            model_id="amazon.titan-text-express-v1",
            role_arn="arn:aws:iam::123:role/R",
            prompt_column="prompt",
            invocation_type="InvokeModel",
            input_s3_uri="s3://b/in/",
            output_s3_uri="s3://b/out/",
        )


def test_invoke_batch_invoke_model_with_model_input_column() -> None:
    output_records = [
        {"recordId": "REC000000000", "modelOutput": {"results": [{"outputText": "ok"}]}},
    ]
    bedrock, s3, captured = _build_batch_clients(output_records)
    df = pd.DataFrame({"body": [{"inputText": "hello"}]})

    with _patch_clients({"bedrock": bedrock, "s3": s3}):
        out = wr.bedrock.invoke_batch(
            df=df,
            model_id="amazon.titan-text-express-v1",
            role_arn="arn:aws:iam::123:role/R",
            model_input_column="body",
            invocation_type="InvokeModel",
            input_s3_uri="s3://b/in/",
            output_s3_uri="s3://b/out/",
            wait=True,
            poll_interval_seconds=0,
        )

    assert captured[0]["modelInput"] == {"inputText": "hello"}
    # InvokeModel jobs do not produce an output_text column (Converse-only convenience).
    assert "output_text" not in out.columns
    assert out["model_output"].iloc[0] == {"results": [{"outputText": "ok"}]}


def test_invoke_batch_either_prompt_or_model_input_required() -> None:
    df = pd.DataFrame({"x": [1]})
    with pytest.raises(exceptions.InvalidArgumentCombination):
        wr.bedrock.invoke_batch(
            df=df,
            model_id="m",
            role_arn="arn:aws:iam::123:role/R",
            input_s3_uri="s3://b/in/",
            output_s3_uri="s3://b/out/",
        )
    with pytest.raises(exceptions.InvalidArgumentCombination):
        wr.bedrock.invoke_batch(
            df=df,
            model_id="m",
            role_arn="arn:aws:iam::123:role/R",
            prompt_column="x",
            model_input_column="x",
            input_s3_uri="s3://b/in/",
            output_s3_uri="s3://b/out/",
        )


def test_invoke_batch_empty_dataframe_raises() -> None:
    with pytest.raises(exceptions.EmptyDataFrame):
        wr.bedrock.invoke_batch(
            df=pd.DataFrame({"prompt": []}),
            model_id="m",
            role_arn="arn:aws:iam::123:role/R",
            prompt_column="prompt",
            input_s3_uri="s3://b/in/",
            output_s3_uri="s3://b/out/",
        )


def test_invoke_batch_below_minimum_records_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    # Restore the production minimum that the autouse fixture lowers for the rest of the suite.
    monkeypatch.setattr(_invoke, "_MIN_RECORDS", 100)
    df = pd.DataFrame({"prompt": [f"p{i}" for i in range(99)]})
    with pytest.raises(exceptions.InvalidArgumentValue, match="minimum of 100"):
        wr.bedrock.invoke_batch(
            df=df,
            model_id="m",
            role_arn="arn:aws:iam::123:role/R",
            prompt_column="prompt",
            input_s3_uri="s3://b/in/",
            output_s3_uri="s3://b/out/",
        )


def test_invoke_batch_failed_job_raises() -> None:
    bedrock, s3, _ = _build_batch_clients([], statuses=["InProgress", "Failed"])
    bedrock.get_model_invocation_job.side_effect = [
        {"status": "InProgress"},
        {"status": "Failed", "message": "something broke"},
    ]
    df = pd.DataFrame({"prompt": ["x"]})

    with _patch_clients({"bedrock": bedrock, "s3": s3}):
        with pytest.raises(exceptions.ServiceApiError, match="Failed"):
            wr.bedrock.invoke_batch(
                df=df,
                model_id="m",
                role_arn="arn:aws:iam::123:role/R",
                prompt_column="prompt",
                input_s3_uri="s3://b/in/",
                output_s3_uri="s3://b/out/",
                wait=True,
                poll_interval_seconds=0,
            )


def test_invoke_batch_no_wait_returns_job_arn_only() -> None:
    bedrock, s3, _ = _build_batch_clients([])
    df = pd.DataFrame({"prompt": ["x"]})

    with _patch_clients({"bedrock": bedrock, "s3": s3}):
        out = wr.bedrock.invoke_batch(
            df=df,
            model_id="m",
            role_arn="arn:aws:iam::123:role/R",
            prompt_column="prompt",
            input_s3_uri="s3://b/in/",
            output_s3_uri="s3://b/out/",
            wait=False,
        )

    assert isinstance(out, str)
    assert out == "arn:aws:bedrock:us-east-1:123:model-invocation-job/abc123def456"
    bedrock.get_model_invocation_job.assert_not_called()


def test_invoke_batch_duplicate_record_ids_raise() -> None:
    bedrock, s3, _ = _build_batch_clients([])
    df = pd.DataFrame({"id": ["a", "b", "a"], "prompt": ["x", "y", "z"]})

    with _patch_clients({"bedrock": bedrock, "s3": s3}):
        with pytest.raises(exceptions.InvalidArgumentValue, match="duplicate"):
            wr.bedrock.invoke_batch(
                df=df,
                model_id="m",
                role_arn="arn:aws:iam::123:role/R",
                prompt_column="prompt",
                record_id_column="id",
                input_s3_uri="s3://b/in/",
                output_s3_uri="s3://b/out/",
            )
    bedrock.create_model_invocation_job.assert_not_called()
    s3.put_object.assert_not_called()


def test_invoke_batch_records_with_errors_are_passed_through() -> None:
    output_records = [
        {
            "recordId": "REC000000000",
            "modelOutput": {"output": {"message": {"content": [{"text": "ok"}]}}},
        },
        {
            "recordId": "REC000000001",
            "error": {"errorCode": 400, "errorMessage": "bad request"},
        },
    ]
    bedrock, s3, _ = _build_batch_clients(output_records)
    df = pd.DataFrame({"prompt": ["good", "bad"]})

    with _patch_clients({"bedrock": bedrock, "s3": s3}):
        out = wr.bedrock.invoke_batch(
            df=df,
            model_id="m",
            role_arn="arn:aws:iam::123:role/R",
            prompt_column="prompt",
            input_s3_uri="s3://b/in/",
            output_s3_uri="s3://b/out/",
            wait=True,
            poll_interval_seconds=0,
        )

    out_sorted = out.sort_values("record_id").reset_index(drop=True)
    assert out_sorted["output_text"].iloc[0] == "ok"
    assert pd.isna(out_sorted["output_text"].iloc[1])
    assert out_sorted["error"].iloc[1] == {"errorCode": 400, "errorMessage": "bad request"}
    assert out_sorted["model_output"].iloc[1] is None


def test_invoke_batch_strip_s3_uri_validates_scheme() -> None:
    with pytest.raises(exceptions.InvalidArgumentValue, match="s3://"):
        _invoke._strip_s3_uri("https://not-s3/")


# ---------------------------------------------------------------------------
# read_invoke_batch
# ---------------------------------------------------------------------------


def _build_wait_clients(
    output_records: list[dict[str, Any]],
    *,
    statuses: list[str] | None = None,
    invocation_type: str = "Converse",
    output_s3_uri: str = "s3://b/out/",
) -> tuple[MagicMock, MagicMock]:
    bedrock = MagicMock()
    s3 = MagicMock()

    statuses = statuses or ["Completed"]
    bedrock.get_model_invocation_job.side_effect = [
        {
            "jobArn": "arn:aws:bedrock:us-east-1:123:model-invocation-job/abc123def456",
            "status": s,
            "modelInvocationType": invocation_type,
            "outputDataConfig": {"s3OutputDataConfig": {"s3Uri": output_s3_uri}},
        }
        for s in statuses
    ]

    s3.list_objects_v2.return_value = {
        "Contents": [{"Key": "out/abc123def456/awswrangler-job.jsonl.out"}],
    }
    s3.get_object.return_value = {
        "Body": io.BytesIO(b"\n".join(json.dumps(r).encode() for r in output_records)),
    }
    return bedrock, s3


def test_read_invoke_batch_returns_flat_results_when_df_omitted() -> None:
    output_records = [
        {
            "recordId": "REC000000000",
            "modelOutput": {"output": {"message": {"content": [{"text": "hi"}]}}},
        },
        {
            "recordId": "REC000000001",
            "modelOutput": {"output": {"message": {"content": [{"text": "bye"}]}}},
        },
    ]
    bedrock, s3 = _build_wait_clients(output_records)

    with _patch_clients({"bedrock": bedrock, "s3": s3}):
        out = wr.bedrock.read_invoke_batch(
            "arn:aws:bedrock:us-east-1:123:model-invocation-job/abc123def456",
            poll_interval_seconds=0,
        )

    assert list(out.columns) == ["record_id", "model_output", "error", "output_text"]
    assert out["output_text"].tolist() == ["hi", "bye"]
    assert out.attrs["status"] == "Completed"
    assert out.attrs["job_arn"].endswith("abc123def456")


def test_read_invoke_batch_merges_back_onto_input_df() -> None:
    output_records = [
        {"recordId": "alpha", "modelOutput": {"output": {"message": {"content": [{"text": "A"}]}}}},
        {"recordId": "beta", "modelOutput": {"output": {"message": {"content": [{"text": "B"}]}}}},
    ]
    bedrock, s3 = _build_wait_clients(output_records)
    df = pd.DataFrame({"id": ["alpha", "beta"], "label": ["x", "y"]})

    with _patch_clients({"bedrock": bedrock, "s3": s3}):
        out = wr.bedrock.read_invoke_batch(
            "arn:aws:bedrock:us-east-1:123:model-invocation-job/abc123def456",
            df=df,
            record_id_column="id",
            poll_interval_seconds=0,
        )

    out_sorted = out.sort_values("id").reset_index(drop=True)
    assert out_sorted["label"].tolist() == ["x", "y"]
    assert out_sorted["output_text"].tolist() == ["A", "B"]


def test_read_invoke_batch_polls_until_terminal() -> None:
    output_records = [
        {"recordId": "REC000000000", "modelOutput": {"output": {"message": {"content": [{"text": "ok"}]}}}}
    ]
    bedrock, s3 = _build_wait_clients(output_records, statuses=["InProgress", "InProgress", "Completed"])

    with _patch_clients({"bedrock": bedrock, "s3": s3}):
        wr.bedrock.read_invoke_batch(
            "arn:aws:bedrock:us-east-1:123:model-invocation-job/abc123def456",
            wait=True,
            poll_interval_seconds=0,
        )

    assert bedrock.get_model_invocation_job.call_count == 3


def test_read_invoke_batch_raises_while_running() -> None:
    bedrock, s3 = _build_wait_clients([], statuses=["InProgress"])

    with _patch_clients({"bedrock": bedrock, "s3": s3}):
        with pytest.raises(exceptions.JobNotFinished, match="InProgress"):
            wr.bedrock.read_invoke_batch(
                "arn:aws:bedrock:us-east-1:123:model-invocation-job/abc123def456",
            )

    assert bedrock.get_model_invocation_job.call_count == 1
    s3.list_objects_v2.assert_not_called()
    s3.get_object.assert_not_called()


def test_read_invoke_batch_failed_status_raises() -> None:
    bedrock, s3 = _build_wait_clients([], statuses=["Failed"])
    bedrock.get_model_invocation_job.side_effect = [
        {
            "jobArn": "arn:aws:bedrock:us-east-1:123:model-invocation-job/abc123def456",
            "status": "Failed",
            "modelInvocationType": "Converse",
            "outputDataConfig": {"s3OutputDataConfig": {"s3Uri": "s3://b/out/"}},
            "message": "boom",
        }
    ]
    with _patch_clients({"bedrock": bedrock, "s3": s3}):
        with pytest.raises(exceptions.ServiceApiError, match="Failed"):
            wr.bedrock.read_invoke_batch(
                "arn:aws:bedrock:us-east-1:123:model-invocation-job/abc123def456",
                poll_interval_seconds=0,
            )


def test_read_invoke_batch_invoke_model_omits_output_text() -> None:
    output_records = [{"recordId": "REC000000000", "modelOutput": {"results": [{"outputText": "ok"}]}}]
    bedrock, s3 = _build_wait_clients(output_records, invocation_type="InvokeModel")

    with _patch_clients({"bedrock": bedrock, "s3": s3}):
        out = wr.bedrock.read_invoke_batch(
            "arn:aws:bedrock:us-east-1:123:model-invocation-job/abc123def456",
            poll_interval_seconds=0,
        )

    assert "output_text" not in out.columns
    assert out["model_output"].iloc[0] == {"results": [{"outputText": "ok"}]}
