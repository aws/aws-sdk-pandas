"""Amazon Bedrock - Batch invocation (PRIVATE)."""

from __future__ import annotations

import io
import json
import logging
import time
import uuid
from typing import TYPE_CHECKING, Any, Iterable, Literal, overload

import boto3

import awswrangler.pandas as pd
from awswrangler import _utils, exceptions
from awswrangler._config import apply_configs

if TYPE_CHECKING:
    from botocore.client import BaseClient

_logger: logging.Logger = logging.getLogger(__name__)

_TERMINAL_STATES = {"Completed", "Failed", "Stopped", "PartiallyCompleted", "Expired"}
_SUCCESS_STATES = {"Completed", "PartiallyCompleted"}
_MIN_RECORDS = 100


def _strip_s3_uri(s3_uri: str) -> tuple[str, str]:
    if not s3_uri.startswith("s3://"):
        raise exceptions.InvalidArgumentValue(f"Expected an s3:// URI, got '{s3_uri}'.")
    bucket, _, key = s3_uri[len("s3://") :].partition("/")
    return bucket, key.rstrip("/")


def _join_s3(prefix: str, *parts: str) -> str:
    base = prefix.rstrip("/")
    suffix = "/".join(p.strip("/") for p in parts if p)
    return f"{base}/{suffix}" if suffix else base


def _build_converse_input(prompt: str) -> dict[str, Any]:
    return {"messages": [{"role": "user", "content": [{"text": prompt}]}]}


def _build_records(
    df: pd.DataFrame,
    *,
    prompt_column: str | None,
    model_input_column: str | None,
    record_id_column: str | None,
    invocation_type: str,
    inference_config: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for i, row in enumerate(df.to_dict(orient="records")):
        record_id = str(row[record_id_column]) if record_id_column else f"REC{i:09d}"
        if model_input_column is not None:
            model_input = row[model_input_column]
        else:
            assert prompt_column is not None  # narrowed by caller
            prompt = str(row[prompt_column])
            if invocation_type == "Converse":
                model_input = _build_converse_input(prompt)
                if inference_config:
                    model_input["inferenceConfig"] = inference_config
            else:
                raise exceptions.InvalidArgumentCombination(
                    "`prompt_column` only supports `invocation_type='Converse'`. "
                    "For `InvokeModel`, pass a pre-built model input via `model_input_column`."
                )
        records.append({"recordId": record_id, "modelInput": model_input})
    return records


def _write_jsonl(s3_client: "BaseClient", records: Iterable[dict[str, Any]], bucket: str, key: str) -> None:
    buf = io.BytesIO()
    for rec in records:
        buf.write(json.dumps(rec).encode("utf-8"))
        buf.write(b"\n")
    buf.seek(0)
    s3_client.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())  # type: ignore[attr-defined]


def _list_output_files(s3_client: "BaseClient", bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    token: str | None = None
    while True:
        kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        response = s3_client.list_objects_v2(**kwargs)  # type: ignore[attr-defined]
        for obj in response.get("Contents", []):
            if obj["Key"].endswith(".jsonl.out") or obj["Key"].endswith(".out"):
                if not obj["Key"].endswith("manifest.json.out"):
                    keys.append(obj["Key"])
        token = response.get("NextContinuationToken")
        if not token:
            return keys


def _read_jsonl(s3_client: "BaseClient", bucket: str, key: str) -> list[dict[str, Any]]:
    body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()  # type: ignore[attr-defined]
    return [json.loads(line) for line in body.splitlines() if line.strip()]


def _extract_converse_text(model_output: dict[str, Any]) -> str | None:
    try:
        content = model_output["output"]["message"]["content"]
        return "".join(part.get("text", "") for part in content) or None
    except (KeyError, TypeError):
        return None


def _wait_for_job(
    bedrock_client: "BaseClient",
    job_arn: str,
    poll_interval_seconds: float,
    max_wait_seconds: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + max_wait_seconds
    while True:
        response: dict[str, Any] = bedrock_client.get_model_invocation_job(jobIdentifier=job_arn)  # type: ignore[attr-defined]
        status = response["status"]
        _logger.debug("Batch inference job %s status: %s", job_arn, status)
        if status in _TERMINAL_STATES:
            return response
        if time.monotonic() >= deadline:
            raise exceptions.ServiceApiError(
                f"Batch inference job {job_arn} did not reach a terminal state within "
                f"{max_wait_seconds:.0f}s (last status: {status})."
            )
        time.sleep(poll_interval_seconds)


def _collect_results(
    s3_client: "BaseClient",
    job_arn: str,
    status: str,
    output_s3_uri: str,
    invocation_type: str,
    df: pd.DataFrame | None,
    record_id_column: str | None,
) -> pd.DataFrame:
    output_bucket, output_prefix = _strip_s3_uri(output_s3_uri)
    job_id = job_arn.rsplit("/", 1)[-1]
    output_search_prefix = _join_s3(output_prefix, job_id) if output_prefix else job_id
    output_keys = _list_output_files(s3_client, output_bucket, output_search_prefix)
    if not output_keys:
        raise exceptions.NoFilesFound(
            f"No output files found under s3://{output_bucket}/{output_search_prefix} for job {job_arn}."
        )

    rows: list[dict[str, Any]] = []
    for key in output_keys:
        for record in _read_jsonl(s3_client, output_bucket, key):
            row: dict[str, Any] = {
                "record_id": record.get("recordId"),
                "model_output": record.get("modelOutput"),
                "error": record.get("error"),
            }
            if invocation_type == "Converse" and record.get("modelOutput") is not None:
                row["output_text"] = _extract_converse_text(record["modelOutput"])
            rows.append(row)
    results = pd.DataFrame(rows)

    if df is not None:
        df_with_id = df.copy()
        if record_id_column is not None:
            if record_id_column not in df_with_id.columns:
                raise exceptions.InvalidArgumentValue(f"Column '{record_id_column}' not found in DataFrame.")
            df_with_id["record_id"] = df_with_id[record_id_column].astype(str)
        else:
            df_with_id["record_id"] = [f"REC{i:09d}" for i in range(len(df_with_id))]
        results = df_with_id.merge(results, on="record_id", how="left")

    results.attrs["job_arn"] = job_arn
    results.attrs["status"] = status
    return results


@overload
def invoke_batch(
    df: pd.DataFrame,
    *,
    model_id: str,
    role_arn: str,
    job_name: str | None = ...,
    prompt_column: str | None = ...,
    model_input_column: str | None = ...,
    record_id_column: str | None = ...,
    invocation_type: str = ...,
    inference_config: dict[str, Any] | None = ...,
    input_s3_uri: str,
    output_s3_uri: str,
    wait: Literal[False] = ...,
    poll_interval_seconds: float = ...,
    max_wait_seconds: float = ...,
    timeout_duration_in_hours: int | None = ...,
    extra_create_kwargs: dict[str, Any] | None = ...,
    boto3_session: boto3.Session | None = ...,
) -> str: ...


@overload
def invoke_batch(
    df: pd.DataFrame,
    *,
    model_id: str,
    role_arn: str,
    job_name: str | None = ...,
    prompt_column: str | None = ...,
    model_input_column: str | None = ...,
    record_id_column: str | None = ...,
    invocation_type: str = ...,
    inference_config: dict[str, Any] | None = ...,
    input_s3_uri: str,
    output_s3_uri: str,
    wait: Literal[True],
    poll_interval_seconds: float = ...,
    max_wait_seconds: float = ...,
    timeout_duration_in_hours: int | None = ...,
    extra_create_kwargs: dict[str, Any] | None = ...,
    boto3_session: boto3.Session | None = ...,
) -> pd.DataFrame: ...


@apply_configs
def invoke_batch(
    df: pd.DataFrame,
    *,
    model_id: str,
    role_arn: str,
    job_name: str | None = None,
    prompt_column: str | None = None,
    model_input_column: str | None = None,
    record_id_column: str | None = None,
    invocation_type: str = "Converse",
    inference_config: dict[str, Any] | None = None,
    input_s3_uri: str,
    output_s3_uri: str,
    wait: bool = False,
    poll_interval_seconds: float = 30.0,
    max_wait_seconds: float = 24 * 60 * 60.0,
    timeout_duration_in_hours: int | None = None,
    extra_create_kwargs: dict[str, Any] | None = None,
    boto3_session: boto3.Session | None = None,
) -> str | pd.DataFrame:
    """Submit a DataFrame to Amazon Bedrock batch inference.

    Each row of ``df`` is converted to a ``{"recordId": ..., "modelInput": ...}`` JSONL record,
    uploaded to ``input_s3_uri``, and processed by a ``CreateModelInvocationJob`` request. By
    default the function returns immediately with the job ARN — persist it and harvest the results
    later with :func:`read_invoke_batch`, which also works from a different process. Pass
    ``wait=True`` to block until the job reaches a terminal state, read the output files under
    ``output_s3_uri``, and return a DataFrame joined back to the original rows.

    Bedrock requires a minimum of 100 records per batch inference job.

    Parameters
    ----------
    df
        Input DataFrame.
    model_id
        Bedrock foundation model id (e.g. ``us.anthropic.claude-haiku-4-5-20251001-v1:0``) or an
        inference profile / custom model ARN.
    role_arn
        IAM role ARN that Bedrock assumes to read input and write output. Required by the API.
    job_name
        Optional batch job name. A unique name is generated when omitted.
    prompt_column
        Column containing the prompt text. Used when ``invocation_type="Converse"`` to build the
        Converse request body automatically. Mutually exclusive with ``model_input_column``.
    model_input_column
        Column containing pre-built ``modelInput`` dictionaries. Use this for ``InvokeModel``
        jobs or when you need full control of the request body (multi-turn, system prompts, etc.).
        Mutually exclusive with ``prompt_column``.
    record_id_column
        Column whose string values are used as ``recordId``. When omitted, sequential ids of the
        form ``REC000000000`` are generated.
    invocation_type
        ``"Converse"`` (default) or ``"InvokeModel"``. Controls the ``modelInvocationType`` of the
        underlying job and the format expected for ``modelInput``.
    inference_config
        Optional Converse ``inferenceConfig`` (e.g. ``{"maxTokens": 1024, "temperature": 0.0}``).
        Only applied when ``prompt_column`` is used.
    input_s3_uri
        S3 prefix where the generated input JSONL is uploaded (e.g. ``s3://bucket/batches/in/``).
    output_s3_uri
        S3 prefix that Bedrock writes results to (e.g. ``s3://bucket/batches/out/``).
    wait
        When ``False`` (default), submit the job and return its ARN as a string. Batch jobs
        commonly run for hours, so blocking is opt-in. When ``True``, poll until the job reaches
        a terminal state and return a DataFrame of results.
    poll_interval_seconds
        Delay between job-status polls.
    max_wait_seconds
        Maximum total wait time before raising. Defaults to 24 hours.
    timeout_duration_in_hours
        Optional ``timeoutDurationInHours`` passed to the job (24-168).
    extra_create_kwargs
        Passthrough kwargs merged into the ``CreateModelInvocationJob`` request (e.g.
        ``{"vpcConfig": {...}, "tags": [...]}``).
    boto3_session
        The default boto3 session will be used if **boto3_session** is ``None``.

    Returns
    -------
        When ``wait=False`` (default), the job ARN as a string. When ``wait=True``, a DataFrame
        containing the original columns plus ``record_id``, ``model_output``, and (for
        ``invocation_type="Converse"``) ``output_text``. Failed records have ``model_output=None``
        and a populated ``error`` column.

    Examples
    --------
    Run a Converse batch over a DataFrame of prompts:

    >>> import awswrangler as wr
    >>> job_arn = wr.bedrock.invoke_batch(
    ...     df=my_df,
    ...     model_id="us.anthropic.claude-haiku-4-5-20251001-v1:0",
    ...     role_arn="arn:aws:iam::123456789012:role/BedrockBatchRole",
    ...     prompt_column="prompt",
    ...     inference_config={"maxTokens": 512},
    ...     input_s3_uri="s3://my-bucket/bedrock/in/",
    ...     output_s3_uri="s3://my-bucket/bedrock/out/",
    ... )
    """
    if (prompt_column is None) == (model_input_column is None):
        raise exceptions.InvalidArgumentCombination("Provide exactly one of `prompt_column` or `model_input_column`.")
    if invocation_type not in {"Converse", "InvokeModel"}:
        raise exceptions.InvalidArgumentValue(
            f"`invocation_type` must be 'Converse' or 'InvokeModel'; got {invocation_type!r}."
        )
    if df.empty:
        raise exceptions.EmptyDataFrame("`df` must contain at least one row.")
    if len(df) < _MIN_RECORDS:
        raise exceptions.InvalidArgumentValue(
            f"Bedrock batch inference requires a minimum of {_MIN_RECORDS} records per job; "
            f"got {len(df)}. For smaller workloads use the on-demand `InvokeModel` API instead."
        )
    for col in (prompt_column, model_input_column, record_id_column):
        if col is not None and col not in df.columns:
            raise exceptions.InvalidArgumentValue(f"Column '{col}' not found in DataFrame.")
    if record_id_column is not None:
        record_ids = df[record_id_column].astype(str)
        if record_ids.duplicated().any():
            duplicated = record_ids[record_ids.duplicated()].unique()[:5].tolist()
            raise exceptions.InvalidArgumentValue(
                f"`record_id_column` '{record_id_column}' contains duplicate values "
                f"(e.g. {duplicated}); recordId must be unique per record."
            )

    job_name = job_name or f"awswrangler-{uuid.uuid4().hex[:12]}"
    input_bucket, input_prefix = _strip_s3_uri(input_s3_uri)
    output_bucket, output_prefix = _strip_s3_uri(output_s3_uri)
    input_key = _join_s3(input_prefix, f"{job_name}.jsonl") if input_prefix else f"{job_name}.jsonl"

    s3_client = _utils.client(service_name="s3", session=boto3_session)
    bedrock_client = _utils.client(service_name="bedrock", session=boto3_session)

    records = _build_records(
        df,
        prompt_column=prompt_column,
        model_input_column=model_input_column,
        record_id_column=record_id_column,
        invocation_type=invocation_type,
        inference_config=inference_config,
    )
    _logger.debug("Uploading %d records to s3://%s/%s", len(records), input_bucket, input_key)
    _write_jsonl(s3_client, records, input_bucket, input_key)

    create_kwargs: dict[str, Any] = {
        "jobName": job_name,
        "modelId": model_id,
        "roleArn": role_arn,
        "modelInvocationType": invocation_type,
        "inputDataConfig": {"s3InputDataConfig": {"s3Uri": f"s3://{input_bucket}/{input_key}"}},
        "outputDataConfig": {"s3OutputDataConfig": {"s3Uri": output_s3_uri}},
    }
    if timeout_duration_in_hours is not None:
        create_kwargs["timeoutDurationInHours"] = timeout_duration_in_hours
    if extra_create_kwargs:
        create_kwargs.update(extra_create_kwargs)

    response = bedrock_client.create_model_invocation_job(**create_kwargs)  # type: ignore[attr-defined]
    job_arn = response["jobArn"]
    _logger.info("Submitted Bedrock batch inference job: %s", job_arn)

    if not wait:
        return job_arn

    job = _wait_for_job(bedrock_client, job_arn, poll_interval_seconds, max_wait_seconds)
    status = job["status"]
    if status not in _SUCCESS_STATES:
        message = job.get("message") or job.get("failureMessage") or ""
        raise exceptions.ServiceApiError(
            f"Bedrock batch inference job {job_arn} ended with status '{status}': {message}"
        )

    df_with_id = df.copy()
    df_with_id["record_id"] = [r["recordId"] for r in records]
    return _collect_results(
        s3_client=s3_client,
        job_arn=job_arn,
        status=status,
        output_s3_uri=output_s3_uri,
        invocation_type=invocation_type,
        df=df_with_id,
        record_id_column="record_id",
    )


@apply_configs
def describe_invoke_batch(
    job_arn: str,
    *,
    boto3_session: boto3.Session | None = None,
) -> dict[str, Any]:
    """Describe a Bedrock batch inference job without blocking or raising on its state.

    A thin wrapper around ``GetModelInvocationJob`` for exception-free status polling — e.g. from
    a scheduled harvester that only calls :func:`read_invoke_batch` once ``status`` is terminal
    (``Completed``, ``PartiallyCompleted``, ``Failed``, ``Stopped``, or ``Expired``).

    Parameters
    ----------
    job_arn
        ARN returned by :func:`invoke_batch` (or any ``CreateModelInvocationJob`` caller).
    boto3_session
        The default boto3 session will be used if **boto3_session** is ``None``.

    Returns
    -------
        The ``GetModelInvocationJob`` response as a dictionary (``status``, ``message``,
        ``submitTime``, data configs, ...), without the HTTP response metadata.

    Examples
    --------
    >>> import awswrangler as wr
    >>> if wr.bedrock.describe_invoke_batch(job_arn)["status"] == "Completed":
    ...     results = wr.bedrock.read_invoke_batch(job_arn)
    """
    bedrock_client = _utils.client(service_name="bedrock", session=boto3_session)
    job: dict[str, Any] = bedrock_client.get_model_invocation_job(jobIdentifier=job_arn)  # type: ignore[attr-defined]
    job.pop("ResponseMetadata", None)
    return job


@apply_configs
def read_invoke_batch(
    job_arn: str,
    *,
    df: pd.DataFrame | None = None,
    record_id_column: str | None = None,
    wait: bool = False,
    poll_interval_seconds: float = 30.0,
    max_wait_seconds: float = 24 * 60 * 60.0,
    boto3_session: boto3.Session | None = None,
) -> pd.DataFrame:
    """Read the results of a Bedrock batch inference job into a DataFrame.

    Use this together with :func:`invoke_batch`: submit the job, persist the returned ``job_arn``,
    and call ``read_invoke_batch(job_arn)`` later — from the same notebook or a different process.
    By default the call never blocks: it checks the job status once and raises
    :class:`~awswrangler.exceptions.JobNotFinished` when the job is still running (check back
    later), returns the results DataFrame when it completed, and raises
    :class:`~awswrangler.exceptions.ServiceApiError` when it ended unsuccessfully. Pass
    ``wait=True`` to poll until the job reaches a terminal state instead.

    The output S3 location and invocation type are read from ``GetModelInvocationJob`` so callers
    do not need to remember them. Pass the original DataFrame and ``record_id_column`` to merge the
    model outputs back onto your input rows; otherwise a flat results frame keyed by ``record_id``
    is returned.

    Parameters
    ----------
    job_arn
        ARN returned by :func:`invoke_batch` (or any ``CreateModelInvocationJob`` caller).
    df
        Optional input DataFrame to merge results back onto. When omitted, the returned frame is
        the flat per-record output (one row per ``recordId``).
    record_id_column
        Column in ``df`` whose stringified values match ``recordId`` in the JSONL output. Required
        when ``df`` is provided and you used a custom ``record_id_column`` in :func:`invoke_batch`;
        omit it if the default sequential ``REC000000000`` ids were used.
    wait
        When ``False`` (default), check the job status once and raise
        :class:`~awswrangler.exceptions.JobNotFinished` if it is not yet in a terminal state.
        When ``True``, poll until the job finishes.
    poll_interval_seconds
        Delay between job-status polls (only used when ``wait=True``).
    max_wait_seconds
        Maximum total wait time before raising (only used when ``wait=True``). Defaults to 24 hours.
    boto3_session
        The default boto3 session will be used if **boto3_session** is ``None``.

    Returns
    -------
        DataFrame containing ``record_id``, ``model_output``, ``error``, and (for Converse jobs)
        ``output_text``. When ``df`` is supplied, the original columns are preserved by merging on
        ``record_id``. The job ARN and final status are exposed via ``result.attrs["job_arn"]``
        and ``result.attrs["status"]``.

    Examples
    --------
    Submit now, harvest later:

    >>> import awswrangler as wr
    >>> job_arn = wr.bedrock.invoke_batch(
    ...     df=my_df,
    ...     model_id="us.anthropic.claude-haiku-4-5-20251001-v1:0",
    ...     role_arn="arn:aws:iam::123456789012:role/BedrockBatchRole",
    ...     prompt_column="prompt",
    ...     record_id_column="id",
    ...     input_s3_uri="s3://my-bucket/bedrock/in/",
    ...     output_s3_uri="s3://my-bucket/bedrock/out/",
    ... )
    >>> # ... hours later, in another process ...
    >>> try:
    ...     results = wr.bedrock.read_invoke_batch(job_arn, df=my_df, record_id_column="id")
    ... except wr.exceptions.JobNotFinished:
    ...     print("still running, check back later")
    """
    s3_client = _utils.client(service_name="s3", session=boto3_session)
    bedrock_client = _utils.client(service_name="bedrock", session=boto3_session)

    job: dict[str, Any] = bedrock_client.get_model_invocation_job(jobIdentifier=job_arn)  # type: ignore[attr-defined]
    status = job["status"]
    if status not in _TERMINAL_STATES:
        if not wait:
            raise exceptions.JobNotFinished(
                f"Bedrock batch inference job {job_arn} is not finished yet (status: '{status}'). "
                "Check back later, or pass `wait=True` to block until it completes."
            )
        job = _wait_for_job(bedrock_client, job_arn, poll_interval_seconds, max_wait_seconds)
        status = job["status"]
    if status not in _SUCCESS_STATES:
        message = job.get("message") or job.get("failureMessage") or ""
        raise exceptions.ServiceApiError(
            f"Bedrock batch inference job {job_arn} ended with status '{status}': {message}"
        )

    invocation_type = job.get("modelInvocationType", "Converse")
    output_s3_uri = job["outputDataConfig"]["s3OutputDataConfig"]["s3Uri"]
    return _collect_results(
        s3_client=s3_client,
        job_arn=job_arn,
        status=status,
        output_s3_uri=output_s3_uri,
        invocation_type=invocation_type,
        df=df,
        record_id_column=record_id_column,
    )
