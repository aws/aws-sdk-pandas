"""Amazon S3 Vectors - Bedrock embedding helper (PRIVATE)."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any, Callable

import boto3

from awswrangler import _utils, exceptions
from awswrangler._executor import _get_executor

if TYPE_CHECKING:
    from botocore.client import BaseClient

_logger: logging.Logger = logging.getLogger(__name__)


def embed_texts(
    texts: list[str],
    model_id: str,
    model_kwargs: dict[str, Any] | None = None,
    use_threads: bool | int = True,
    boto3_session: boto3.Session | None = None,
) -> list[list[float]]:
    """Embed a list of strings via Amazon Bedrock, optionally in parallel.

    Supported model id prefixes: ``amazon.titan-embed-text-*``, ``cohere.embed-*``.
    """
    if not texts:
        return []

    extra = dict(model_kwargs or {})
    build_body: Callable[[str], dict[str, Any]]
    parse_response: Callable[[dict[str, Any]], list[float]]

    if model_id.startswith("amazon.titan-embed-text"):

        def build_body(text: str) -> dict[str, Any]:
            return {"inputText": text, **extra}

        def parse_response(payload: dict[str, Any]) -> list[float]:
            return list(payload["embedding"])

    elif model_id.startswith("cohere.embed"):
        extra.setdefault("input_type", "search_document")

        def build_body(text: str) -> dict[str, Any]:
            return {"texts": [text], **extra}

        def parse_response(payload: dict[str, Any]) -> list[float]:
            return list(payload["embeddings"][0])

    else:
        raise exceptions.InvalidArgument(
            f"Unsupported Bedrock embedding model_id '{model_id}'. "
            "Pre-compute embeddings and pass them via `vector_column` instead. "
            "Supported model id prefixes: 'amazon.titan-embed-text', 'cohere.embed'."
        )

    def embed_one(client: "BaseClient", text: str) -> list[float]:
        response = client.invoke_model(  # type: ignore[attr-defined]
            modelId=model_id,
            accept="application/json",
            contentType="application/json",
            body=json.dumps(build_body(text)).encode("utf-8"),
        )
        return parse_response(json.loads(response["body"].read()))

    client = _utils.client(service_name="bedrock-runtime", session=boto3_session)
    executor = _get_executor(use_threads=use_threads)
    return list(executor.map(embed_one, client, texts))
