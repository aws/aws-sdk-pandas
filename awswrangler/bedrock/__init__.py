"""Amazon Bedrock Module."""

from awswrangler.bedrock._invoke import invoke_batch, read_invoke_batch

__all__ = [
    "invoke_batch",
    "read_invoke_batch",
]
