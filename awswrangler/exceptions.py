"""Centralized exceptions Module."""


class S3WaitObjectTimeout(Exception):
    """Raise after wr.s3.wait_object_exists() reaches the timeout."""

    pass


class AWSCredentialsNotFound(Exception):
    """Boto3 didn't find any AWS credential."""

    pass
