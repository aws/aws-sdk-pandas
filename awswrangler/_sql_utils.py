"""SQL utilities."""
import re

from awswrangler import exceptions


def identifier(sql):
    if not isinstance(sql, str):
        raise exceptions.InvalidArgumentValue("identifier must be a str")

    if len(sql) == 0:
        raise exceptions.InvalidArgumentValue("identifier must be > 0 characters in length")

    if re.search(r"[^a-zA-Z0-9-_ ]", sql):
        raise exceptions.InvalidArgumentValue(
            "identifier must contain only alphanumeric characters, spaces, underscores, or hyphens"
        )

    return f"`{sql}`"
