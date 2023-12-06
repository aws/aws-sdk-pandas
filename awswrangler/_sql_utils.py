"""SQL utilities."""
import re

from awswrangler import exceptions


def identifier(sql: str) -> str:
    """
    Turn the input into an escaped SQL identifier, such as the name of a table or column.

    sql: str
        Identifier to use in SQL.

    Returns
    -------
    str
        Escaped SQL identifier.
    """
    if not isinstance(sql, str):
        raise exceptions.InvalidArgumentValue("identifier must be a str")

    if len(sql) == 0:
        raise exceptions.InvalidArgumentValue("identifier must be > 0 characters in length")

    if re.search(r"[^a-zA-Z0-9-_ ]", sql):
        raise exceptions.InvalidArgumentValue(
            "identifier must contain only alphanumeric characters, spaces, underscores, or hyphens"
        )

    return f"`{sql}`"
