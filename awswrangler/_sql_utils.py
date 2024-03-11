"""SQL utilities."""

import re

from awswrangler import exceptions


def identifier(sql: str, sql_mode: str = "mysql") -> str:
    """
    Turn the input into an escaped SQL identifier, such as the name of a table or column.

    sql: str
        Identifier to use in SQL.
    sql_mode: str
        "mysql" for default MySQL identifiers (backticks), "ansi" for ANSI-compatible identifiers (double quotes), or
        "mssql" for MSSQL identifiers (square brackets).

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

    if sql_mode == "mysql":
        return f"`{sql}`"
    elif sql_mode == "ansi":
        return f'"{sql}"'
    elif sql_mode == "mssql":
        return f"[{sql}]"

    raise ValueError(f"Unknown SQL MODE: {sql_mode}")
