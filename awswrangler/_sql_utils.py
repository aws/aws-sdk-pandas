"""SQL utilities."""
from awswrangler import exceptions


def identifier(sql):
    if not isinstance(sql, str):
        raise exceptions.InvalidArgumentValue("identifier must be a str")

    if len(sql) == 0:
        raise exceptions.InvalidArgumentValue("identifier must be > 0 characters in length")

    quote = not sql[0].isalpha()

    for c in sql[1:]:
        if not (c.isalpha() or c.isdecimal() or c in "-_$"):
            if c == "\u0000":
                raise exceptions.InvalidArgumentValue(
                    "identifier cannot contain the code zero character"
                )
            quote = True
            break

    if quote:
        sql = sql.replace('"', '""')

    return f'`{sql}`'
