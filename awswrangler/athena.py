"""Amazon Athena Module."""

import re
import unicodedata


def _normalize_name(name: str) -> str:
    name = "".join(c for c in unicodedata.normalize("NFD", name) if unicodedata.category(c) != "Mn")
    name = name.replace("{", "_")
    name = name.replace("}", "_")
    name = name.replace("]", "_")
    name = name.replace("[", "_")
    name = name.replace(")", "_")
    name = name.replace("(", "_")
    name = name.replace(" ", "_")
    name = name.replace("-", "_")
    name = name.replace(".", "_")
    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    name = re.sub("([a-z0-9])([A-Z])", r"\1_\2", name)
    name = name.lower()
    name = re.sub(r"(_)\1+", "\\1", name)  # remove repeated underscores
    name = name[1:] if name.startswith("_") else name  # remove trailing underscores
    name = name[:-1] if name.endswith("_") else name  # remove trailing underscores
    return name


def normalize_column_name(column: str) -> str:
    """Convert the column name to be compatible with Amazon Athena.

    https://docs.aws.amazon.com/athena/latest/ug/tables-databases-columns-names.html

    Parameters
    ----------
    column : str
        Column name.

    Returns
    -------
    str
        Normalized column name.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.athena.normalize_column_name("MyNewColumn")
    "my_new_column"

    """
    return _normalize_name(name=column)


def normalize_table_name(table: str) -> str:
    """Convert the table name to be compatible with Amazon Athena.

    https://docs.aws.amazon.com/athena/latest/ug/tables-databases-columns-names.html

    Parameters
    ----------
    table : str
        Table name.

    Returns
    -------
    str
        Normalized table name.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.athena.normalize_table_name("MyNewTable")
    "my_new_table"

    """
    return _normalize_name(name=table)
