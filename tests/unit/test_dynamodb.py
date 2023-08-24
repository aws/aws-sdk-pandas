import itertools
import tempfile
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict

import pyarrow as pa
import pytest
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

import awswrangler as wr
import awswrangler.pandas as pd

from .._utils import is_ray_modin

pytestmark = pytest.mark.distributed


@pytest.mark.parametrize(
    "params",
    [
        {
            "KeySchema": [{"AttributeName": "title", "KeyType": "HASH"}, {"AttributeName": "year", "KeyType": "RANGE"}],
            "AttributeDefinitions": [
                {"AttributeName": "title", "AttributeType": "S"},
                {"AttributeName": "year", "AttributeType": "N"},
            ],
        }
    ],
)
@pytest.mark.parametrize("use_threads", [False, True])
def test_write(params: Dict[str, Any], use_threads: bool, dynamodb_table: str) -> None:
    df = pd.DataFrame(
        {
            "title": ["Titanic", "Snatch", "The Godfather"],
            "year": [1997, 2000, 1972],
            "genre": ["drama", "caper story", "crime"],
        }
    )
    path = tempfile.gettempdir()
    query = f'SELECT * FROM "{dynamodb_table}"'

    # JSON
    file_path = f"{path}/movies.json"
    df.to_json(file_path, orient="records")
    wr.dynamodb.put_json(file_path, dynamodb_table, use_threads=use_threads)
    df2 = wr.dynamodb.read_partiql_query(query)
    assert df.shape == df2.shape

    # CSV
    wr.dynamodb.delete_items(items=df.to_dict("records"), table_name=dynamodb_table)
    file_path = f"{path}/movies.csv"
    df.to_csv(file_path, index=False)
    wr.dynamodb.put_csv(file_path, dynamodb_table, use_threads=use_threads)
    df3 = wr.dynamodb.read_partiql_query(query)
    assert df.shape == df3.shape


@pytest.mark.parametrize(
    "params",
    [
        {
            "KeySchema": [{"AttributeName": "par0", "KeyType": "HASH"}, {"AttributeName": "par1", "KeyType": "RANGE"}],
            "AttributeDefinitions": [
                {"AttributeName": "par0", "AttributeType": "N"},
                {"AttributeName": "par1", "AttributeType": "S"},
            ],
        }
    ],
)
@pytest.mark.parametrize("use_threads", [False, True])
def test_read_partiql(params: Dict[str, Any], use_threads: bool, dynamodb_table: str) -> None:
    df = pd.DataFrame(
        {
            "par0": [1, 1, 2],
            "par1": ["a", "b", "b"],
            "iint64": [1, 2, 3],
            "string": ["Seattle", "New York", "Washington"],
            "decimal": [Decimal(1.0), Decimal(2.0), Decimal(3.0)],
            "bool": [True, False, False],
            "binary": [b"0", b"1", b"2"],
        }
    )

    wr.dynamodb.put_df(df=df, table_name=dynamodb_table, use_threads=use_threads)

    query: str = f'SELECT * FROM "{dynamodb_table}"'
    df2 = wr.dynamodb.read_partiql_query(
        query=f'SELECT * FROM "{dynamodb_table}" WHERE par0=? AND par1=?',
        parameters=[2, "b"],
    )
    assert df2.shape == (1, len(df.columns))

    dfs = wr.dynamodb.read_partiql_query(query, chunked=True)
    assert df.shape == pd.concat(dfs).shape


@pytest.mark.parametrize(
    "params",
    [
        {
            "KeySchema": [{"AttributeName": "title", "KeyType": "HASH"}, {"AttributeName": "year", "KeyType": "RANGE"}],
            "AttributeDefinitions": [
                {"AttributeName": "title", "AttributeType": "S"},
                {"AttributeName": "year", "AttributeType": "N"},
            ],
        }
    ],
)
@pytest.mark.parametrize("use_threads", [False, True])
def test_execute_statement(params: Dict[str, Any], use_threads: bool, dynamodb_table: str) -> None:
    df = pd.DataFrame(
        {
            "title": ["Titanic", "Snatch", "The Godfather"],
            "year": [1997, 2000, 1972],
            "genre": ["drama", "caper story", "crime"],
        }
    )

    wr.dynamodb.put_df(df=df, table_name=dynamodb_table, use_threads=use_threads)

    title = "The Lord of the Rings: The Fellowship of the Ring"
    year = datetime.now().year
    genre = "epic"
    rating = Decimal("9.9")
    plot = "The fate of Middle-earth hangs in the balance as Frodo and his companions begin their journey to Mount Doom"

    # Insert items
    wr.dynamodb.execute_statement(
        statement=f"INSERT INTO {dynamodb_table} VALUE {{'title': ?, 'year': ?, 'genre': ?, 'info': ?}}",
        parameters=[title, year, genre, {"plot": plot, "rating": rating}],
    )

    # Select items
    items = wr.dynamodb.execute_statement(
        statement=f'SELECT * FROM "{dynamodb_table}" WHERE title=? AND year=?',
        parameters=[title, year],
    )
    assert len(list(items)) == 1

    # Update items
    wr.dynamodb.execute_statement(
        statement=f'UPDATE "{dynamodb_table}" SET info.rating=? WHERE title=? AND year=?',
        parameters=[Decimal(10), title, year],
    )
    df2 = wr.dynamodb.read_partiql_query(
        query=f'SELECT info FROM "{dynamodb_table}" WHERE title=? AND year=?',
        parameters=[title, year],
    )
    info = df2.iloc[0, 0]
    assert info["rating"] == 10

    # Delete items
    wr.dynamodb.execute_statement(
        statement=f'DELETE FROM "{dynamodb_table}" WHERE title=? AND year=?',
        parameters=[title, year],
    )
    df3 = wr.dynamodb.read_partiql_query(f'SELECT * FROM "{dynamodb_table}"')
    assert df.shape == df3.shape


@pytest.mark.parametrize(
    "params",
    [
        {
            "KeySchema": [{"AttributeName": "par0", "KeyType": "HASH"}, {"AttributeName": "par1", "KeyType": "RANGE"}],
            "AttributeDefinitions": [
                {"AttributeName": "par0", "AttributeType": "N"},
                {"AttributeName": "par1", "AttributeType": "S"},
            ],
        }
    ],
)
@pytest.mark.parametrize("format", ["csv", "json"])
@pytest.mark.parametrize("use_threads", [False, True])
def test_dynamodb_put_from_file(
    format: str, use_threads: bool, params: Dict[str, Any], dynamodb_table: str, local_filename: str
) -> None:
    df = pd.DataFrame({"par0": [1, 2], "par1": ["foo", "boo"]})

    if format == "csv":
        df.to_csv(local_filename, index=False)
        wr.dynamodb.put_csv(
            path=local_filename,
            table_name=dynamodb_table,
            use_threads=use_threads,
        )
    elif format == "json":
        df.to_json(local_filename, orient="records")
        wr.dynamodb.put_json(
            path=local_filename,
            table_name=dynamodb_table,
            use_threads=use_threads,
        )
    else:
        raise RuntimeError(f"Unknown format {format}")

    df2 = wr.dynamodb.read_partiql_query(query=f"SELECT * FROM {dynamodb_table}")

    assert df.shape == df2.shape


@pytest.mark.parametrize(
    "params",
    [
        {
            "KeySchema": [{"AttributeName": "par0", "KeyType": "HASH"}, {"AttributeName": "par1", "KeyType": "RANGE"}],
            "AttributeDefinitions": [
                {"AttributeName": "par0", "AttributeType": "N"},
                {"AttributeName": "par1", "AttributeType": "S"},
            ],
        }
    ],
)
@pytest.mark.parametrize("use_threads", [False, True])
@pytest.mark.parametrize("chunked", [False, True])
def test_read_items_simple(params: Dict[str, Any], dynamodb_table: str, use_threads: bool, chunked: bool) -> None:
    data = [
        {
            "par0": Decimal("2"),
            "par1": "b",
            "bool": False,
            "string": "Washington",
            "iint64": Decimal("3"),
            "binary": b"2",
            "decimal": Decimal("3"),
        },
        {
            "par0": Decimal("1"),
            "par1": "a",
            "bool": True,
            "string": "Seattle",
            "iint64": Decimal("1"),
            "binary": b"0",
            "decimal": Decimal("1"),
        },
        {
            "par0": Decimal("1"),
            "par1": "b",
            "bool": False,
            "string": "New York",
            "iint64": Decimal("2"),
            "binary": b"1",
            "decimal": Decimal("2"),
        },
    ]
    df = pd.DataFrame(data)
    wr.dynamodb.put_df(df=df, table_name=dynamodb_table, use_threads=use_threads)

    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.dynamodb.read_items(table_name=dynamodb_table, use_threads=use_threads, chunked=chunked)

    with pytest.raises(wr.exceptions.InvalidArgumentType):
        wr.dynamodb.read_items(
            table_name=dynamodb_table,
            partition_values=[1],
            use_threads=use_threads,
            chunked=chunked,
        )

    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.dynamodb.read_items(
            table_name=dynamodb_table,
            partition_values=[1, 2],
            sort_values=["a"],
            use_threads=use_threads,
            chunked=chunked,
        )

    with pytest.raises((ClientError, AttributeError)):
        out = wr.dynamodb.read_items(
            table_name=dynamodb_table, filter_expression="nonsense", use_threads=use_threads, chunked=chunked
        )
        if chunked:
            next(out)

    df2 = wr.dynamodb.read_items(
        table_name=dynamodb_table,
        max_items_evaluated=2,
        pyarrow_additional_kwargs={"types_mapper": None},
        use_threads=use_threads,
        chunked=chunked,
    )
    if chunked:
        df2 = pd.concat(df2)
    assert df2.shape == (2, len(df.columns))
    assert df2.dtypes.to_list() == df.dtypes.to_list()

    df3 = wr.dynamodb.read_items(
        table_name=dynamodb_table,
        partition_values=[2],
        sort_values=["b"],
        use_threads=use_threads,
        chunked=chunked,
    )
    if chunked:
        df3 = pd.concat(df3)
    assert df3.shape == (1, len(df.columns))

    df4 = wr.dynamodb.read_items(
        table_name=dynamodb_table,
        allow_full_scan=True,
        as_dataframe=False,
        use_threads=use_threads,
        chunked=chunked,
    )
    if chunked:
        df4 = list(itertools.chain.from_iterable(df4))
    assert df4 == data


@pytest.mark.parametrize(
    "params",
    [
        {
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "N"}],
        }
    ],
)
@pytest.mark.parametrize("use_threads", [False, True])
def test_read_items_reserved(params: Dict[str, Any], dynamodb_table: str, use_threads: bool) -> None:
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "capacity": [Decimal("23"), Decimal("46"), Decimal("92")],
            "connection": ["direct", "indirect", None],
            "volume": [100, 200, 300],
        }
    )
    wr.dynamodb.put_df(df=df, table_name=dynamodb_table, use_threads=use_threads)

    columns = ["id", "capacity", "connection"]
    df2 = wr.dynamodb.read_items(table_name=dynamodb_table, partition_values=[2], columns=columns)
    assert df2.shape == (1, len(columns))


@pytest.mark.parametrize(
    "params",
    [
        {
            "KeySchema": [
                {"AttributeName": "Author", "KeyType": "HASH"},
                {"AttributeName": "Title", "KeyType": "RANGE"},
            ],
            "AttributeDefinitions": [
                {"AttributeName": "Author", "AttributeType": "S"},
                {"AttributeName": "Title", "AttributeType": "S"},
                {"AttributeName": "Category", "AttributeType": "S"},
            ],
            "GlobalSecondaryIndexes": [
                {
                    "IndexName": "CategoryIndex",
                    "KeySchema": [{"AttributeName": "Category", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
        }
    ],
)
@pytest.mark.parametrize("use_threads", [False, True])
@pytest.mark.parametrize("chunked", [False, True])
def test_read_items_index(params: Dict[str, Any], dynamodb_table: str, use_threads: bool, chunked: bool) -> None:
    df = pd.DataFrame(
        {
            "Author": ["John Grisham", "John Grisham", "James Patterson"],
            "Title": ["The Rainmaker", "The Firm", "Along Came a Spider"],
            "Category": ["Suspense", "Suspense", "Suspense"],
            "Formats": [
                {"Hardcover": "J4SUKVGU", "Paperback": "D7YF4FCX"},
                {"Hardcover": "Q7QWE3U2", "Paperback": "ZVZAYY4F", "Audiobook": "DJ9KS9NM"},
                {"Hardcover": "C9NR6RJ7", "Paperback": "37JVGDZG", "Audiobook": "6348WX3U"},
            ],
        }
    )
    wr.dynamodb.put_df(df=df, table_name=dynamodb_table, use_threads=use_threads)

    df2 = wr.dynamodb.read_items(
        table_name=dynamodb_table,
        key_condition_expression=Key("Category").eq("Suspense"),
        index_name="CategoryIndex",
        max_items_evaluated=1,
        chunked=chunked,
    )
    if chunked:
        df2 = pd.concat(df2)
    assert df2.shape == (1, len(df.columns))

    df3 = wr.dynamodb.read_items(
        table_name=dynamodb_table, allow_full_scan=True, index_name="CategoryIndex", use_threads=1, chunked=chunked
    )
    if chunked:
        df3 = pd.concat(df3)
    assert df3.shape == df.shape


@pytest.mark.parametrize(
    "params",
    [
        {
            "KeySchema": [{"AttributeName": "par0", "KeyType": "HASH"}, {"AttributeName": "par1", "KeyType": "RANGE"}],
            "AttributeDefinitions": [
                {"AttributeName": "par0", "AttributeType": "N"},
                {"AttributeName": "par1", "AttributeType": "S"},
            ],
        }
    ],
)
@pytest.mark.parametrize("use_threads", [False, True])
def test_read_items_expression(params: Dict[str, Any], dynamodb_table: str, use_threads: bool) -> None:
    df = pd.DataFrame(
        {
            "par0": [1, 1, 2],
            "par1": ["a", "b", "c"],
            "operator": ["Ali", "Sarah", "Eido"],
            "volume": [100, 200, 300],
            "compliant": [True, False, False],
        }
    )
    wr.dynamodb.put_df(df=df, table_name=dynamodb_table)

    # Batch read
    df1 = wr.dynamodb.read_items(
        table_name=dynamodb_table,
        partition_values=[1, 2],
        sort_values=["b", "c"],
    )
    assert df1.shape == (2, len(df.columns))

    # KeyConditionExpression as Key
    df2 = wr.dynamodb.read_items(
        table_name=dynamodb_table, key_condition_expression=(Key("par0").eq(1) & Key("par1").eq("b"))
    )
    assert df2.shape == (1, len(df.columns))

    # KeyConditionExpression as string
    df3 = wr.dynamodb.read_items(
        table_name=dynamodb_table,
        key_condition_expression="par0 = :v1 and par1 = :v2",
        expression_attribute_values={":v1": 1, ":v2": "b"},
    )
    assert df3.shape == (1, len(df.columns))

    # FilterExpression as Attr
    df4 = wr.dynamodb.read_items(table_name=dynamodb_table, filter_expression=Attr("par0").eq(1))
    assert df4.shape == (2, len(df.columns))

    # FilterExpression as string
    df5 = wr.dynamodb.read_items(
        table_name=dynamodb_table, filter_expression="par0 = :v", expression_attribute_values={":v": 1}
    )
    assert df5.shape == (2, len(df.columns))

    # Reserved keyword
    df6 = wr.dynamodb.read_items(
        table_name=dynamodb_table,
        filter_expression="#operator = :v",
        expression_attribute_names={"#operator": "operator"},
        expression_attribute_values={":v": "Eido"},
    )
    assert df6.shape == (1, len(df.columns))


@pytest.mark.parametrize(
    "params",
    [
        {
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "N"}],
        }
    ],
)
@pytest.mark.parametrize("max_items_evaluated", [1, 3, 5])
@pytest.mark.parametrize("chunked", [False, True])
def test_read_items_limited(
    params: Dict[str, Any], dynamodb_table: str, max_items_evaluated: int, chunked: bool
) -> None:
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "word": ["this", "is", "a", "test"],
            "char_count": [4, 2, 1, 4],
        }
    )
    wr.dynamodb.put_df(df=df, table_name=dynamodb_table)

    df2 = wr.dynamodb.read_items(
        table_name=dynamodb_table,
        filter_expression=Attr("id").eq(1),
        max_items_evaluated=max_items_evaluated,
        chunked=chunked,
    )
    if chunked:
        df2 = pd.concat(df2)
    assert df2.shape == (1, len(df.columns))

    df3 = wr.dynamodb.read_items(
        table_name=dynamodb_table,
        max_items_evaluated=max_items_evaluated,
        chunked=chunked,
    )
    if chunked:
        df3 = pd.concat(df3)
    assert df3.shape == (min(max_items_evaluated, len(df)), len(df.columns))


@pytest.mark.parametrize(
    "params",
    [
        {
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "N"}],
        }
    ],
)
@pytest.mark.parametrize("chunked", [False, True])
def test_read_items_schema(params, dynamodb_table: str, chunked: bool):
    df = pd.DataFrame(
        {
            "id": [Decimal("123.4"), Decimal("226.49"), Decimal("320.320"), Decimal("425.0753")],
            "word": ["this", "is", "a", "test"],
            "char_count": [4, 2, 1, 4],
        }
    )
    wr.dynamodb.put_df(df=df, table_name=dynamodb_table)

    if not is_ray_modin:
        with pytest.raises(pa.ArrowInvalid):
            wr.dynamodb.read_items(table_name=dynamodb_table, allow_full_scan=True)

    schema = pa.schema([("id", pa.decimal128(7, 4)), ("word", pa.string()), ("char_count", pa.int8())])
    kwargs = {
        "table_name": dynamodb_table,
        "chunked": chunked,
        "pyarrow_additional_kwargs": {"schema": schema},
    }
    wr.dynamodb.read_items(allow_full_scan=True, **kwargs)
    wr.dynamodb.read_items(filter_expression=Attr("id").eq(1), **kwargs)
