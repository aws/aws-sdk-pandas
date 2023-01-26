import tempfile
from datetime import datetime
from decimal import Decimal

import pandas as pd
import pytest

import awswrangler as wr


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
def test_write(params, dynamodb_table):
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
    wr.dynamodb.put_json(file_path, dynamodb_table)
    df2 = wr.dynamodb.read_partiql_query(query)
    assert df.shape == df2.shape

    # CSV
    wr.dynamodb.delete_items(items=df.to_dict("records"), table_name=dynamodb_table)
    file_path = f"{path}/movies.csv"
    df.to_csv(file_path, index=False)
    wr.dynamodb.put_csv(file_path, dynamodb_table)
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
def test_read_partiql(params, dynamodb_table):
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

    wr.dynamodb.put_df(df=df, table_name=dynamodb_table)

    query: str = f'SELECT * FROM "{dynamodb_table}"'
    df2 = wr.dynamodb.read_partiql_query(query, dtype={"iint64": int})
    assert df.shape == df2.shape
    assert df2.iint64.dtype == "int64"

    df3 = wr.dynamodb.read_partiql_query(
        query=f'SELECT * FROM "{dynamodb_table}" WHERE par0=? AND par1=?',
        parameters=[2, "b"],
    )
    assert df3.shape == (1, len(df.columns))

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
def test_execute_statement(params, dynamodb_table):
    df = pd.DataFrame(
        {
            "title": ["Titanic", "Snatch", "The Godfather"],
            "year": [1997, 2000, 1972],
            "genre": ["drama", "caper story", "crime"],
        }
    )

    wr.dynamodb.put_df(df=df, table_name=dynamodb_table)

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
