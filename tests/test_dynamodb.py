import tempfile
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
    query = f"SELECT * FROM {dynamodb_table}"

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
def test_read_items_simple(params, dynamodb_table):
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
    wr.dynamodb.put_df(df=df, table_name=dynamodb_table)

    df2 = wr.dynamodb.read_items(table_name=dynamodb_table, max_items_evaluated=5)
    assert df2.shape == df.shape
    assert df2.dtypes.to_list() == df.dtypes.to_list()

    df3 = wr.dynamodb.read_items(table_name=dynamodb_table, partition_values=[2], sort_values=["b"])
    assert df3.shape == (1, len(df.columns))

    assert wr.dynamodb.read_items(table_name=dynamodb_table, allow_full_scan=True, as_dataframe=False) == data


@pytest.mark.parametrize(
    "params",
    [
        {
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "N"}],
        }
    ],
)
def test_read_items_reserved(params, dynamodb_table):
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "capacity": [Decimal("23"), Decimal("46"), Decimal("92")],
            "connection": ["direct", "indirect", "binary"],
            "volume": [100, 200, 300],
        }
    )
    wr.dynamodb.put_df(df=df, table_name=dynamodb_table)

    columns = ["id", "capacity", "connection"]
    df2 = wr.dynamodb.read_items(table_name=dynamodb_table, partition_values=[2], columns=columns)
    assert df2.shape == (1, len(columns))
