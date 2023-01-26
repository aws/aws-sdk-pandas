import tempfile
from decimal import Decimal

import pandas as pd
import pytest
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

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

    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.dynamodb.read_items(table_name=dynamodb_table)

    with pytest.raises(wr.exceptions.InvalidArgumentType):
        wr.dynamodb.read_items(
            table_name=dynamodb_table,
            partition_values=[1],
        )

    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.dynamodb.read_items(
            table_name=dynamodb_table,
            partition_values=[1, 2],
            sort_values=["a"],
        )

    with pytest.raises(ClientError):
        wr.dynamodb.read_items(table_name=dynamodb_table, filter_expression="nonsense")

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
            "connection": ["direct", "indirect", None],
            "volume": [100, 200, 300],
        }
    )
    wr.dynamodb.put_df(df=df, table_name=dynamodb_table)

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
def test_read_items_index(params, dynamodb_table):
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
    wr.dynamodb.put_df(df=df, table_name=dynamodb_table)

    df2 = wr.dynamodb.read_items(
        table_name=dynamodb_table,
        key_condition_expression=Key("Category").eq("Suspense"),
        index_name="CategoryIndex",
    )
    assert df2.shape == df.shape

    df3 = wr.dynamodb.read_items(table_name=dynamodb_table, allow_full_scan=True, index_name="CategoryIndex")
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
def test_read_items_expression(params, dynamodb_table):
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
    df = wr.dynamodb.read_items(
        table_name=dynamodb_table,
        filter_expression="#operator = :v",
        expression_attribute_names={"#operator": "operator"},
        expression_attribute_values={":v": "Eido"},
    )
