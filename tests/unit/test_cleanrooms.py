import pytest

import awswrangler as wr
import awswrangler.pandas as pd

pytestmark = pytest.mark.distributed


@pytest.fixture()
def data(cleanrooms_s3_bucket_name: str, cleanrooms_glue_database_name: str) -> None:
    df_purchases = pd.DataFrame(
        {
            "purchase_id": list(range(100, 109)),
            "user_id": [1, 2, 3, 1, 2, 3, 4, 5, 6],
            "sale_value": [2.2, 1.1, 6.2, 2.3, 7.8, 9.9, 7.3, 9.7, 0.7],
        }
    )
    wr.s3.to_parquet(
        df_purchases,
        f"s3://{cleanrooms_s3_bucket_name}/purchases/",
        dataset=True,
        database=cleanrooms_glue_database_name,
        table="purchases",
        mode="overwrite",
    )

    df_users = pd.DataFrame(
        {
            "user_id": list(range(1, 9)),
            "city": ["LA", "NYC", "Chicago", "NYC", "NYC", "LA", "Seattle", "Seattle"],
        }
    )
    wr.s3.to_parquet(
        df_users,
        f"s3://{cleanrooms_s3_bucket_name}/users/",
        dataset=True,
        database=cleanrooms_glue_database_name,
        table="users",
        mode="overwrite",
    )


def test_read_sql_query(data: None, cleanrooms_membership_id: str, cleanrooms_s3_bucket_name: str):
    sql = """SELECT city, AVG(p.sale_value)
    FROM users u
        INNER JOIN purchases p ON u.user_id = p.user_id
    GROUP BY city
    """
    chunksize = 2
    df_chunked = wr.cleanrooms.read_sql_query(
        sql=sql,
        membership_id=cleanrooms_membership_id,
        output_bucket=cleanrooms_s3_bucket_name,
        output_prefix="results",
        chunksize=chunksize,
        keep_files=False,
    )
    for df in df_chunked:
        assert df.shape == (chunksize, 2)

    sql = """SELECT COUNT(p.purchase_id), SUM(p.sale_value), city
    FROM users u
        INNER JOIN purchases p ON u.user_id = p.user_id
    GROUP BY city
    """
    df = wr.cleanrooms.read_sql_query(
        sql=sql,
        membership_id=cleanrooms_membership_id,
        output_bucket=cleanrooms_s3_bucket_name,
        output_prefix="results",
        keep_files=False,
    )
    assert df.shape == (2, 3)
