import modin.pandas as pd

from awswrangler.distributed.ray.datasources import DynamoDBDatasource
from awswrangler.distributed.ray.modin._utils import _ray_dataset_from_df


def _put_df_distributed(
    df: pd.DataFrame,
    table_name: str,
    boto3_session: None,
) -> None:
    # Create Ray Dataset
    ds = _ray_dataset_from_df(df)

    ds.write_datasource(
        datasource=DynamoDBDatasource(),
        table_name=table_name,
    )
