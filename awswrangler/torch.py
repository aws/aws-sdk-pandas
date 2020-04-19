"""PyTorch Module."""

import logging

import sqlalchemy  # type: ignore
import torch
from torch.utils.data.dataset import IterableDataset

from awswrangler import db

_logger: logging.Logger = logging.getLogger(__name__)


# class S3Dataset(Dataset):
#     """PyTorch Map-Style S3 Dataset.
#
#     Parameters
#     ----------
#     path : Union[str, List[str]]
#         S3 prefix (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
#     boto3_session : boto3.Session(), optional
#         Boto3 Session. The default boto3 session will be used if boto3_session receive None.
#
#     Returns
#     -------
#     torch.utils.data.Dataset
#
#     Examples
#     --------
#     >>> import awswrangler as wr
#     >>> import boto3
#     >>> label_fn = lambda path: path.split[0][-2]
#     >>> ds = wr.torch.S3Dataset('s3://bucket/path', label_fn, boto3.Session())
#
#     """
#     def __init__(self, path: Union[str, List[str]], label_fn, boto3_session):
#         super(S3IterableDataset).__init__()
#         self.label_fn = label_fn
#         self.paths: List[str] = s3._path2list(
#             path=path,
#             boto3_session=self.boto3_session
#         )
#         self._s3 = boto3_session.resource('s3')
#
#     def _fetch_obj(self, path):
#         obj = _s3.Object(bucket_name, key).get()
#         return obj['Body'].read()
#
#     def __getitem__(self, index):
#         path = self.paths[index])
#         return [self._fetch_obj(path), label_fn(path)]
#
#     def __len__(self):
#         return len(self.paths)


class SQLDataset(IterableDataset):  # pylint: disable=too-few-public-methods,abstract-method
    """Pytorch Iterable SQL Dataset."""

    def __init__(self, sql: str, con: sqlalchemy.engine.Engine):
        """Pytorch Iterable SQL Dataset.

        Support for **Redshift**, **PostgreSQL** and **MySQL**.

        Parameters
        ----------
        sql : str
            Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
        con : sqlalchemy.engine.Engine
            SQLAlchemy Engine. Please use,
            wr.db.get_engine(), wr.db.get_redshift_temp_engine() or wr.catalog.get_engine()

        Returns
        -------
        torch.utils.data.dataset.IterableDataset

        Examples
        --------
        >>> import awswrangler as wr
        >>> con = wr.catalog.get_engine("aws-data-wrangler-postgresql")
        >>> ds = wr.torch.SQLDataset('select * from public.tutorial', con=con)

        """
        super().__init__()
        self.sql = sql
        self.con = con

    def __iter__(self):
        """Iterate over the Dataset."""
        worker_info = torch.utils.data.get_worker_info()
        if worker_info is None:  # single-process data loading, return the full iterator
            pass
        else:  # in a worker process
            raise NotImplementedError()

        for ds in db._read_sql_query(fn=db._records2numpy, sql=self.sql, con=self.con):
            for row in ds:
                yield row
