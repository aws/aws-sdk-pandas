"""PyTorch Module."""

from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Union

import torch
import boto3  # type: ignore
import botocore.exceptions  # type: ignore
import pandas as pd  # type: ignore
import pandas.io.parsers  # type: ignore
import pyarrow as pa  # type: ignore
import pyarrow.lib  # type: ignore
import pyarrow.parquet  # type: ignore
import s3fs  # type: ignore
from boto3.s3.transfer import TransferConfig  # type: ignore
from pandas.io.common import infer_compression  # type: ignore
from  torch.utils.data import Dataset, IterableDataset

from awswrangler import _data_types, _utils, catalog, exceptions, s3

_logger: logging.Logger = logging.getLogger(__name__)


class S3Dataset(Dataset):
    """PyTorch Map-Style S3 Dataset.

    Parameters
    ----------
    path : Union[str, List[str]]
        S3 prefix (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    torch.utils.data.Dataset

    Examples
    --------
    >>> import awswrangler as wr
    >>> import boto3
    >>> label_fn = lambda path: path.split[0][-2]
    >>> ds = wr.torch.S3Dataset('s3://bucket/path', label_fn, boto3.Session())

    """
    def __init__(self, path: Union[str, List[str]], label_fn, boto3_session):
        super(S3IterableDataset).__init__()
        self.label_fn = label_fn
        self.paths: List[str] = s3._path2list(
            path=path,
            boto3_session=self.boto3_session
        )
        self._s3 = boto3_session.resource('s3')

    def _fetch_obj(self, path):
        obj = _s3.Object(bucket_name, key).get()
        return obj['Body'].read()

    def __getitem__(self, index):
        path = self.paths[index])
        return [self._fetch_obj(path), label_fn(path)]

    def __len__(self):
        return len(self.paths)


class SQLDataset(torch.utils.data.IterableDataset):
    """PyTorch Iterable SQL Dataset.

    Parameters
    ----------
    path : Union[str, List[str]]
        S3 prefix (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    torch.utils.data.Dataset

    Examples
    --------
    >>> import awswrangler as wr
    >>> con = wr.catalog.get_engine("aws-data-wrangler-postgresql")
    >>> ds = wr.torch.SQLDataset('select * from public.tutorial', con=con)

    """
    def __init__(self, ):
        super(SQLDataset).__init__(
            sql: str,
            con: sqlalchemy.engine.Engine,
            index_col: Optional[Union[str, List[str]]] = None,
        ):
        self.sql = sql
        self.con = con
        self.index_col = index_col

    def __iter__(self):
        worker_info = torch.utils.data.get_worker_info()
        if worker_info is None:  # single-process data loading, return the full iterator
            pass
        else:  # in a worker process
            raise NotImplemented()

        for ds in wr.db._read_sql_query(
            fn=wr.db._records2numpy,
            sql=self.sql,
            con=self.con,
            index_col=self.index_col,
        ):
            for row in ds:
                yield row
