"""PyTorch Module."""

import logging
from io import BytesIO
from typing import Optional, Union, List

import sqlalchemy  # type: ignore
import numpy as np  # type: ignore
import boto3  # type: ignore
import torch
from torch.utils.data.dataset import Dataset, IterableDataset
from PIL import Image
from torchvision.transforms.functional import to_tensor

from awswrangler import db, s3, _utils

_logger: logging.Logger = logging.getLogger(__name__)


class _BaseS3Dataset(Dataset):
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
    def __init__(self, path: Union[str, List[str]], suffix: str, boto3_session):
        super().__init__()
        self.session = _utils.ensure_session(session=boto3_session)
        self.paths: List[str] = s3._path2list(
            path=path,
            suffix=suffix,
            boto3_session=self.session
        )

    def __getitem__(self, index):
        path = self.paths[index]
        obj = self._fetch_obj(path)
        return [self.parser_fn(obj), self.label_fn(path)]

    def __len__(self):
        return len(self.paths)

    def _fetch_obj(self, path):
        bucket, key = _utils.parse_path(path=path)
        buff = BytesIO()
        client_s3: boto3.client = _utils.client(service_name="s3", session=self.session)
        client_s3.download_fileobj(Bucket=bucket, Key=key, Fileobj=buff)
        return buff.seek(0)

    def parser_fn(self, obj):
        pass

    def label_fn(self, obj):
        pass


class ImageS3Dataset(Dataset):

    @staticmethod
    def parser_fn(obj):
        image = Image.open('YOUR_PATH')
        tensor = to_tensor(image)
        tensor.unsqueeze_(0)
        return tensor

    @staticmethod
    def label_fn(obj):
        pass


class SQLDataset(IterableDataset):  # pylint: disable=too-few-public-methods,abstract-method
    """Pytorch Iterable SQL Dataset."""

    def __init__(self, sql: str, con: sqlalchemy.engine.Engine, chunksize: Optional[int] = None,):
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
        self.chunksize = chunksize

    def __iter__(self):
        """Iterate over the Dataset."""
        worker_info = torch.utils.data.get_worker_info()
        if worker_info is None:  # single-process data loading, return the full iterator
            pass
        else:  # in a worker process
            raise NotImplementedError()
        ret = db._read_sql_query(fn=db._records2numpy, sql=self.sql, con=self.con, chunksize=self.chunksize)
        if isinstance(ret, np.ndarray):
            ret = [ret]
        for ds in ret:
            for row in ds:
                yield torch.as_tensor(row, dtype=torch.float)
