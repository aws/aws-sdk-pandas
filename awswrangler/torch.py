"""PyTorch Module."""

import re
import logging
from io import BytesIO
from typing import Any, Iterator, List, Optional, Tuple, Union

import numpy as np  # type: ignore
import sqlalchemy  # type: ignore
import boto3  # type: ignore
import torch
from torch.utils.data.dataset import Dataset, IterableDataset

from awswrangler import db, _utils, s3

_logger: logging.Logger = logging.getLogger(__name__)


class _BaseS3Dataset(Dataset):
    """PyTorch Map-Style S3 Dataset."""

    def __init__(self, path: Union[str, List[str]], suffix: str, boto3_session):
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

        """
        super().__init__()
        self.session = _utils.ensure_session(session=boto3_session)
        self.paths: List[str] = s3._path2list(
            path=path,
            suffix=suffix,
            boto3_session=self.session,
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

    def label_fn(self, path):
        pass


class _S3PartitionedDataset(_BaseS3Dataset):

    def label_fn(self, path):
        return int(re.findall(r'/(.*?)=(.*?)/', path)[-1][1])


# class AudioS3Dataset(_S3PartitionedDataset):
#
#     def __init__(self):
#         """PyTorch S3 Audio Dataset.
#
#         Assumes audio files are stored with the following structure:
#
#         bucket
#         ├── class=0
#         │   ├── audio0.wav
#         │   └── audio1.wav
#         └── class=1
#             ├── audio2.wav
#             └── audio3.wav
#
#         Parameters
#         ----------
#         path : Union[str, List[str]]
#             S3 prefix (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
#         boto3_session : boto3.Session(), optional
#             Boto3 Session. The default boto3 session will be used if boto3_session receive None.
#
#         Returns
#         -------
#         torch.utils.data.Dataset
#
#         Examples
#         --------
#         >>> import awswrangler as wr
#         >>> import boto3
#         >>> ds = wr.torch.AudioS3Dataset('s3://bucket/path', boto3.Session())
#
#         """
#         super(AudioS3Dataset, self).__init__()
#         import torchaudio
#
#     def parser_fn(self, obj):
#         waveform, sample_rate = torchaudio.load(obj)
#         return waveform, sample_rate


# class LambdaS3Dataset(_BaseS3Dataset):
#     """PyTorch S3 Audio Dataset.
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
#     >>> parse_fn = lambda x: torch.tensor(x)
#     >>> label_fn = lambda x: x.split('.')[-1]
#     >>> ds = wr.torch.LambdaS3Dataset('s3://bucket/path', boto3.Session(), parse_fn=parse_fn, label_fn=label_fn)
#
#     """
#     def __init__(self, parse_fn, label_fn):
#         self._parse_fn = parse_fn
#         self._label_fn = label_fn
#
#     def label_fn(self, path):
#         return self._label_fn(path)
#
#     def parse_fn(self, obj):
#         return self._parse_fn(obj)
#
#
# class ImageS3Dataset(_S3PartitionedDataset):
#
#     def __init__(self):
#         """PyTorch Image S3 Dataset.
#
#         Assumes Images are stored with the following structure:
#
#         bucket
#         ├── class=0
#         │   ├── img0.jpeg
#         │   └── img1.jpeg
#         └── class=1
#             ├── img2.jpeg
#             └── img3.jpeg
#
#         Parameters
#         ----------
#         path : Union[str, List[str]]
#             S3 prefix (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
#         boto3_session : boto3.Session(), optional
#             Boto3 Session. The default boto3 session will be used if boto3_session receive None.
#
#         Returns
#         -------
#         torch.utils.data.Dataset
#
#         Examples
#         --------
#         >>> import awswrangler as wr
#         >>> import boto3
#         >>> ds = wr.torch.ImageS3Dataset('s3://bucket/path', boto3.Session())
#
#         """
#         super(ImageS3Dataset, self).__init__()
#         from PIL import Image
#         from torchvision.transforms.functional import to_tensor
#
#     def parser_fn(self, obj):
#         image = Image.open(obj)
#         tensor = to_tensor(image)
#         tensor.unsqueeze_(0)
#         return tensor


class SQLDataset(IterableDataset):  # pylint: disable=too-few-public-methods,abstract-method
    """Pytorch Iterable SQL Dataset."""

    def __init__(
        self,
        sql: str,
        con: sqlalchemy.engine.Engine,
        label_col: Optional[Union[int, str]] = None,
        chunksize: Optional[int] = None,
    ):
        """Pytorch Iterable SQL Dataset.

        Support for **Redshift**, **PostgreSQL** and **MySQL**.

        Parameters
        ----------
        sql : str
            Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
        con : sqlalchemy.engine.Engine
            SQLAlchemy Engine. Please use,
            wr.db.get_engine(), wr.db.get_redshift_temp_engine() or wr.catalog.get_engine()
        label_col : int, optional
            Label column number

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
        self._sql = sql
        self._con = con
        self._label_col = label_col
        self._chunksize = chunksize

    def __iter__(self) -> Union[Iterator[torch.Tensor], Iterator[Tuple[torch.Tensor, torch.Tensor]]]:
        """Iterate over the Dataset."""
        if torch.utils.data.get_worker_info() is not None:  # type: ignore
            raise NotImplementedError()
        db._validate_engine(con=self._con)
        with self._con.connect() as con:
            cursor: Any = con.execute(self._sql)
            if (self._label_col is not None) and isinstance(self._label_col, str):
                label_col: Optional[int] = list(cursor.keys()).index(self._label_col)
            else:
                label_col = self._label_col
            _logger.debug(f"label_col: {label_col}")
            return self._records2tensor(cursor=cursor, chunksize=self._chunksize, label_col=label_col)

    @staticmethod
    def _records2tensor(
        cursor: Any, chunksize: Optional[int] = None, label_col: Optional[int] = None
    ) -> Union[Iterator[torch.Tensor], Iterator[Tuple[torch.Tensor, torch.Tensor]]]:  # pylint: disable=unused-argument
        chunks: Iterator[Union[np.ndarray, Tuple[np.ndarray, np.ndarray]]]
        if chunksize is None:
            chunks = iter([SQLDataset._records2numpy(records=cursor.fetchall(), label_col=label_col)])
        else:
            chunks = db._iterate_cursor(  # pylint: disable=protected-access
                fn=SQLDataset._records2numpy, cursor=cursor, chunksize=chunksize, label_col=label_col
            )
        if label_col is None:
            for data in chunks:
                for data_row in data:
                    yield torch.as_tensor(data_row, dtype=torch.float)  # pylint: disable=no-member
        for data, label in chunks:
            for data_row, label_row in zip(data, label):
                ts_data: torch.Tensor = torch.as_tensor(data_row, dtype=torch.float)  # pylint: disable=no-member
                ts_label: torch.Tensor = torch.as_tensor(label_row, dtype=torch.float)  # pylint: disable=no-member
                yield ts_data, ts_label

    @staticmethod
    def _records2numpy(
        records: List[Tuple[Any]], label_col: Optional[int] = None
    ) -> Union[np.ndarray, Tuple[np.ndarray, np.ndarray]]:  # pylint: disable=unused-argument
        arr: np.ndarray = np.array(records, dtype=np.float)
        if label_col is None:
            return arr
        data: np.ndarray = np.concatenate([arr[:, :label_col], arr[:, (label_col + 1) :]], axis=1)  # noqa: E203
        label: np.ndarray = arr[:, label_col]
        return data, label
