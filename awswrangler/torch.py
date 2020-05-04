"""PyTorch Module."""
import io
import logging
import os
import pathlib
import re
from collections.abc import Iterable
from io import BytesIO
from typing import Any, Callable, Iterator, List, Optional, Tuple, Union

import boto3  # type: ignore
import numpy as np  # type: ignore
import sqlalchemy  # type: ignore
import torch  # type: ignore
import torchaudio  # type: ignore
from PIL import Image  # type: ignore
from torch.utils.data.dataset import Dataset, IterableDataset  # type: ignore
from torchvision.transforms.functional import to_tensor  # type: ignore

from awswrangler import _utils, db, s3

_logger: logging.Logger = logging.getLogger(__name__)


class _BaseS3Dataset:
    """PyTorch Amazon S3 Map-Style Dataset."""

    def __init__(
        self, path: Union[str, List[str]], suffix: Optional[str] = None, boto3_session: Optional[boto3.Session] = None
    ):
        r"""PyTorch Map-Style S3 Dataset.

        Parameters
        ----------
        path : Union[str, List[str]]
            S3 prefix (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
        suffix: str, optional
            S3 suffix filtering of object keys (i.e. suffix=".png" -> s3://\*.png).
        boto3_session : boto3.Session(), optional
            Boto3 Session. The default boto3 session will be used if boto3_session receive None.

        Returns
        -------
        torch.utils.data.Dataset

        """
        super().__init__()
        self._session = _utils.ensure_session(session=boto3_session)
        self._paths: List[str] = s3._path2list(  # pylint: disable=protected-access
            path=path, suffix=suffix, boto3_session=self._session
        )

    def _fetch_data(self, path: str) -> Any:
        """Add parquet and csv support."""
        bucket, key = _utils.parse_path(path=path)
        buff = BytesIO()
        client_s3: boto3.client = _utils.client(service_name="s3", session=self._session)
        client_s3.download_fileobj(Bucket=bucket, Key=key, Fileobj=buff)
        buff.seek(0)
        return buff

    @staticmethod
    def _load_data(data: io.BytesIO, path: str) -> Any:
        if path.endswith(".pt"):
            data = torch.load(data)
        elif path.endswith(".tar.gz") or path.endswith(".tgz"):  # pragma: no cover
            raise NotImplementedError("Tar loader not implemented!")
            # tarfile.open(fileobj=data)
            # tar = tarfile.open(fileobj=data)
            # for member in tar.getmembers():
        else:  # pragma: no cover
            raise NotImplementedError()

        return data


class _ListS3Dataset(_BaseS3Dataset, Dataset):
    """PyTorch Amazon S3 Map-Style List Dataset."""

    def __getitem__(self, index):
        path = self._paths[index]
        data = self._fetch_data(path)
        return [self._data_fn(data), self._label_fn(path)]

    def __len__(self):
        return len(self._paths)

    def _data_fn(self, data) -> Any:  # pragma: no cover
        raise NotImplementedError()

    def _label_fn(self, path: str) -> Any:  # pragma: no cover
        raise NotImplementedError()


class _S3PartitionedDataset(_ListS3Dataset):
    """PyTorch Amazon S3 Map-Style Partitioned Dataset."""

    def _label_fn(self, path: str) -> torch.Tensor:
        label = int(re.findall(r"/(.*?)=(.*?)/", path)[-1][1])
        return torch.tensor([label])  # pylint: disable=not-callable

    def _data_fn(self, data) -> Any:  # pragma: no cover
        raise NotImplementedError()


# class S3FilesDataset(_BaseS3Dataset, Dataset):
#     """PyTorch Amazon S3 Files Map-Style Dataset."""
#
#     def __init__(
#         self, path: Union[str, List[str]], suffix: Optional[str] = None, boto3_session: Optional[boto3.Session] = None
#     ):
#         """PyTorch S3 Files Map-Style Dataset.
#
#         Each file under Amazon S3 path would be handled as a tensor or batch of tensors.
#
#         Note
#         ----
#         All files will be loaded to memory since random access is needed.
#
#         Parameters
#         ----------
#         path : Union[str, List[str]]
#             S3 prefix (e.g. s3://bucket/prefix) or
#             list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
#         boto3_session : boto3.Session(), optional
#             Boto3 Session. The default boto3 session will be used if boto3_session receive None.
#
#         Returns
#         -------
#         torch.utils.data.Dataset
#
#         """
#         super(S3FilesDataset, self).__init__(path, suffix, boto3_session)
#         self._download_files()
#
#     def _download_files(self) -> None:
#         self._data = []
#         for path in self._paths:
#             data = self._fetch_data(path)
#             data = self._load_data(data, path)
#             self._data.append(data)
#
#         self.data = torch.cat(self._data, dim=0)
#
#     def __getitem__(self, index):
#         return self._data[index]
#
#     def __len__(self):
#         return len(self._data)


class LambdaS3Dataset(_ListS3Dataset):
    """PyTorch Amazon S3 Lambda Map-Style Dataset."""

    def __init__(
        self,
        path: Union[str, List[str]],
        data_fn: Callable,
        label_fn: Callable,
        suffix: Optional[str] = None,
        boto3_session: Optional[boto3.Session] = None,
    ):
        r"""PyTorch Amazon S3 Lambda Dataset.

        Parameters
        ----------
        path : Union[str, List[str]]
            S3 prefix (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
        data_fn: Callable
            Function that receives a io.BytesIO object and returns a torch.Tensor
        label_fn: Callable
            Function that receives object path (str) and return a torch.Tensor
        suffix: str, optional
            S3 suffix filtering of object keys (i.e. suffix=".png" -> s3://\*.png).
        boto3_session : boto3.Session(), optional
            Boto3 Session. The default boto3 session will be used if boto3_session receive None.

        Returns
        -------
        torch.utils.data.Dataset

        Examples
        --------
        >>> import re
        >>> import torch
        >>> import awswrangler as wr
        >>> ds = wr.torch.LambdaS3Dataset(
        >>>     's3://bucket/path',
        >>>     data_fn=lambda x: torch.load(x),
        >>>     label_fn=lambda x: torch.Tensor(int(re.findall(r"/class=(.*?)/", x)[-1])),
        >>> )

        """
        super(LambdaS3Dataset, self).__init__(path, suffix, boto3_session)
        self._data_func = data_fn
        self._label_func = label_fn

    def _label_fn(self, path: str) -> torch.Tensor:
        return self._label_func(path)

    def _data_fn(self, data) -> torch.Tensor:
        return self._data_func(data)


class AudioS3Dataset(_S3PartitionedDataset):
    """PyTorch S3 Audio Dataset."""

    def __init__(
        self,
        path: Union[str, List[str]],
        cache_dir: str = "/tmp/",
        suffix: Optional[str] = None,
        boto3_session: Optional[boto3.Session] = None,
    ):
        r"""PyTorch Amazon S3 Audio Dataset.

        Read individual WAV audio files stores in Amazon S3 and return
        them as torch tensors.

        Note
        ----
        This dataset assumes audio files are stored with the following structure:


        ::

            bucket
            ├── class=0
            │   ├── audio0.wav
            │   └── audio1.wav
            └── class=1
                ├── audio2.wav
                └── audio3.wav

        Parameters
        ----------
        path : Union[str, List[str]]
            S3 prefix (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
        suffix: str, optional
            S3 suffix filtering of object keys (i.e. suffix=".png" -> s3://\*.png).
        boto3_session : boto3.Session(), optional
            Boto3 Session. The default boto3 session will be used if boto3_session receive None.

        Returns
        -------
        torch.utils.data.Dataset

        Examples
        --------
        Create a Audio S3 Dataset

        >>> import awswrangler as wr
        >>> ds = wr.torch.AudioS3Dataset('s3://bucket/path')


        Training a Model

        >>> criterion = CrossEntropyLoss().to(device)
        >>> opt = SGD(model.parameters(), 0.025)
        >>> loader = DataLoader(dataset, batch_size=batch_size, num_workers=num_workers)
        >>>
        >>> for epoch in range(epochs):
        >>>
        >>>     correct = 0
        >>>     model.train()
        >>>     for i, (inputs, labels) in enumerate(loader):
        >>>
        >>>         # Forward Pass
        >>>         outputs = model(inputs)
        >>>
        >>>         # Backward Pass
        >>>         loss = criterion(outputs, labels)
        >>>         loss.backward()
        >>>         opt.step()
        >>>         opt.zero_grad()
        >>>
        >>>         # Accuracy
        >>>         _, predicted = torch.max(outputs.data, 1)
        >>>         correct += (predicted == labels).sum().item()
        >>>         accuracy = 100 * correct / ((i+1) * batch_size)
        >>>         print(f'batch: {i} loss: {loss.mean().item():.4f} acc: {accuracy:.2f}')

        """
        super(AudioS3Dataset, self).__init__(path, suffix, boto3_session)
        self._cache_dir: str = cache_dir[:-1] if cache_dir.endswith("/") else cache_dir

    def _data_fn(self, filename: str) -> Tuple[Any, Any]:  # pylint: disable=arguments-differ
        waveform, sample_rate = torchaudio.load(filename)
        os.remove(path=filename)
        return waveform, sample_rate

    def _fetch_data(self, path: str) -> str:
        bucket, key = _utils.parse_path(path=path)
        filename: str = f"{self._cache_dir}/{bucket}/{key}"
        pathlib.Path(filename).parent.mkdir(parents=True, exist_ok=True)
        client_s3 = _utils.client(service_name="s3", session=self._session)
        client_s3.download_file(Bucket=bucket, Key=key, Filename=filename)
        return filename


class ImageS3Dataset(_S3PartitionedDataset):
    """PyTorch Amazon S3 Image Dataset."""

    def __init__(self, path: Union[str, List[str]], suffix: str, boto3_session: boto3.Session):
        r"""PyTorch Amazon S3 Image Dataset.

        ImageS3Dataset assumes images are patitioned (within class=<value> folders) in Amazon S3.
        Each lisited object will be loaded by default Pillow library.

        Note
        ----
        Assumes Images are stored with the following structure:


        ::

            bucket
            ├── class=0
            │   ├── img0.jpeg
            │   └── img1.jpeg
            └── class=1
                ├── img2.jpeg
                └── img3.jpeg

        Parameters
        ----------
        path : Union[str, List[str]]
            S3 prefix (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
        suffix: str, optional
            S3 suffix filtering of object keys (i.e. suffix=".png" -> s3://\*.png).
        boto3_session : boto3.Session(), optional
            Boto3 Session. The default boto3 session will be used if boto3_session receive None.

        Returns
        -------
        torch.utils.data.Dataset

        Examples
        --------
        >>> import awswrangler as wr
        >>> ds = wr.torch.ImageS3Dataset('s3://bucket/path')

        """
        super(ImageS3Dataset, self).__init__(path, suffix, boto3_session)

    def _data_fn(self, data: io.BytesIO) -> Any:
        image = Image.open(data)
        tensor = to_tensor(image)
        return tensor


class S3IterableDataset(IterableDataset, _BaseS3Dataset):  # pylint: disable=abstract-method
    r"""PyTorch Amazon S3 Iterable Dataset.

    Parameters
    ----------
    path : Union[str, List[str]]
        S3 prefix (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
    suffix: str, optional
        S3 suffix filtering of object keys (i.e. suffix=".png" -> s3://\*.png).
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    torch.utils.data.Dataset

    Examples
    --------
    >>> import awswrangler as wr
    >>> ds = wr.torch.S3IterableDataset('s3://bucket/path')

    """

    def __iter__(self) -> Union[Iterator[torch.Tensor], Iterator[Tuple[torch.Tensor, torch.Tensor]]]:
        """Iterate over data returning tensors or expanding Iterables."""
        for path in self._paths:
            data = self._fetch_data(path)
            data = self._load_data(data, path)

            if isinstance(data, torch.Tensor):
                pass
            elif isinstance(data, Iterable) and all([isinstance(d, torch.Tensor) for d in data]):
                data = zip(*data)
            else:  # pragma: no cover
                raise NotImplementedError(f"ERROR: Type: {type(data)} has not been implemented!")
            for d in data:
                yield d


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
            Label column number.
        chunksize : int, optional
            The chunksize determines que number of rows to be retrived from the database at each time.

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
            raise NotImplementedError()  # pragma: no cover
        db._validate_engine(con=self._con)  # pylint: disable=protected-access
        with self._con.connect() as con:
            cursor: Any = con.execute(self._sql)
            if (self._label_col is not None) and isinstance(self._label_col, str):
                label_col: Optional[int] = list(cursor.keys()).index(self._label_col)
            else:
                label_col = self._label_col
            _logger.debug("label_col: %s", label_col)
            if self._chunksize is None:
                return SQLDataset._records2tensor(records=cursor.fetchall(), label_col=label_col)
            return self._iterate_cursor(cursor=cursor, chunksize=self._chunksize, label_col=label_col)

    @staticmethod
    def _iterate_cursor(
        cursor: Any, chunksize: int, label_col: Optional[int] = None
    ) -> Union[Iterator[torch.Tensor], Iterator[Tuple[torch.Tensor, torch.Tensor]]]:
        while True:
            records = cursor.fetchmany(chunksize)
            if not records:
                break
            yield from SQLDataset._records2tensor(records=records, label_col=label_col)

    @staticmethod
    def _records2tensor(
        records: List[Tuple[Any]], label_col: Optional[int] = None
    ) -> Union[Iterator[torch.Tensor], Iterator[Tuple[torch.Tensor, torch.Tensor]]]:  # pylint: disable=unused-argument
        for row in records:
            if label_col is None:
                arr_data: np.ndarray = np.array(row, dtype=np.float)
                yield torch.as_tensor(arr_data, dtype=torch.float)  # pylint: disable=no-member
            else:
                arr_data = np.array(row[:label_col] + row[label_col + 1 :], dtype=np.float)  # noqa: E203
                arr_label: np.ndarray = np.array(row[label_col], dtype=np.long)
                ts_data: torch.Tensor = torch.as_tensor(arr_data, dtype=torch.float)  # pylint: disable=no-member
                ts_label: torch.Tensor = torch.as_tensor(arr_label, dtype=torch.long)  # pylint: disable=no-member
                yield ts_data, ts_label
