"""Amazon S3 Module."""

import gzip
import multiprocessing as mp
from io import BytesIO
from logging import Logger, getLogger
from time import sleep
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Tuple
from uuid import uuid4

import pandas as pd  # type: ignore
import pyarrow as pa  # type: ignore
from boto3.s3.transfer import TransferConfig  # type: ignore
from botocore.exceptions import ClientError  # type: ignore
from pandas.io.common import infer_compression  # type: ignore
from pyarrow import parquet as pq  # type: ignore

from awswrangler import utils
from awswrangler.exceptions import InvalidCompression, S3WaitObjectTimeout

if TYPE_CHECKING:  # pragma: no cover
    from awswrangler.session import Session, _SessionPrimitives
    import boto3  # type: ignore

logger: Logger = getLogger(__name__)


class S3:
    """Amazon S3 Class."""
    def __init__(self, session: "Session"):
        """Amazon S3 Class Constructor.

        Note
        ----
        Don't use it directly, call through a Session().
        e.g. wr.SERVICE.FUNCTION() (Default Session)

        Parameters
        ----------
        session : awswrangler.Session()
            Wrangler's Session

        """
        self._session: "Session" = session

    def does_object_exists(self, path: str) -> bool:
        """Check if object exists on S3.

        Parameters
        ----------
        path: str
            S3 path (e.g. s3://bucket/key).

        Returns
        -------
        bool
            True if exists, False otherwise.

        Examples
        --------
        >>> import awswrangler as wr
        >>> wr.s3.does_object_exists("s3://bucket/key_real")
        True
        >>> wr.s3.does_object_exists("s3://bucket/key_unreal")
        False

        """
        bucket: str
        key: str
        bucket, key = path.replace("s3://", "").split("/", 1)
        try:
            self._session.s3_client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as ex:
            if ex.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                return False
            raise ex  # pragma: no cover

    def wait_object_exists(self, path: str, polling_sleep: float = 0.1, timeout: Optional[float] = 10.0) -> None:
        """Wait object exists on S3.

        Parameters
        ----------
        path : str
            S3 path (e.g. s3://bucket/key).
        polling_sleep : float
            Sleep between each retry (Seconds).
        timeout : float, optional
            Timeout (Seconds).

        Returns
        -------
        None
            None

        Raises
        ------
        S3WaitObjectTimeout
            Raised in case of timeout.

        Examples
        --------
        >>> import awswrangler as wr
        >>> wr.s3.wait_object_exists("s3://bucket/key_expected")

        """
        time_acc: float = 0.0
        while self.does_object_exists(path=path) is False:
            sleep(polling_sleep)
            if timeout is not None:
                time_acc += polling_sleep
                if time_acc >= timeout:
                    raise S3WaitObjectTimeout(f"Waited for {path} for {time_acc} seconds")

    @staticmethod
    def parse_path(path: str) -> Tuple[str, str]:
        """Split a full S3 path in bucket and key strings.

        "s3://bucket/key" -> ("bucket", "key")

        Parameters
        ----------
        path : str
            S3 path (e.g. s3://bucket/key).

        Returns
        -------
        Tuple[str, str]
            Tuple of bucket and key strings

        Examples
        --------
        >>> import awswrangler as wr
        >>> bucket, key = wr.s3.parse_path("s3://bucket/key")

        """
        parts = path.replace("s3://", "").split("/", 1)
        bucket: str = parts[0]
        key: str = ""
        if len(parts) == 2:
            key = key if parts[1] is None else parts[1]
        return bucket, key

    def get_bucket_region(self, bucket: str) -> str:
        """Get bucket region.

        Parameters
        ----------
        bucket : str
            Bucket name.

        Returns
        -------
        str
            Region code (e.g. "us-east-1").

        Examples
        --------
        >>> import awswrangler as wr
        >>> region = wr.s3.get_bucket_region("bucket-name")

        """
        logger.debug(f"bucket: {bucket}")
        region: str = self._session.s3_client.get_bucket_location(Bucket=bucket)["LocationConstraint"]
        region = "us-east-1" if region is None else region
        logger.debug(f"region: {region}")
        return region

    def list_objects(self, path: str) -> List[str]:
        """List Amazon S3 objects from a prefix.

        Parameters
        ----------
        path : str
            S3 path (e.g. s3://bucket/prefix).

        Returns
        -------
        List[str]
            List of objects paths.

        Examples
        --------
        >>> import awswrangler as wr
        >>> wr.s3.list_objects("s3://bucket/prefix")
        ["s3://bucket/prefix0", "s3://bucket/prefix1", "s3://bucket/prefix2"]

        """
        paginator = self._session.s3_client.get_paginator("list_objects_v2")
        bucket: str
        prefix: str
        bucket, prefix = self.parse_path(path=path)
        response_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix, PaginationConfig={"PageSize": 1000})
        paths: List[str] = []
        for page in response_iterator:
            if page.get("Contents") is not None:
                for content in page.get("Contents"):
                    if (content is not None) and ("Key" in content):
                        key: str = content["Key"]
                        paths.append(f"s3://{bucket}/{key}")
        return paths

    def delete_objects_list(self, paths: List[str], parallel: bool = True) -> None:
        """Delete all listed Amazon S3 objects.

        Note
        ----
        In case of `parallel=True` the number of process that will be spawned will be get from os.cpu_count().

        Parameters
        ----------
        paths : str
            S3 path (e.g. s3://bucket/prefix).
        parallel : bool
            True to enable parallel requests, False to disable.

        Returns
        -------
        None
            None.

        Examples
        --------
        >>> import awswrangler as wr
        >>> wr.s3.delete_objects_list(["s3://bucket/key0", "s3://bucket/key1"])

        """
        if len(paths) < 1:
            return
        cpus: int = utils.get_cpu_count(parallel=parallel)
        buckets: Dict[str, List[str]] = self._split_paths_by_bucket(paths=paths)
        for bucket, keys in buckets.items():
            if cpus == 1:
                self._delete_objects(s3_client=self._session.s3_client, bucket=bucket, keys=keys)
            else:
                chunks: List[List[str]] = utils.chunkify(lst=keys, num_chunks=cpus)
                procs: List[mp.Process] = []
                for chunk in chunks:
                    proc: mp.Process = mp.Process(
                        target=self._delete_objects_remote,
                        args=(
                            self._session.primitives,
                            bucket,
                            chunk,
                        ),
                    )
                    proc.daemon = False
                    proc.start()
                    procs.append(proc)
                for proc in procs:
                    proc.join()

    @staticmethod
    def _delete_objects_remote(session_primitives: "_SessionPrimitives", bucket: str, keys: List[str]) -> None:
        session: "Session" = session_primitives.build_session()
        s3_client: boto3.client = session.s3_client
        S3._delete_objects(s3_client=s3_client, bucket=bucket, keys=keys)

    @staticmethod
    def _delete_objects(s3_client: "boto3.client", bucket: str, keys: List[str]) -> None:
        chunks: List[List[str]] = utils.chunkify(lst=keys, max_length=1_000)
        logger.debug(f"len(chunks): {len(chunks)}")
        for chunk in chunks:
            batch: List[Dict[str, str]] = [{"Key": key} for key in chunk]
            s3_client.delete_objects(Bucket=bucket, Delete={"Objects": batch})

    @staticmethod
    def _split_paths_by_bucket(paths: List[str]) -> Dict[str, List[str]]:
        buckets: Dict[str, List[str]] = {}
        bucket: str
        key: str
        for path in paths:
            bucket, key = S3.parse_path(path=path)
            if bucket not in buckets:
                buckets[bucket] = []
            buckets[bucket].append(key)
        return buckets

    def delete_objects_prefix(self, path: str, parallel: bool = True) -> None:
        """Delete all Amazon S3 objects under the received prefix.

        Note
        ----
        In case of `parallel=True` the number of process that will be spawned will be get from os.cpu_count().

        Parameters
        ----------
        path : str
            S3 prefix path (e.g. s3://bucket/prefix).
        parallel : bool
            True to enable parallel requests, False to disable.

        Returns
        -------
        None
            None.

        Examples
        --------
        >>> import awswrangler as wr
        >>> wr.s3.delete_objects_prefix(path="s3://bucket/prefix"])

        """
        paths: List[str] = self.list_objects(path=path)
        self.delete_objects_list(paths=paths, parallel=parallel)

    def _writer_factory(self,
                        file_writer: Callable,
                        df: pd.DataFrame,
                        path: str,
                        filename: Optional[str] = None,
                        partition_cols: Optional[List[str]] = None,
                        mode: str = "append",
                        parallel: bool = True,
                        **pd_kwargs) -> List[str]:
        cpus: int = utils.get_cpu_count(parallel=parallel)
        paths: List[str] = []
        path = path if path[-1] == "/" else f"{path}/"
        if filename is not None:
            paths.append(file_writer(df=df, path=path, filename=filename, cpus=cpus, **pd_kwargs))
        else:
            if (mode == "overwrite") or ((mode == "partition_upsert") and (not partition_cols)):
                self.delete_objects_prefix(path=path, parallel=parallel)
            if not partition_cols:
                paths.append(file_writer(df=df, path=path, cpus=cpus, **pd_kwargs))
            else:
                for keys, subgroup in df.groupby(by=partition_cols, observed=True):
                    subgroup = subgroup.drop(partition_cols, axis="columns")
                    keys = (keys, ) if not isinstance(keys, tuple) else keys
                    subdir = "/".join([f"{name}={val}" for name, val in zip(partition_cols, keys)])
                    prefix: str = f"{path}{subdir}/"
                    if mode == "partition_upsert":
                        self.delete_objects_prefix(path=prefix, parallel=parallel)
                    paths.append(file_writer(df=subgroup, path=prefix, cpus=cpus, **pd_kwargs))
        return paths

    def to_csv(self,
               df: pd.DataFrame,
               path: str,
               filename: Optional[str] = None,
               partition_cols: Optional[List[str]] = None,
               mode: str = "append",
               parallel: bool = True,
               **pd_kwargs) -> List[str]:
        """Write CSV file(s) on Amazon S3.

        Note
        ----
        In case of `parallel=True` the number of process that will be spawned will be get from os.cpu_count().

        Parameters
        ----------
        df: pandas.DataFrame
            Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
        path : str
            S3 path (e.g. s3://bucket/prefix).
        filename : str, optional
            The default behavior (`filename=None`) uses random names, but if you prefer pass a filename.
            It will disable the partitioning.
        partition_cols: List[str], optional
            List of column names that will be used to create partitions.
        mode: str
            "append", "overwrite", "partition_upsert"
        parallel : bool
            True to enable parallel requests, False to disable.
        pd_kwargs:
            keyword arguments forwarded to pandas.DataFrame.to_csv()
            https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_csv.html

        Returns
        -------
        List[str]
            List with the s3 paths created.

        Examples
        --------
        Writing with filename

        >>> import awswrangler as wr
        >>> import pandas as pd
        >>> wr.s3.to_csv(
        ...     df=pd.DataFrame({"col": [1, 2, 3]}),
        ...     path="s3://bucket/prefix",
        ...     filename="my_file.csv"
        ... )

        Writing partitioned dataset

        >>> import awswrangler as wr
        >>> import pandas as pd
        >>> wr.s3.to_csv(
        ...     df=pd.DataFrame({
        ...         "col": [1, 2, 3],
        ...         "col2": ["A", "A", "B"]
        ...     }),
        ...     path="s3://bucket/prefix",
        ...     partition_cols=["col2"]
        ... )

        """
        return self._writer_factory(file_writer=self._write_csv_file,
                                    df=df,
                                    path=path,
                                    filename=filename,
                                    partition_cols=partition_cols,
                                    mode=mode,
                                    parallel=parallel,
                                    **pd_kwargs)

    def _write_csv_file(self,
                        df: pd.DataFrame,
                        path: str,
                        cpus: int,
                        filename: Optional[str] = None,
                        **pd_kwargs) -> str:
        compression: Optional[str] = pd_kwargs.get("compression")
        if compression is None:
            compression_ext: str = ""
        elif compression == "gzip":
            compression_ext = ".gz"
            pd_kwargs["compression"] = None
        else:
            raise InvalidCompression(f"{compression} is invalid, please use gzip.")  # pragma: no cover
        file_path: str = f"{path}{uuid4().hex}{compression_ext}.csv" if filename is None else f"{path}{filename}"
        if compression is None:
            file_obj: BytesIO = BytesIO(initial_bytes=bytes(df.to_csv(None, **pd_kwargs), "utf-8"))
        else:
            file_obj = BytesIO()
            with gzip.open(file_obj, 'wb') as f:
                f.write(df.to_csv(None, **pd_kwargs).encode(encoding="utf-8"))
            file_obj.seek(0)
        self._upload_fileobj(file_obj=file_obj, path=file_path, cpus=cpus)
        return file_path

    def to_parquet(self,
                   df: pd.DataFrame,
                   path: str,
                   filename: Optional[str] = None,
                   partition_cols: Optional[List[str]] = None,
                   mode: str = "append",
                   parallel: bool = True,
                   **pd_kwargs) -> List[str]:
        """Write Parquet file(s) on Amazon S3.

        Note
        ----
        In case of `parallel=True` the number of process that will be spawned will be get from os.cpu_count().

        Parameters
        ----------
        df: pandas.DataFrame
            Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
        path : str
            S3 path (e.g. s3://bucket/prefix).
        filename : str, optional
            The default behavior (`filename=None`) uses random names, but if you prefer pass a filename.
            It will disable the partitioning.
        partition_cols: List[str], optional
            List of column names that will be used to create partitions.
        mode: str
            "append", "overwrite", "partition_upsert"
        parallel : bool
            True to enable parallel requests, False to disable.
        pd_kwargs:
            keyword arguments forwarded to pandas.DataFrame.to_parquet()
            https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_parquet.html

        Returns
        -------
        List[str]
            List with the s3 paths created.

        Examples
        --------
        Writing with filename

        >>> import awswrangler as wr
        >>> import pandas as pd
        >>> wr.s3.to_parquet(
        ...     df=pd.DataFrame({"col": [1, 2, 3]}),
        ...     path="s3://bucket/prefix",
        ...     filename="my_file.parquet"
        ... )

        Writing partitioned dataset

        >>> import awswrangler as wr
        >>> import pandas as pd
        >>> wr.s3.to_parquet(
        ...     df=pd.DataFrame({
        ...         "col": [1, 2, 3],
        ...         "col2": ["A", "A", "B"]
        ...     }),
        ...     path="s3://bucket/prefix",
        ...     partition_cols=["col2"]
        ... )

        """
        return self._writer_factory(file_writer=self._write_parquet_file,
                                    df=df,
                                    path=path,
                                    filename=filename,
                                    partition_cols=partition_cols,
                                    mode=mode,
                                    parallel=parallel,
                                    **pd_kwargs)

    def _write_parquet_file(self,
                            df: pd.DataFrame,
                            path: str,
                            cpus: int,
                            filename: Optional[str] = None,
                            compression: Optional[str] = "snappy",
                            **pd_kwargs) -> str:
        if compression is None:
            compression_ext: str = ""
        elif compression == "snappy":
            compression_ext = ".snappy"
        elif compression == "gzip":
            compression_ext = ".gz"
        else:
            raise InvalidCompression(f"{compression} is invalid, please use snappy or gzip.")  # pragma: no cover
        file_path: str = f"{path}{uuid4().hex}{compression_ext}.parquet" if filename is None else f"{path}{filename}"
        file_obj: BytesIO = BytesIO()
        table: pa.Table = pa.Table.from_pandas(df=df, nthreads=cpus, preserve_index=False, safe=False)
        pq.write_table(table=table,
                       where=file_obj,
                       coerce_timestamps="ms",
                       compression=compression,
                       flavor="spark",
                       **pd_kwargs)
        del table
        file_obj.seek(0)
        self._upload_fileobj(file_obj=file_obj, path=file_path, cpus=cpus)
        return file_path

    def _upload_fileobj(self, file_obj: BytesIO, path: str, cpus: int) -> None:
        bucket: str
        key: str
        bucket, key = self.parse_path(path=path)
        if cpus > 1:
            config: TransferConfig = TransferConfig(max_concurrency=cpus, use_threads=True)
        else:
            config = TransferConfig(max_concurrency=1, use_threads=False)
        self._session.s3_client.upload_fileobj(Fileobj=file_obj, Bucket=bucket, Key=key, Config=config)
        file_obj.close()

    def _download_fileobj(self, path: str, cpus: int) -> BytesIO:
        bucket: str
        key: str
        bucket, key = self.parse_path(path=path)
        if cpus > 1:
            config: TransferConfig = TransferConfig(max_concurrency=cpus, use_threads=True)
        else:
            config = TransferConfig(max_concurrency=1, use_threads=False)
        file_obj: BytesIO = BytesIO()
        self._session.s3_client.download_fileobj(Fileobj=file_obj, Bucket=bucket, Key=key, Config=config)
        file_obj.seek(0)
        return file_obj

    def read_csv(self, path: str, parallel: bool = True, **pd_kwargs) -> pd.DataFrame:
        """Read CSV file from Amazon S3 to Pandas DataFrame.

        Note
        ----
        In case of `parallel=True` the number of process that will be spawned will be get from os.cpu_count().

        Parameters
        ----------
        path : str
            S3 path (e.g. s3://bucket/filename.csv).
        parallel : bool
            True to enable parallel requests, False to disable.
        pd_kwargs:
            keyword arguments forwarded to pandas.read_csv().
            https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html

        Returns
        -------
        pandas.DataFrame
            Pandas DataFrame.

        Examples
        --------
        >>> import awswrangler as wr
        >>> df = wr.s3.read_csv(path="s3://bucket/filename.csv")

        """
        cpus: int = utils.get_cpu_count(parallel=parallel)
        if pd_kwargs.get('compression', 'infer') == 'infer':
            pd_kwargs['compression'] = infer_compression(path, compression='infer')
        file_obj: BytesIO = self._download_fileobj(path=path, cpus=cpus)
        df: pd.DataFrame = pd.read_csv(file_obj, **pd_kwargs)
        file_obj.close()
        return df

    def read_parquet(self, path: str, parallel: bool = True, **pd_kwargs) -> pd.DataFrame:
        """Read Apache Parquet file from Amazon S3 to Pandas DataFrame.

        Note
        ----
        In case of `parallel=True` the number of process that will be spawned will be get from os.cpu_count().

        Parameters
        ----------
        path : str
            S3 path (e.g. s3://bucket/filename.parquet).
        parallel : bool
            True to enable parallel requests, False to disable.
        pd_kwargs:
            keyword arguments forwarded to pandas.read_parquet().
            https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_parquet.html

        Returns
        -------
        pandas.DataFrame
            Pandas DataFrame.

        Examples
        --------
        >>> import awswrangler as wr
        >>> df = wr.s3.read_parquet(path="s3://bucket/filename.parquet")

        """
        cpus: int = utils.get_cpu_count(parallel=parallel)
        file_obj: BytesIO = self._download_fileobj(path=path, cpus=cpus)
        pd_kwargs["use_threads"] = True if cpus > 1 else False
        table: pa.Table = pq.read_table(source=file_obj, **pd_kwargs)
        file_obj.close()
        del file_obj
        df: pd.DataFrame = table.to_pandas(split_blocks=True, self_destruct=True)
        del table  # not necessary, but a good practice
        return df
