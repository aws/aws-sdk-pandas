"""Amazon S3 Module."""

import multiprocessing as mp
from logging import INFO, Logger, getLogger
from math import ceil
from time import sleep
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

import s3fs  # type: ignore
import tenacity  # type: ignore
from boto3 import client, resource  # type: ignore
from botocore.exceptions import ClientError  # type: ignore
from botocore.exceptions import ConnectTimeoutError, HTTPClientError

from awswrangler.exceptions import S3WaitObjectTimeout
from awswrangler.utils import calculate_bounders, wait_process_release

if TYPE_CHECKING:
    from awswrangler.session import Session, SessionPrimitives

logger: Logger = getLogger(__name__)


def mkdir_if_not_exists(fs: s3fs.S3FileSystem, path: str) -> None:
    """
    Create a directory if not exists.

    :param fs: Files-system
    :param path: Directory path
    :return: None
    """
    if fs._isfilestore() and not fs.exists(path):
        try:
            fs.mkdir(path)
        except OSError:
            assert fs.exists(path)


def get_fs(session_primitives: Optional["SessionPrimitives"] = None) -> s3fs.S3FileSystem:
    """
    Get the file-system abstraction for S3.

    :param session_primitives: Wrangler Session Primitives
    :return: file-system abstraction
    """
    aws_access_key_id, aws_secret_access_key, profile_name = None, None, None
    args: Dict[str, Any] = {}

    if session_primitives is not None:
        if session_primitives.aws_access_key_id is not None:
            aws_access_key_id = session_primitives.aws_access_key_id
        if session_primitives.aws_secret_access_key is not None:
            aws_secret_access_key = session_primitives.aws_secret_access_key
        if session_primitives.profile_name is not None:
            profile_name = session_primitives.profile_name
        if session_primitives.botocore_max_retries is not None:
            args["config_kwargs"] = {"retries": {"max_attempts": session_primitives.botocore_max_retries}}
        if session_primitives.s3_additional_kwargs is not None:
            args["s3_additional_kwargs"] = session_primitives.s3_additional_kwargs

    if profile_name:
        args["profile_name"] = profile_name
    elif aws_access_key_id and aws_secret_access_key:
        args["key"] = aws_access_key_id,
        args["secret"] = aws_secret_access_key

    args["default_cache_type"] = "none"
    args["default_fill_cache"] = False
    fs = s3fs.S3FileSystem(**args)
    return fs


class S3:
    """Amazon S3 Class."""
    def __init__(self, session: "Session"):
        """
        Amazon S3 Class Constructor.

        Don't use it directly, call through a Session().
        e.g. wr.redshift.your_method()

        :param session: awswrangler.Session()
        """
        self._session: "Session" = session
        self._client_s3: client = session.boto3_session.client(service_name="s3",
                                                               use_ssl=True,
                                                               config=session.botocore_config)

    @tenacity.retry(retry=tenacity.retry_if_exception_type(exception_types=(ClientError, HTTPClientError,
                                                                            ConnectTimeoutError)),
                    wait=tenacity.wait_random_exponential(multiplier=0.5),
                    stop=tenacity.stop_after_attempt(max_attempt_number=10),
                    reraise=True,
                    after=tenacity.after_log(logger, INFO))
    def does_object_exists(self, path: str) -> bool:
        """
        Check if object exists on S3.

        :param path: S3 path (e.g. s3://...)
        :return: boolean
        """
        bucket: str
        key: str
        bucket, key = path.replace("s3://", "").split("/", 1)
        try:
            self._client_s3.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as ex:
            if ex.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                return False
            raise ex

    def wait_object_exists(self, path: str, polling_sleep: float = 0.1, timeout: Optional[float] = 10.0) -> None:
        """
        Wait object exists on S3.

        :param path: S3 path (e.g. s3://...)
        :param polling_sleep: Milliseconds
        :param timeout: Timeout (seconds)
        :return: None
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
        """
        Split a full S3 path in bucket and key.

        "s3://bucket/key" -> ("bucket", "key")

        :param path: Full S3 Path (e.g. "s3://bucket/key")
        :return: Tuple of bucket and key
        """
        bucket: str
        bucket, path = path.replace("s3://", "").split("/", 1)
        if path is None:
            path = ""
        return bucket, path

    def delete_objects(self, path: str, procs_io_bound: Optional[int] = None) -> None:
        """
        Delete all objects in the received S3 path.

        :param path: S3 path (e.g. "s3://bucket/path")
        :param procs_io_bound: Number of processes to be used for I/O bound operations
        :return: None
        """
        procs_io_bound = procs_io_bound if procs_io_bound is not None else self._session.procs_io_bound if self._session.procs_io_bound is not None else 1
        bucket, path = self.parse_path(path=path)
        procs: List[mp.Process] = []
        args: Dict[str, Any] = {"Bucket": bucket, "MaxKeys": 1000, "Prefix": path}
        logger.debug(f"Arguments: \n{args}")
        next_continuation_token: Optional[str] = ""
        while next_continuation_token is not None:
            res: Dict = self._client_s3.list_objects_v2(**args)
            if res.get("Contents") is None:
                break
            keys: List[Dict[str, str]] = [{
                "Key": x.get("Key")
            } for x in res.get("Contents") if "Key" in x]  # type: ignore
            logger.debug(f"Number of listed keys: {len(keys)}")
            next_continuation_token = res.get("NextContinuationToken")
            if next_continuation_token:
                args["ContinuationToken"] = next_continuation_token
                proc: mp.Process = mp.Process(
                    target=self._delete_objects_batch,
                    args=(self._session.primitives, bucket, keys),
                )
                proc.daemon = False
                proc.start()
                procs.append(proc)
                if len(procs) == procs_io_bound:
                    wait_process_release(procs)
            else:
                logger.debug(f"Starting last delete call...")
                self._delete_objects_batch(self._session.primitives, bucket, keys)
        logger.debug(f"Waiting final processes...")
        for proc in procs:
            proc.join()

    def delete_listed_objects(self, objects_paths: List[str], procs_io_bound: Optional[int] = None) -> None:
        """
        Delete a list of S3 objects (Parallel).

        :param objects_paths: List of S3 paths (e.g. ["s3://...", "s3://..."])
        :param procs_io_bound: Number of processes to be used for I/O bound operations
        :return: None
        """
        procs_io_bound = procs_io_bound if procs_io_bound is not None else self._session.procs_io_bound if self._session.procs_io_bound is not None else 1
        logger.debug(f"procs_io_bound: {procs_io_bound}")
        buckets: Dict[str, List[Dict[str, str]]] = {}
        for path in objects_paths:
            path_cleaned: str = path.replace("s3://", "")
            bucket_name: str = path_cleaned.split("/", 1)[0]
            if bucket_name not in buckets:
                buckets[bucket_name] = []
            buckets[bucket_name].append({"Key": path_cleaned.split("/", 1)[1]})

        bucket: str
        batch: List[Dict[str, str]]
        for bucket, batch in buckets.items():
            procs: List[mp.Process] = []
            logger.debug(f"bucket: {bucket}")
            if procs_io_bound > 1:
                logger.debug(f"len(batch): {len(batch)}")
                num_requests: int = int(ceil((float(len(batch)) / 1000.0)))
                num_requests = procs_io_bound if procs_io_bound < num_requests else num_requests
                bounders: List[Tuple[int, int]] = calculate_bounders(len(batch), num_requests)
                logger.debug(f"bounders: {bounders}")
                bounder: Tuple[int, int]
                if len(bounders) == 1:
                    bounder = bounders[0]
                    self._delete_objects_batch(session_primitives=self._session.primitives,
                                               bucket=bucket,
                                               batch=batch[bounder[0]:bounder[1]])
                else:
                    for bounder in bounders:
                        proc: mp.Process = mp.Process(
                            target=self._delete_objects_batch,
                            args=(
                                self._session.primitives,
                                bucket,
                                batch[bounder[0]:bounder[1]],
                            ),
                        )
                        proc.daemon = False
                        proc.start()
                        procs.append(proc)
                    for proc in procs:
                        proc.join()
            else:
                self._delete_objects_batch(session_primitives=self._session.primitives, bucket=bucket, batch=batch)

    def delete_not_listed_objects(self, objects_paths: List[str], procs_io_bound: Optional[int] = None) -> None:
        """
        Delete all NOT listed objects.

        :param objects_paths: List of objects paths to be held.
        :param procs_io_bound: Number of processes to be used for I/O bound operations
        :return: None
        """
        procs_io_bound = procs_io_bound if procs_io_bound is not None else self._session.procs_io_bound if self._session.procs_io_bound is not None else 1
        logger.debug(f"procs_io_bound: {procs_io_bound}")

        partitions: Dict[str, List[str]] = {}
        for object_path in objects_paths:
            partition_path = f"{object_path.rsplit('/', 1)[0]}/"
            if partition_path not in partitions:
                partitions[partition_path] = []
            partitions[partition_path].append(object_path)
        procs = []
        for partition_path, batch in partitions.items():
            proc = mp.Process(
                target=self._delete_not_listed_batch,
                args=(self._session.primitives, partition_path, batch, 1),
            )
            proc.daemon = False
            proc.start()
            procs.append(proc)
            if len(procs) == self._session.procs_io_bound:
                wait_process_release(procs)
        logger.debug(f"Waiting final processes...")
        for proc in procs:
            proc.join()

    @staticmethod
    def _delete_not_listed_batch(session_primitives: "SessionPrimitives", partition_path, batch, procs_io_bound=None):
        session: "Session" = session_primitives.session
        procs_io_bound = procs_io_bound if procs_io_bound is not None else session.procs_io_bound if session.procs_io_bound is not None else 1
        logger.debug(f"procs_io_bound: {procs_io_bound}")
        keys = session.s3.list_objects(path=partition_path)
        dead_keys = [key for key in keys if key not in batch]
        session.s3.delete_listed_objects(objects_paths=dead_keys, procs_io_bound=1)

    @staticmethod
    def _delete_objects_batch(session_primitives: "SessionPrimitives", bucket: str, batch: List[Dict[str,
                                                                                                     str]]) -> None:
        session: "Session" = session_primitives.session
        client_s3: client = session.boto3_session.client(service_name="s3",
                                                         use_ssl=True,
                                                         config=session.botocore_config)
        num_requests: int = int(ceil((float(len(batch)) / 1000.0)))
        bounders: List[Tuple[int, int]] = calculate_bounders(len(batch), num_requests)
        logger.debug(f"Bounders: {bounders}")
        for bounder in bounders:
            client_s3.delete_objects(Bucket=bucket, Delete={"Objects": batch[bounder[0]:bounder[1]]})

    def list_objects(self, path: str) -> List[str]:
        """
        List Amazon S3 objects.

        :param path: S3 path (e.g. "s3://bucket/path")
        :return: List of objects paths
        """
        bucket: str
        bucket, path = self.parse_path(path=path)
        args: Dict[str, Any] = {"Bucket": bucket, "MaxKeys": 1000, "Prefix": path}
        next_continuation_token: Optional[str] = ""
        keys: List[str] = []
        while next_continuation_token is not None:
            res: Dict[str, Any] = self._client_s3.list_objects_v2(**args)
            if res.get("Contents") is None:
                break
            keys += [
                f"s3://{bucket}/{x['Key']}" for x in res.get("Contents")  # type: ignore
                if (x is not None) and ("Key" in x)
            ]
            next_continuation_token = res.get("NextContinuationToken")
            if next_continuation_token:
                args["ContinuationToken"] = next_continuation_token
        return keys

    @staticmethod
    @tenacity.retry(retry=tenacity.retry_if_exception_type(exception_types=(ClientError, HTTPClientError,
                                                                            ConnectTimeoutError)),
                    wait=tenacity.wait_random_exponential(multiplier=0.5),
                    stop=tenacity.stop_after_attempt(max_attempt_number=10),
                    reraise=True,
                    after=tenacity.after_log(logger, INFO))
    def _head_object_with_retry(client_s3: client, bucket: str, key: str) -> Dict[str, Any]:
        return client_s3.head_object(Bucket=bucket, Key=key)

    @staticmethod
    def _get_objects_head_remote(send_pipe, session_primitives: "SessionPrimitives", objects_paths):
        session: "Session" = session_primitives.session
        client_s3: client = session.boto3_session.client(service_name="s3",
                                                         use_ssl=True,
                                                         config=session.botocore_config)
        objects_sizes = {}
        logger.debug(f"len(objects_paths): {len(objects_paths)}")
        for object_path in objects_paths:
            bucket, key = object_path.replace("s3://", "").split("/", 1)
            res = S3._head_object_with_retry(client_s3=client_s3, bucket=bucket, key=key)
            size = res["ContentLength"]
            objects_sizes[object_path] = size
        logger.debug(f"len(objects_sizes): {len(objects_sizes)}")
        send_pipe.send(objects_sizes)
        send_pipe.close()

    def get_objects_sizes(self, objects_paths: List[str], procs_io_bound: Optional[int] = None) -> Dict[str, int]:
        """
        Get sizes of a list of S3 objects.

        :param objects_paths: List of objects paths to be held.
        :param procs_io_bound: Number of processes to be used for I/O bound operations
        :return: Dictionary of paths and sizes (bytes)
        """
        procs_io_bound = procs_io_bound if procs_io_bound is not None else self._session.procs_io_bound if self._session.procs_io_bound is not None else 1
        logger.debug(f"procs_io_bound: {procs_io_bound}")
        objects_sizes: Dict[str, int] = {}
        procs = []
        receive_pipes = []
        bounders = calculate_bounders(len(objects_paths), procs_io_bound)
        logger.debug(f"len(bounders): {len(bounders)}")
        for bounder in bounders:
            receive_pipe, send_pipe = mp.Pipe()
            logger.debug(f"bounder: {bounder}")
            proc = mp.Process(
                target=self._get_objects_head_remote,
                args=(
                    send_pipe,
                    self._session.primitives,
                    objects_paths[bounder[0]:bounder[1]],
                ),
            )
            proc.daemon = False
            proc.start()
            procs.append(proc)
            receive_pipes.append(receive_pipe)
        logger.debug(f"len(procs): {len(bounders)}")
        for i in range(len(procs)):
            logger.debug(f"Waiting pipe number: {i}")
            received = receive_pipes[i].recv()
            objects_sizes.update(received)
            logger.debug(f"Waiting proc number: {i}")
            procs[i].join()
            logger.debug(f"Closing proc number: {i}")
            receive_pipes[i].close()
        return objects_sizes

    def copy_listed_objects(self,
                            objects_paths: List[str],
                            source_path: str,
                            target_path: str,
                            mode: str = "append",
                            procs_io_bound: Optional[int] = None) -> None:
        """
        Copy a list of S3 objects.

        :param objects_paths: List of objects paths
        :param source_path: Source directory
        :param target_path: Target directory
        :param mode: "append" | "overwrite" | "overwrite_partitions"
        :param procs_io_bound: procs_io_bound: Number of processes to be used for I/O bound operations
        :return: None
        """
        procs_io_bound = procs_io_bound if procs_io_bound is not None else self._session.procs_io_bound if self._session.procs_io_bound is not None else 1
        logger.debug(f"procs_io_bound: {procs_io_bound}")
        logger.debug(f"len(objects_paths): {len(objects_paths)}")
        if source_path[-1] == "/":
            source_path = source_path[:-1]
        if target_path[-1] == "/":
            target_path = target_path[:-1]

        if mode == "overwrite":
            logger.debug(f"Deleting to overwrite: {target_path}")
            self._session.s3.delete_objects(path=target_path)
        elif mode == "overwrite_partitions":
            objects_wo_prefix = [o.replace(f"{source_path}/", "") for o in objects_paths]
            objects_wo_filename = [f"{o.rpartition('/')[0]}/" for o in objects_wo_prefix]
            partitions_paths = list(set(objects_wo_filename))
            target_partitions_paths = [f"{target_path}/{p}" for p in partitions_paths]
            for path in target_partitions_paths:
                logger.debug(f"Deleting to overwrite_partitions: {path}")
                self._session.s3.delete_objects(path=path)

        batch = []
        for obj in objects_paths:
            object_wo_prefix = obj.replace(f"{source_path}/", "")
            target_object = f"{target_path}/{object_wo_prefix}"
            batch.append((obj, target_object))

        if procs_io_bound > 1:
            bounders = calculate_bounders(len(objects_paths), procs_io_bound)
            logger.debug(f"bounders: {bounders}")
            procs = []
            for bounder in bounders:
                proc = mp.Process(
                    target=self._copy_objects_batch,
                    args=(
                        self._session.primitives,
                        batch[bounder[0]:bounder[1]],
                    ),
                )
                proc.daemon = False
                proc.start()
                procs.append(proc)
            for proc in procs:
                proc.join()
        else:
            self._copy_objects_batch(session_primitives=self._session.primitives, batch=batch)

    @staticmethod
    def _copy_objects_batch(session_primitives: "SessionPrimitives", batch):
        session: "Session" = session_primitives.session
        resource_s3: resource = session.boto3_session.resource(service_name="s3", config=session.botocore_config)
        logger.debug(f"len(batch): {len(batch)}")
        for source_obj, target_obj in batch:
            source_bucket, source_key = S3.parse_path(path=source_obj)
            copy_source = {"Bucket": source_bucket, "Key": source_key}
            target_bucket, target_key = S3.parse_path(path=target_obj)
            resource_s3.meta.client.copy(copy_source, target_bucket, target_key)

    def get_bucket_region(self, bucket: str) -> str:
        """
        Get bucket region.

        :param bucket: Bucket name
        :return: region code (e.g. "us-east-1")
        """
        logger.debug(f"bucket: {bucket}")
        region: str = self._client_s3.get_bucket_location(Bucket=bucket)["LocationConstraint"]
        region = "us-east-1" if region is None else region
        logger.debug(f"region: {region}")
        return region
