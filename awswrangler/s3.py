import multiprocessing as mp
from math import ceil
import logging

from botocore.exceptions import ClientError
import s3fs
import tenacity

from awswrangler.utils import calculate_bounders

logger = logging.getLogger(__name__)


def mkdir_if_not_exists(fs, path):
    if fs._isfilestore() and not fs.exists(path):
        try:
            fs.mkdir(path)
        except OSError:
            assert fs.exists(path)


def get_fs(session_primitives):
    aws_access_key_id, aws_secret_access_key, profile_name, config = None, None, None, None
    if session_primitives:
        aws_access_key_id = (session_primitives.aws_access_key_id
                             if session_primitives.aws_access_key_id else None)
        aws_secret_access_key = (session_primitives.aws_secret_access_key
                                 if session_primitives.aws_secret_access_key
                                 else None)
        profile_name = (session_primitives.profile_name
                        if session_primitives.profile_name else None)
        config = {
            "retries": {
                "max_attempts": session_primitives.botocore_max_retries
            }
        }
    if profile_name:
        return s3fs.S3FileSystem(profile_name=profile_name,
                                 config_kwargs=config)
    elif aws_access_key_id and aws_secret_access_key:
        return s3fs.S3FileSystem(key=aws_access_key_id,
                                 secret=aws_secret_access_key,
                                 config_kwargs=config)
    else:
        return s3fs.S3FileSystem(config_kwargs=config)


class S3:
    def __init__(self, session):
        self._session = session

    @staticmethod
    def parse_path(path):
        bucket, path = path.replace("s3://", "").split("/", 1)
        if not path:
            path = ""
        elif len(path) == 1:
            if path[0] != "/":
                path += "/"
            else:
                path = ""
        else:
            if path[-1] != "/":
                path += "/"
        return bucket, path

    @staticmethod
    def parse_object_path(path):
        return path.replace("s3://", "").split("/", 1)

    def delete_objects(self, path):
        bucket, path = self.parse_path(path=path)
        client = self._session.boto3_session.client(
            service_name="s3", config=self._session.botocore_config)
        procs = []
        args = {"Bucket": bucket, "MaxKeys": 1000, "Prefix": path}
        logger.debug(f"Arguments: \n{args}")
        next_continuation_token = True
        while next_continuation_token:
            res = client.list_objects_v2(**args)
            if not res.get("Contents"):
                break
            keys = [{"Key": x.get("Key")} for x in res.get("Contents")]
            logger.debug(f"Number of listed keys: {len(keys)}")
            next_continuation_token = res.get("NextContinuationToken")
            if next_continuation_token:
                args["ContinuationToken"] = next_continuation_token
                proc = mp.Process(
                    target=self.delete_objects_batch,
                    args=(self._session.primitives, bucket, keys),
                )
                proc.daemon = False
                proc.start()
                procs.append(proc)
                while len(procs) >= self._session.procs_io_bound:
                    logger.debug(
                        f"len(procs) ({len(procs)}) >= self._session.procs_io_bound ({self._session.procs_io_bound})"
                    )
                    procs[0].join()
                    del procs[0]
                    logger.debug(f"Processes deleted from list.")
            else:
                logger.debug(f"Starting last delete call...")
                self.delete_objects_batch(self._session.primitives, bucket,
                                          keys)
        logger.debug(f"Waiting final processes...")
        for proc in procs:
            proc.join()

    def delete_listed_objects(self, objects_paths, procs_io_bound=None):
        if not procs_io_bound:
            procs_io_bound = self._session.procs_io_bound
        logger.debug(f"procs_io_bound: {procs_io_bound}")
        buckets = {}
        for path in objects_paths:
            path_cleaned = path.replace("s3://", "")
            bucket_name = path_cleaned.split("/", 1)[0]
            if bucket_name not in buckets:
                buckets[bucket_name] = []
            buckets[bucket_name].append({"Key": path_cleaned.split("/", 1)[1]})

        for bucket, batch in buckets.items():
            procs = []
            logger.debug(f"bucket: {bucket}")
            if procs_io_bound > 1:
                logger.debug(f"len(batch): {len(batch)}")
                bounders = calculate_bounders(len(batch), procs_io_bound)
                logger.debug(f"bounders: {bounders}")
                for bounder in bounders:
                    proc = mp.Process(
                        target=self.delete_objects_batch,
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
                self.delete_objects_batch(
                    session_primitives=self._session.primitives,
                    bucket=bucket,
                    batch=batch)

    def delete_not_listed_objects(self, objects_paths, procs_io_bound=None):
        if not procs_io_bound:
            procs_io_bound = self._session.procs_io_bound
        logger.debug(f"procs_io_bound: {procs_io_bound}")

        partitions = {}
        for object_path in objects_paths:
            partition_path = f"{object_path.rsplit('/', 1)[0]}/"
            if partition_path not in partitions:
                partitions[partition_path] = []
            partitions[partition_path].append(object_path)
        procs = []
        for partition_path, batch in partitions.items():
            proc = mp.Process(
                target=self.delete_not_listed_batch,
                args=(self._session.primitives, partition_path, batch, 1),
            )
            proc.daemon = False
            proc.start()
            procs.append(proc)
            while len(procs) >= procs_io_bound:
                logger.debug(
                    f"len(procs) ({len(procs)}) >= procs_io_bound ({procs_io_bound})"
                )
                procs[0].join()
                del procs[0]
                logger.debug(f"Processes deleted from list.")
        logger.debug(f"Waiting final processes...")
        for proc in procs:
            proc.join()

    @staticmethod
    def delete_not_listed_batch(session_primitives,
                                partition_path,
                                batch,
                                procs_io_bound=None):
        session = session_primitives.session
        if not procs_io_bound:
            procs_io_bound = session.procs_io_bound
        logger.debug(f"procs_io_bound: {procs_io_bound}")
        keys = session.s3.list_objects(path=partition_path)
        dead_keys = [key for key in keys if key not in batch]
        session.s3.delete_listed_objects(objects_paths=dead_keys,
                                         procs_io_bound=1)

    @staticmethod
    def delete_objects_batch(session_primitives, bucket, batch):
        session = session_primitives.session
        client = session.boto3_session.client(service_name="s3",
                                              config=session.botocore_config)
        num_requests = int(ceil((float(len(batch)) / 1000.0)))
        bounders = calculate_bounders(len(batch), num_requests)
        logger.debug(f"Bounders: {bounders}")
        for bounder in bounders:
            client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": batch[bounder[0]:bounder[1]]})

    def list_objects(self, path):
        bucket, path = path.replace("s3://", "").split("/", 1)
        if path[-1] != "/":
            path += "/"
        client = self._session.boto3_session.client(
            service_name="s3", config=self._session.botocore_config)
        args = {"Bucket": bucket, "MaxKeys": 1000, "Prefix": path}
        next_continuation_token = True
        keys = []
        while next_continuation_token:
            res = client.list_objects_v2(**args)
            if not res.get("Contents"):
                break
            keys += [
                f"s3://{bucket}/{x.get('Key')}" for x in res.get("Contents")
            ]
            next_continuation_token = res.get("NextContinuationToken")
            if next_continuation_token:
                args["ContinuationToken"] = next_continuation_token
        return keys

    @staticmethod
    @tenacity.retry(
        retry=tenacity.retry_if_exception_type(exception_types=ClientError),
        wait=tenacity.wait_random_exponential(multiplier=0.5, max=10),
        stop=tenacity.stop_after_attempt(max_attempt_number=15),
        reraise=True,
    )
    def head_object_with_retry(client, bucket, key):
        return client.head_object(Bucket=bucket, Key=key)

    @staticmethod
    def _get_objects_head_remote(send_pipe, session_primitives, objects_paths):
        session = session_primitives.session
        client = session.boto3_session.client(service_name="s3",
                                              config=session.botocore_config)
        objects_sizes = {}
        logger.debug(f"len(objects_paths): {len(objects_paths)}")
        for object_path in objects_paths:
            bucket, key = object_path.replace("s3://", "").split("/", 1)
            res = S3.head_object_with_retry(client=client,
                                            bucket=bucket,
                                            key=key)
            size = res["ContentLength"]
            objects_sizes[object_path] = size
        logger.debug(f"len(objects_sizes): {len(objects_sizes)}")
        send_pipe.send(objects_sizes)
        send_pipe.close()

    def get_objects_sizes(self, objects_paths, procs_io_bound=None):
        if not procs_io_bound:
            procs_io_bound = self._session.procs_io_bound
        logger.debug(f"procs_io_bound: {procs_io_bound}")
        objects_sizes = {}
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
            receved = receive_pipes[i].recv()
            objects_sizes.update(receved)
            logger.debug(f"Waiting proc number: {i}")
            procs[i].join()
            logger.debug(f"Closing proc number: {i}")
            receive_pipes[i].close()
        return objects_sizes

    def copy_listed_objects(self,
                            objects_paths,
                            source_path,
                            target_path,
                            mode="append",
                            procs_io_bound=None):
        if not procs_io_bound:
            procs_io_bound = self._session.procs_io_bound
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
            objects_wo_prefix = [
                o.replace(f"{source_path}/", "") for o in objects_paths
            ]
            objects_wo_filename = [
                f"{o.rpartition('/')[0]}/" for o in objects_wo_prefix
            ]
            partitions_paths = list(set(objects_wo_filename))
            target_partitions_paths = [
                f"{target_path}/{p}" for p in partitions_paths
            ]
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
                    target=self.copy_objects_batch,
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
            self.copy_objects_batch(
                session_primitives=self._session.primitives, batch=batch)

    @staticmethod
    def copy_objects_batch(session_primitives, batch):
        session = session_primitives.session
        resource = session.boto3_session.resource(
            service_name="s3", config=session.botocore_config)
        logger.debug(f"len(batch): {len(batch)}")
        for source_obj, target_obj in batch:
            source_bucket, source_key = S3.parse_object_path(path=source_obj)
            copy_source = {"Bucket": source_bucket, "Key": source_key}
            target_bucket, target_key = S3.parse_object_path(path=target_obj)
            resource.meta.client.copy(copy_source, target_bucket, target_key)
