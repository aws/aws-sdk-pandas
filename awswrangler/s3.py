import multiprocessing as mp
from math import ceil

import s3fs

from awswrangler.utils import calculate_bounders


def mkdir_if_not_exists(fs, path):
    if fs._isfilestore() and not fs.exists(path):
        try:
            fs.mkdir(path)
        except OSError:
            assert fs.exists(path)


def get_fs(session_primitives):
    aws_access_key_id, aws_secret_access_key, profile_name = None, None, None
    if session_primitives:
        aws_access_key_id = (
            session_primitives.aws_access_key_id
            if session_primitives.aws_access_key_id
            else None
        )
        aws_secret_access_key = (
            session_primitives.aws_secret_access_key
            if session_primitives.aws_secret_access_key
            else None
        )
        profile_name = (
            session_primitives.profile_name if session_primitives.profile_name else None
        )
    if profile_name:
        return s3fs.S3FileSystem(profile_name=profile_name)
    elif aws_access_key_id and aws_secret_access_key:
        return s3fs.S3FileSystem(key=aws_access_key_id, secret=aws_secret_access_key)
    else:
        return s3fs.S3FileSystem()


class S3:
    def __init__(self, session):
        self._session = session

    def delete_objects(self, path):
        bucket, path = path.replace("s3://", "").split("/", 1)
        if path[-1] != "/":
            path += "/"
        client = self._session.boto3_session.client(
            service_name="s3", config=self._session.botocore_config
        )
        procs = []
        args = {"Bucket": bucket, "MaxKeys": 1000, "Prefix": path}
        next_continuation_token = True
        while next_continuation_token:
            res = client.list_objects_v2(**args)
            if not res.get("Contents"):
                break
            keys = [{"Key": x.get("Key")} for x in res.get("Contents")]
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
            else:
                self.delete_objects_batch(self._session.primitives, bucket, keys)
        for proc in procs:
            proc.join()

    def delete_listed_objects(self, objects_paths, procs_io_bound=None):
        if not procs_io_bound:
            procs_io_bound = self._session.procs_io_bound
        buckets = {}
        for path in objects_paths:
            path_cleaned = path.replace("s3://", "")
            bucket_name = path_cleaned.split("/", 1)[0]
            if bucket_name not in buckets:
                buckets[bucket_name] = []
            buckets[bucket_name].append({"Key": path_cleaned.split("/", 1)[1]})

        for bucket, batch in buckets.items():
            procs = []
            if procs_io_bound > 1:
                bounders = calculate_bounders(len(batch), procs_io_bound)
                for bounder in bounders:
                    proc = mp.Process(
                        target=self.delete_objects_batch,
                        args=(
                            self._session.primitives,
                            bucket,
                            batch[bounder[0] : bounder[1]],
                        ),
                    )
                    proc.daemon = False
                    proc.start()
                    procs.append(proc)
            else:
                self.delete_objects_batch(self._session.primitives, bucket, batch)
            for proc in procs:
                proc.join()

    def delete_not_listed_objects(self, objects_paths):
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
                args=(self._session.primitives, partition_path, batch),
            )
            proc.daemon = False
            proc.start()
            procs.append(proc)
        for proc in procs:
            proc.join()

    @staticmethod
    def delete_not_listed_batch(session_primitives, partition_path, batch):
        session = session_primitives.session
        keys = session.s3.list_objects(path=partition_path)
        dead_keys = [key for key in keys if key not in batch]
        session.s3.delete_listed_objects(objects_paths=dead_keys)

    @staticmethod
    def delete_objects_batch(session_primitives, bucket, batch):
        session = session_primitives.session
        client = session.boto3_session.client(
            service_name="s3", config=session.botocore_config
        )
        num_requests = int(ceil((float(len(batch)) / 1000.0)))
        bounders = calculate_bounders(len(batch), num_requests)
        for bounder in bounders:
            client.delete_objects(
                Bucket=bucket, Delete={"Objects": batch[bounder[0] : bounder[1]]}
            )

    def list_objects(self, path):
        bucket, path = path.replace("s3://", "").split("/", 1)
        if path[-1] != "/":
            path += "/"
        client = self._session.boto3_session.client(
            service_name="s3", config=self._session.botocore_config
        )
        args = {"Bucket": bucket, "MaxKeys": 1000, "Prefix": path}
        next_continuation_token = True
        keys = []
        while next_continuation_token:
            res = client.list_objects_v2(**args)
            if not res.get("Contents"):
                break
            keys += [f"s3://{bucket}/{x.get('Key')}" for x in res.get("Contents")]
            next_continuation_token = res.get("NextContinuationToken")
            if next_continuation_token:
                args["ContinuationToken"] = next_continuation_token
        return keys

    @staticmethod
    def _get_objects_head_remote(send_pipe, session_primitives, objects_paths):
        session = session_primitives.session
        client = session.boto3_session.client(
            service_name="s3", config=session.botocore_config
        )
        objects_sizes = {}
        for object_path in objects_paths:
            bucket, key = object_path.replace("s3://", "").split("/", 1)
            size = client.head_object(Bucket=bucket, Key=key).get("ContentLength")
            objects_sizes[object_path] = size
        send_pipe.send(objects_sizes)
        send_pipe.close()

    def get_objects_sizes(self, objects_paths, procs_io_bound=None):
        if not procs_io_bound:
            procs_io_bound = self._session.procs_io_bound
        objects_sizes = {}
        procs = []
        receive_pipes = []
        bounders = calculate_bounders(len(objects_paths), procs_io_bound)
        for bounder in bounders:
            receive_pipe, send_pipe = mp.Pipe()
            proc = mp.Process(
                target=self._get_objects_head_remote,
                args=(
                    send_pipe,
                    self._session.primitives,
                    objects_paths[bounder[0] : bounder[1]],
                ),
            )
            proc.daemon = False
            proc.start()
            procs.append(proc)
            receive_pipes.append(receive_pipe)
        for i in range(len(procs)):
            objects_sizes.update(receive_pipes[i].recv())
            procs[i].join()
            receive_pipes[i].close()
        return objects_sizes
