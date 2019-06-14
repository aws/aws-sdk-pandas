import multiprocessing as mp

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

    def delete_objects(self, path, batch_size=1000):
        bucket, path = path.replace("s3://", "").split("/", 1)
        if path[-1] != "/":
            path += "/"
        client = self._session.boto3_session.client("s3")
        procs = []
        args = {"Bucket": bucket, "MaxKeys": batch_size, "Prefix": path}
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
                    target=self.del_objs_batch,
                    args=(self._session.primitives, bucket, keys),
                )
                proc.daemon = True
                proc.start()
                procs.append(proc)
            else:
                self.del_objs_batch(self._session.primitives, bucket, keys)
        for proc in procs:
            proc.join()

    def delete_listed_objects(self, bucket, batch, batch_size=1000):
        if len(batch) > batch_size:
            num_procs = mp.cpu_count()
            bounders = calculate_bounders(len(batch), num_procs)
            procs = []
            for bounder in bounders:
                proc = mp.Process(
                    target=self.del_objs_batch,
                    args=(
                        self._session.primitives,
                        bucket,
                        batch[bounder[0] : bounder[1]],
                    ),
                )
                proc.daemon = True
                proc.start()
                procs.append(proc)
            for proc in procs:
                proc.join()
        else:
            self.del_objs_batch(self._session.primitives, bucket, batch)

    @staticmethod
    def del_objs_batch(session_primitives, bucket, batch):
        client = session_primitives.session.boto3_session.client("s3")
        client.delete_objects(Bucket=bucket, Delete={"Objects": batch})

    def list_objects(self, path, batch_size=1000):
        bucket, path = path.replace("s3://", "").split("/", 1)
        if path[-1] != "/":
            path += "/"
        client = self._session.boto3_session.client("s3")
        args = {"Bucket": bucket, "MaxKeys": batch_size, "Prefix": path}
        next_continuation_token = True
        keys = []
        while next_continuation_token:
            res = client.list_objects_v2(**args)
            if not res.get("Contents"):
                break
            keys += [{"Key": x.get("Key")} for x in res.get("Contents")]
            next_continuation_token = res.get("NextContinuationToken")
            if next_continuation_token:
                args["ContinuationToken"] = next_continuation_token
        return keys
