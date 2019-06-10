import multiprocessing as mp

import s3fs

from awswrangler.common import get_session, calculate_bounders


def del_objs_batch(bucket, batch, session_primitives=None):
    client = get_session(session_primitives=session_primitives).client("s3")
    client.delete_objects(Bucket=bucket, Delete={"Objects": batch})


def delete_objects(path, session_primitives=None, batch_size=1000):
    bucket, path = path.replace("s3://", "").split("/", 1)
    if path[-1] != "/":
        path += "/"
    client = get_session(session_primitives=session_primitives).client("s3")
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
                target=del_objs_batch, args=(bucket, keys, session_primitives)
            )
            proc.daemon = True
            proc.start()
            procs.append(proc)
        else:
            del_objs_batch(bucket, keys, session_primitives)
    for proc in procs:
        proc.join()


def list_objects(path, session_primitives=None, batch_size=1000):
    bucket, path = path.replace("s3://", "").split("/", 1)
    if path[-1] != "/":
        path += "/"
    client = get_session(session_primitives=session_primitives).client("s3")
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


def delete_listed_objects(bucket, batch, session_primitives=None, batch_size=1000):
    if len(batch) > batch_size:
        num_procs = mp.cpu_count()
        bounders = calculate_bounders(len(batch), num_procs)
        procs = []
        for bounder in bounders:
            proc = mp.Process(
                target=del_objs_batch,
                args=(bucket, batch[bounder[0] : bounder[1]], session_primitives),
            )
            proc.daemon = True
            proc.start()
            procs.append(proc)
        for proc in procs:
            proc.join()
    else:
        del_objs_batch(bucket, batch, session_primitives)


def get_fs(session_primitives=None):
    key, secret, profile = None, None, None
    if session_primitives:
        key = session_primitives.key if session_primitives.key else None
        secret = session_primitives.secret if session_primitives.secret else None
        profile = session_primitives.profile if session_primitives.profile else None
    if profile:
        return s3fs.S3FileSystem(profile_name=profile)
    elif key and secret:
        return s3fs.S3FileSystem(key=key, secret=secret)
    else:
        return s3fs.S3FileSystem()


def mkdir_if_not_exists(fs, path):
    if fs._isfilestore() and not fs.exists(path):
        try:
            fs.mkdir(path)
        except OSError:
            assert fs.exists(path)
