import multiprocessing as mp

import s3fs

from ..common import get_session


def del_objs_batch_wrapper(args):
    return del_objs_batch(*args)


def del_objs_batch(bucket, batch, session_primitives=None):
    client = get_session(session_primitives=session_primitives).client("s3")
    client.delete_objects(Bucket=bucket, Delete={"Objects": batch})


def delete_objects(path, session_primitives=None, batch_size=1000):
    bucket, path = path.replace("s3://", "").split("/", 1)
    if path[-1] != "/":
        path += "/"
    client = get_session(session_primitives=session_primitives).client("s3")
    pool = mp.Pool(mp.cpu_count())
    procs = []
    args = {"Bucket": bucket, "MaxKeys": batch_size, "Prefix": path}
    NextContinuationToken = True
    while NextContinuationToken:
        res = client.list_objects_v2(**args)
        if not res.get("Contents"):
            break
        keys = [{"Key": x.get("Key")} for x in res.get("Contents")]
        NextContinuationToken = res.get("NextContinuationToken")
        if NextContinuationToken:
            args["ContinuationToken"] = NextContinuationToken
            procs.append(
                pool.apply_async(
                    del_objs_batch_wrapper, ((bucket, keys, session_primitives),)
                )
            )
        else:
            del_objs_batch(bucket, keys, session_primitives)
    for proc in procs:
        proc.get()


def list_objects(path, session_primitives=None, batch_size=1000):
    bucket, path = path.replace("s3://", "").split("/", 1)
    if path[-1] != "/":
        path += "/"
    client = get_session(session_primitives=session_primitives).client("s3")
    args = {"Bucket": bucket, "MaxKeys": batch_size, "Prefix": path}
    NextContinuationToken = True
    keys = []
    while NextContinuationToken:
        res = client.list_objects_v2(**args)
        if not res.get("Contents"):
            break
        keys += [{"Key": x.get("Key")} for x in res.get("Contents")]
        NextContinuationToken = res.get("NextContinuationToken")
        if NextContinuationToken:
            args["ContinuationToken"] = NextContinuationToken
    return keys


def calc_bounders(num, cpus):
    cpus = num if num < cpus else cpus
    size = int(num / cpus)
    rest = num % cpus
    bounders = []
    start = 0
    end = -1
    for _ in range(cpus):
        start = end + 1
        end += size
        if rest:
            end += 1
            rest -= 1
        bounders.append((start, end))
    return bounders


def delete_listed_objects(bucket, batch, session_primitives=None, batch_size=1000):
    if len(batch) > batch_size:
        cpus = mp.cpu_count()
        bounders = calc_bounders(len(batch), cpus)
        args = []
        for item in bounders:
            args.append((bucket, batch[item[0] : item[1] + 1], session_primitives))
        pool = mp.Pool(cpus)
        pool.map(del_objs_batch_wrapper, args)
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
