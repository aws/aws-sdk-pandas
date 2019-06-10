import multiprocessing as mp
import boto3


def calc_bounders(num, cpus):
    cpus = num if num < cpus else cpus
    size = int(num / cpus)
    rest = num % cpus
    bounders = []
    end = -1
    for _ in range(cpus):
        start = end + 1
        end += size
        if rest:
            end += 1
            rest -= 1
        bounders.append((start, end))
    return bounders


def wrt_fake_objs_batch_wrapper(args):
    return wrt_fake_objs_batch(*args)


def wrt_fake_objs_batch(bucket, path, start, end):
    s3 = boto3.resource("s3")
    for obj_id in range(start, end + 1):
        s3.Object(bucket, f"{path}{obj_id}.txt").put(Body=str(obj_id).zfill(10))


def write_fake_objects(bucket, path, num):
    if path[-1] != "/":
        path += "/"
    if num < 10:
        wrt_fake_objs_batch(bucket, path, 0, num - 1)
        return
    cpus = mp.cpu_count()
    bounders = calc_bounders(num, cpus)
    args = []
    for item in bounders:
        args.append((bucket, path, item[0], item[1]))
    pool = mp.Pool(cpus)
    pool.map(wrt_fake_objs_batch_wrapper, args)
