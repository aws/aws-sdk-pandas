import boto3


def get_session(
    session_primitives=None, key=None, secret=None, profile=None, region=None
):
    """
    Return a configured Boto3 Session object
    """
    if session_primitives:
        key = session_primitives.key if session_primitives.key else key
        secret = session_primitives.secret if session_primitives.secret else secret
        profile = session_primitives.profile if session_primitives.profile else profile
        region = session_primitives.region if session_primitives.region else region
    if profile:
        return boto3.Session(region_name=region, profile_name=profile)
    elif key and secret:
        return boto3.Session(
            region_name=region, aws_access_key_id=key, aws_secret_access_key=secret
        )
    else:
        return boto3.Session(region_name=region)


class SessionPrimitives:
    def __init__(self, key=None, secret=None, profile=None, region=None):
        self.key = key
        self.secret = secret
        self.profile = profile
        self.region = region


def calculate_bounders(num_items, num_groups):
    num_groups = num_items if num_items < num_groups else num_groups
    size = int(num_items / num_groups)
    rest = num_items % num_groups
    bounders = []
    end = 0
    for _ in range(num_groups):
        start = end
        end += size
        if rest:
            end += 1
            rest -= 1
        bounders.append((start, end))
    return bounders
