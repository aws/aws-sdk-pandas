import boto3


def get_session(
    session_primitives=None, key=None, secret=None, profile=None, region=None
):
    """
    Return a configured boto3 Session object
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
