from awswrangler.common import get_session
from awswrangler.athena.utils import run_query, query_validation
from awswrangler.s3 import read as s3_read


def read(
    database, query, s3_output=None, region=None, key=None, secret=None, profile=None
):
    """
    Read any Glue object through the AWS Athena
    """
    session = get_session(key=key, secret=secret, profile=profile, region=region)
    if not s3_output:
        account_id = session.client("sts").get_caller_identity().get("Account")
        session_region = session.region_name
        s3_output = (
            "s3://aws-athena-query-results-" + account_id + "-" + session_region + "/"
        )
        s3 = session.resource("s3")
        s3.Bucket(s3_output)
    athena_client = session.client("athena")
    qe = run_query(athena_client, query, database, s3_output)
    validation = query_validation(athena_client, qe)
    if validation.get("QueryExecution").get("Status").get("State") == "FAILED":
        message_error = "Your query is not valid: " + validation.get(
            "QueryExecution"
        ).get("Status").get("StateChangeReason")
        raise Exception(message_error)
    else:
        file = s3_output + qe + ".csv"
        df = s3_read(file, key=key, secret=secret, profile=profile, region=region)
    return df
