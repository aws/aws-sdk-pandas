from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lakeformation as lf
from aws_cdk import aws_s3 as s3
from aws_cdk import core as cdk


class LakeFormationStack(cdk.Stack):  # type: ignore
    def __init__(
        self,
        scope: cdk.Construct,
        construct_id: str,
        **kwargs: str,
    ) -> None:
        """
        AWS Data Wrangler Development LakeFormation Infrastructure.
        """
        super().__init__(scope, construct_id, **kwargs)

        self._set_lakeformation_infra()

    def _set_lakeformation_infra(self) -> None:
        bucket = s3.Bucket(
            self,
            id="aws-data-wrangler-lf",
            block_public_access=s3.BlockPublicAccess(
                block_public_acls=True,
                block_public_policy=True,
                ignore_public_acls=True,
                restrict_public_buckets=True,
            ),
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="CleaningUp",
                    enabled=True,
                    expiration=cdk.Duration.days(1),
                    abort_incomplete_multipart_upload_after=cdk.Duration.days(1),
                ),
            ],
            versioned=True,
        )

        transaction_role = iam.Role(
            self,
            "aws-data-wrangler-lf-transaction-role",
            assumed_by=iam.ServicePrincipal("lakeformation.amazonaws.com"),
            inline_policies={
                "Root": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "s3:DeleteObject",
                                "s3:GetObject",
                                "s3:PutObject",
                            ],
                            resources=[
                                f"{bucket.bucket_arn}/*",
                            ],
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "s3:ListObject",
                            ],
                            resources=[
                                f"{bucket.bucket_arn}",
                            ],
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "execute-api:Invoke",
                            ],
                            resources=[
                                f"arn:{self.partition}:execute-api:*:*:*/*/POST/reportStatus",
                            ],
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "lakeformation:CancelTransaction",
                                "lakeformation:CommitTransaction",
                                "lakeformation:GetTableObjects",
                                "lakeformation:StartTransaction",
                                "lakeformation:UpdateTableObjects",
                            ],
                            resources=["*"],
                        ),
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "glue:GetPartitions",
                                "glue:GetTable",
                                "glue:UpdateTable",
                            ],
                            resources=["*"],
                        ),
                    ]
                ),
            },
        )

        lf.CfnResource(
            self,
            "aws-data-wrangler-bucket-lf-registration",
            resource_arn=bucket.bucket_arn,
            use_service_linked_role=False,
            role_arn=transaction_role.role_arn,
        )

        glue_db = glue.Database(
            self,
            id="aws-data-wrangler-lf-glue-db",
            database_name="aws_data_wrangler_lakeformation",
        )

        cdk.CfnOutput(self, "LakeFormationGlueDatabase", value=glue_db.database_name)
