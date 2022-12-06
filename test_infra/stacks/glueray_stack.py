from aws_cdk import CfnOutput, RemovalPolicy, Stack
from aws_cdk import aws_athena as athena
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from constructs import Construct


class GlueRayStack(Stack):  # type: ignore
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        bucket: s3.Bucket,
        **kwargs: str,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.script_bucket = s3.Bucket(
            self,
            "Script Bucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.athena_workgroup = athena.CfnWorkGroup(
            self,
            "Workgroup",
            state="ENABLED",
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=bucket.s3_url_for_object(""),
                ),
            ),
            description="AWS SDK for pandas with Glue on Ray",
            name="GlueRayWorkGroup",
        )

        self.glue_service_role = iam.Role(
            self,
            "Glue Job Role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"),
                iam.ManagedPolicy.from_aws_managed_policy_name("CloudWatchFullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonAthenaFullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3ReadOnlyAccess"),
            ],
        )

        bucket.grant_read_write(self.glue_service_role)

        # Where should the ZIP be uploaded
        zip_key = "awswrangler.zip"
        self.wrangler_asset_path = self.script_bucket.s3_url_for_object(zip_key)

        # CFN outputs
        CfnOutput(
            self,
            "Script Bucket Name",
            value=self.script_bucket.bucket_name,
            export_name="ScriptBucketName",
        )

        CfnOutput(
            self,
            "AWS SDK for pandas ZIP Key",
            value=zip_key,
            export_name="AWSSDKforpandasZIPKey",
        )

        CfnOutput(
            self,
            "AWS SDK for pandas ZIP Location",
            value=self.wrangler_asset_path,
            export_name="AWSSDKforpandasZIPLocation",
        )

        CfnOutput(
            self,
            "Glue Job Role Arn",
            value=self.glue_service_role.role_arn,
            export_name="GlueRayJobRoleArn",
        )

        CfnOutput(
            self,
            "Glue Ray Athena Workgroup Name",
            value=self.athena_workgroup.ref,
            export_name="GlueRayAthenaWorkgroupName",
        )
