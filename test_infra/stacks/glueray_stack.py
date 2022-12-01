from aws_cdk import CfnOutput, Duration, RemovalPolicy, Stack
from aws_cdk import aws_athena as athena
from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_assets as s3_assets
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
            ],
        )

        bucket.grant_read_write(self.glue_service_role)
        self.script_bucket.grant_read(self.glue_service_role)

        # Grant data permissions to Glue job
        amazon_reviews_pds_bucket = s3.Bucket.from_bucket_name(
            self,
            "amazon_reviews_pds",
            bucket_name="amazon-reviews-pds",
        )
        amazon_reviews_pds_bucket.grant_read(self.glue_service_role)

        # Where should the ZIP be uploaded
        self.wrangler_asset_path = self.script_bucket.s3_url_for_object("awswrangler.zip")

        # Create Glue Job
        glue_job1 = self._create_glue_job(id="Glue Job 1", script_path="glue_scripts/glue_example_1.py")

        # CFN outputs
        CfnOutput(
            self,
            "AWS SDK for pandas ZIP Location",
            value=self.wrangler_asset_path,
            export_name="WranglerZipLocation",
        )

        CfnOutput(
            self,
            "Glue Job 1 Name",
            value=glue_job1.ref,
            export_name="GlueJob1Name",
        )

    def _deploy_glue_asset(self, id: str, path: str) -> s3_assets.Asset:
        return s3_assets.Asset(
            self,
            id,
            path=path,
            readers=[self.glue_service_role],
        )

    def _create_glue_job(self, id: str, script_path: str) -> glue.CfnJob:
        glue_job_script = self._deploy_glue_asset(f"{id} Script Asset", script_path)

        return glue.CfnJob(
            self,
            id,
            command=glue.CfnJob.JobCommandProperty(
                name="glueray",
                python_version="3.9",
                script_location=glue_job_script.s3_object_url,
            ),
            default_arguments={
                "--additional-python-modules": self.wrangler_asset_path,
                "--auto-scaling-ray-min-workers": "5",
                "--athena-workgroup": self.athena_workgroup.ref,
            },
            glue_version="4.0",
            role=self.glue_service_role.role_name,
            worker_type="Z.2X",
            number_of_workers=5,
        )
