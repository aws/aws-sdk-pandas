from aws_cdk import RemovalPolicy, Stack
from aws_cdk import aws_athena as athena
from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_assets as s3_assets
from aws_cdk import custom_resources as cr
from constructs import Construct


class GlueRayStack(Stack):  # type: ignore
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        **kwargs: str,
    ) -> None:
        """
        AWS SDK for pandas Development Databases Infrastructure.
        Includes Redshift, Aurora PostgreSQL, Aurora MySQL, Microsoft SQL Server, Oracle Database.
        """
        super().__init__(scope, construct_id, **kwargs)

        self.data_bucket = s3.Bucket(
            self,
            "Data Bucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,
        )

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
                    output_location=self.data_bucket.s3_url_for_object("unload/"),
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

        self.data_bucket.grant_read_write(self.glue_service_role)
        self.script_bucket.grant_read(self.glue_service_role)

        # Grant data permissions to Glue job
        amazon_reviews_pds_bucket = s3.Bucket.from_bucket_name(
            self,
            "amazon_reviews_pds",
            bucket_name="amazon-reviews-pds",
        )
        amazon_reviews_pds_bucket.grant_read(self.glue_service_role)

        # Deploy AWS SDK for pandas ZIP onto S3
        self.wrangler_asset_path = self._deploy_wrangler_zip("AWS SDK for pandas")

        # Create Glue Job
        self._create_glue_job(id="Glue Job 1", script_path="glue_scripts/glue_example_1.py")

    def _deploy_glue_asset(self, id: str, path: str) -> s3_assets.Asset:
        return s3_assets.Asset(
            self,
            id,
            path=path,
            readers=[self.glue_service_role],
        )

    def _deploy_wrangler_zip(self, id: str) -> str:
        destination_key = "awswrangler.zip"
        asset_location = s3_assets.Asset(self, id, path="../awswrangler")

        call = cr.AwsSdkCall(
            service="S3",
            action="copyObject",
            parameters={
                "CopySource": f"{asset_location.s3_bucket_name}/{asset_location.s3_object_key}",
                "Bucket": self.script_bucket.bucket_name,
                "Key": destination_key,
            },
            physical_resource_id=cr.PhysicalResourceId.of(
                destination_key,
            ),
        )

        custom_resource = cr.AwsCustomResource(
            self,
            f"{id} Copy Action",
            on_create=call,
            on_update=call,
            policy=cr.AwsCustomResourcePolicy.from_sdk_calls(
                resources=cr.AwsCustomResourcePolicy.ANY_RESOURCE,
            ),
        )
        asset_location.grant_read(custom_resource)
        self.script_bucket.grant_put(custom_resource, objects_key_pattern=destination_key)

        return self.script_bucket.s3_url_for_object(destination_key)

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
                "--extra-py-files": self.wrangler_asset_path,
                "--auto-scaling-ray-min-workers": "5",
                "--glue-ray-data-bucket": self.data_bucket.bucket_name,
                "--athena-workgroup": self.athena_workgroup.ref,
            },
            glue_version="4.0",
            role=self.glue_service_role.role_name,
            worker_type="Z.2X",
            number_of_workers=5,
        )
