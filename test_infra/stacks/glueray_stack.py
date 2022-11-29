import json

from aws_cdk import RemovalPolicy, Stack
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

        amazon_reviews_pds_bucket = s3.Bucket.from_bucket_name(
            self,
            "amazon_reviews_pds",
            bucket_name="amazon-reviews-pds",
        )
        amazon_reviews_pds_bucket.grant_read(self.glue_service_role)

        glue_job_script_1 = s3_assets.Asset(
            self,
            "Glue Script 1",
            path="glue_scripts/glue_example_1.py",
        )
        glue_job_script_1.grant_read(self.glue_service_role)

        self.glue_job_1 = glue.CfnJob(
            self,
            "Glue Job 1",
            command=glue.CfnJob.JobCommandProperty(
                name="glueray",
                python_version="3.9",
                script_location=glue_job_script_1.s3_object_url,
            ),
            default_arguments={
                "--auto-scaling-ray-min-workers": "5",
                "--glue-ray-data-bucket": self.data_bucket.bucket_name,
                "--athena-workgroup": self.athena_workgroup.ref,
            },
            glue_version="4.0",
            role=self.glue_service_role.role_name,
            worker_type="Z.2X",
            number_of_workers=5,
        )
