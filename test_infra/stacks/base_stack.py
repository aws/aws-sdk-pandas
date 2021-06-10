from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from aws_cdk import aws_kms as kms
from aws_cdk import aws_logs as logs
from aws_cdk import aws_s3 as s3
from aws_cdk import core as cdk


class BaseStack(cdk.Stack):  # type: ignore
    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs: str) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.vpc = ec2.Vpc(
            self,
            "aws-data-wrangler-vpc",
            cidr="11.19.224.0/19",
            enable_dns_hostnames=True,
            enable_dns_support=True,
        )
        cdk.Tags.of(self.vpc).add("Name", "aws-data-wrangler")
        self.key = kms.Key(
            self,
            id="aws-data-wrangler-key",
            description="Aws Data Wrangler Test Key.",
            policy=iam.PolicyDocument(
                statements=[
                    iam.PolicyStatement(
                        sid="Enable IAM User Permissions",
                        effect=iam.Effect.ALLOW,
                        actions=["kms:*"],
                        principals=[iam.AccountRootPrincipal()],
                        resources=["*"],
                    )
                ]
            ),
        )
        kms.Alias(
            self,
            "aws-data-wrangler-key-alias",
            alias_name="alias/aws-data-wrangler-key",
            target_key=self.key,
        )
        self.bucket = s3.Bucket(
            self,
            id="aws-data-wrangler",
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
        )
        glue_db = glue.Database(
            self,
            id="aws_data_wrangler_glue_database",
            database_name="aws_data_wrangler",
        )
        log_group = logs.LogGroup(
            self,
            id="aws_data_wrangler_log_group",
            retention=logs.RetentionDays.ONE_MONTH,
        )
        log_stream = logs.LogStream(
            self,
            id="aws_data_wrangler_log_stream",
            log_group=log_group,
        )
        cdk.CfnOutput(self, "Region", value=self.region)
        cdk.CfnOutput(
            self,
            "VPC",
            value=self.vpc.vpc_id,
            export_name="aws-data-wrangler-base-VPC",
        )
        cdk.CfnOutput(
            self,
            "PublicSubnet1",
            value=self.vpc.public_subnets[0].subnet_id,
            export_name="aws-data-wrangler-base-PublicSubnet1",
        )
        cdk.CfnOutput(
            self,
            "PublicSubnet2",
            value=self.vpc.public_subnets[1].subnet_id,
            export_name="aws-data-wrangler-base-PublicSubnet2",
        )
        cdk.CfnOutput(
            self,
            "PrivateSubnet",
            value=self.vpc.private_subnets[0].subnet_id,
            export_name="aws-data-wrangler-base-PrivateSubnet",
        )
        cdk.CfnOutput(
            self,
            "KmsKeyArn",
            value=self.key.key_arn,
            export_name="aws-data-wrangler-base-KmsKeyArn",
        )
        cdk.CfnOutput(
            self,
            "BucketName",
            value=self.bucket.bucket_name,
            export_name="aws-data-wrangler-base-BucketName",
        )
        cdk.CfnOutput(self, "GlueDatabaseName", value=glue_db.database_name)
        cdk.CfnOutput(self, "LogGroupName", value=log_group.log_group_name)
        cdk.CfnOutput(self, "LogStream", value=log_stream.log_stream_name)

    @property
    def get_bucket(self) -> s3.Bucket:
        return self.bucket

    @property
    def get_vpc(self) -> ec2.Vpc:
        return self.vpc

    @property
    def get_key(self) -> kms.Key:
        return self.key
