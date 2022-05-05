from aws_cdk import Stack
from aws_cdk import aws_iam as iam
from constructs import Construct


class RayStack(Stack):  # type: ignore
    def __init__(self, scope: Construct, construct_id: str, **kwargs: str) -> None:
        """
        Ray Cluster Infrastructure.
        Includes IAM role and instance profile.
        """
        super().__init__(scope, construct_id, **kwargs)

        # Ray execution Role
        ray_exec_role = iam.Role(
            self,
            "ray-execution-role",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonEC2FullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("CloudWatchFullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSSMFullAccess"),
            ],
        )

        # Add IAM pass role for a head instance to launch worker nodes
        # w/ an instance profile
        iam.Policy(
            self,
            "ray-execution-role-policy-pass-role",
            policy_name="IAMPassRole",
            roles=[ray_exec_role],
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW, actions=["iam:PassRole"], resources=[ray_exec_role.role_arn]
                ),
            ],
        )

        # Add instance profile
        iam.CfnInstanceProfile(
            self,
            "ray-instance-profile",
            roles=[ray_exec_role.role_name],
            instance_profile_name="ray-cluster-instance-profile",
        )
