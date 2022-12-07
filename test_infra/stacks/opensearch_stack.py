from aws_cdk import CfnOutput, RemovalPolicy, Stack
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_iam as iam
from aws_cdk import aws_kms as kms
from aws_cdk import aws_opensearchservice as opensearch
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_secretsmanager as secrets
from constructs import Construct


def validate_domain_name(name: str):
    if not 3 <= len(name) <= 28:
        raise ValueError(f"invalid domain name ({name}) - bad length ({len(name)})")
    for c in name:
        if not ("a" <= c <= "z" or c.isdigit() or c in ["-"]):
            raise ValueError(f'invalid domain name ({name}) - bad character ("{c}")')


class OpenSearchStack(Stack):  # type: ignore
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        vpc: ec2.IVpc,
        bucket: s3.IBucket,
        key: kms.Key,
        **kwargs: str,
    ) -> None:
        """
        AWS SDK for pandas Development OpenSearch Infrastructure.
        Includes OpenSearch, Elasticsearch, ...
        """
        super().__init__(scope, construct_id, **kwargs)

        self.vpc = vpc
        self.key = key
        self.bucket = bucket

        self._set_opensearch_infra()
        self._setup_opensearch_1()
        self._setup_elasticsearch_7_10_fgac()

    def _set_opensearch_infra(self) -> None:
        self.username = "test"
        # fmt: off
        self.password_secret = secrets.Secret(
            self,
            "opensearch-password-secret",
            secret_name="aws-sdk-pandas/opensearch_password",
            generate_secret_string=secrets.SecretStringGenerator(exclude_characters="/@\"\' \\"),
        ).secret_value
        # fmt: on
        self.password = self.password_secret.to_string()
        if self.node.try_get_context("network") == "public":
            self.connectivity = {}
        else:
            self.connectivity = {"vpc": self.vpc, "vpc_subnets": [{"subnets": [self.vpc.private_subnets[0]]}]}

    def _setup_opensearch_1(self) -> None:
        domain_name = "sdk-pandas-os-1"
        validate_domain_name(domain_name)
        domain_arn = f"arn:aws:es:{self.region}:{self.account}:domain/{domain_name}"
        domain = opensearch.Domain(
            self,
            domain_name,
            domain_name=domain_name,
            version=opensearch.EngineVersion.OPENSEARCH_1_3,
            capacity=opensearch.CapacityConfig(data_node_instance_type="t3.medium.search", data_nodes=1),
            **self.connectivity,
            access_policies=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["es:*"],
                    principals=[iam.AccountRootPrincipal()],
                    resources=[f"{domain_arn}/*"],
                )
            ],
            removal_policy=RemovalPolicy.DESTROY,
        )

        CfnOutput(self, "DomainEndpoint-sdk-pandas-os-1", value=domain.domain_endpoint)

    def _setup_elasticsearch_7_10_fgac(self) -> None:
        domain_name = "sdk-pandas-es-7-10-fgac"
        validate_domain_name(domain_name)
        domain_arn = f"arn:aws:es:{self.region}:{self.account}:domain/{domain_name}"
        domain = opensearch.Domain(
            self,
            domain_name,
            domain_name=domain_name,
            version=opensearch.EngineVersion.ELASTICSEARCH_7_10,
            capacity=opensearch.CapacityConfig(data_node_instance_type="t3.medium.search", data_nodes=1),
            **self.connectivity,
            access_policies=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["es:*"],
                    principals=[iam.AnyPrincipal()],  # FGACs
                    resources=[f"{domain_arn}/*"],
                )
            ],
            fine_grained_access_control=opensearch.AdvancedSecurityOptions(
                master_user_name=self.username,
                master_user_password=self.password_secret,
            ),
            node_to_node_encryption=True,
            encryption_at_rest=opensearch.EncryptionAtRestOptions(enabled=True, kms_key=self.key),
            enforce_https=True,
            removal_policy=RemovalPolicy.DESTROY,
        )

        CfnOutput(self, "DomainEndpoint-sdkpandases710fgac", value=domain.domain_endpoint)
