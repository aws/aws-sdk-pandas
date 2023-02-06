import json
from typing import Any, Dict, List, Optional

from aws_cdk import CfnOutput, RemovalPolicy, Stack
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_iam as iam
from aws_cdk import aws_kms as kms
from aws_cdk import aws_opensearchserverless as opensearchserverless
from aws_cdk import aws_opensearchservice as opensearch
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_secretsmanager as secrets
from aws_cdk import aws_ssm as ssm
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
        self._setup_opensearch_serverless()

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
            enforce_https=True,
            node_to_node_encryption=True,
            encryption_at_rest=opensearch.EncryptionAtRestOptions(
                enabled=True,
            ),
            removal_policy=RemovalPolicy.DESTROY,
        )

        CfnOutput(self, "DomainEndpointsdkpandasos1", value=domain.domain_endpoint).override_logical_id(
            "DomainEndpointsdkpandasos1"
        )

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

        CfnOutput(self, "DomainEndpointsdkpandases710fgac", value=domain.domain_endpoint).override_logical_id(
            "DomainEndpointsdkpandases710fgac"
        )

    def _setup_opensearch_serverless(self) -> None:
        collection_name = "sdk-pandas-aoss-1"
        self.cfn_collection = opensearchserverless.CfnCollection(
            self,
            collection_name,
            name=collection_name,
            type="SEARCH",
        )

        key = kms.Key(
            self,
            f"{collection_name}-key",
            removal_policy=RemovalPolicy.DESTROY,
            alias=f"{collection_name}-key",
            enable_key_rotation=True,
        )

        cfn_encryption_policy = opensearchserverless.CfnSecurityPolicy(
            self,
            f"{collection_name}-encryption",
            name=f"{collection_name}-encryption",
            type="encryption",
            policy=self._get_encryption_policy(
                collection_name=self.cfn_collection.name,
                kms_key_arn=key.key_arn,
            ),
        )

        cfn_network_policy = opensearchserverless.CfnSecurityPolicy(
            self,
            f"{collection_name}-network",
            name=f"{collection_name}-network",
            type="network",
            policy=self._get_network_policy(
                collection_name=self.cfn_collection.name,
            ),
        )

        self.cfn_collection.add_depends_on(cfn_encryption_policy)
        self.cfn_collection.add_depends_on(cfn_network_policy)

        CfnOutput(
            self,
            "CollectionNamesdkpandasaoss",
            value=self.cfn_collection.name,
        ).override_logical_id("CollectionNamesdkpandasaoss")
        CfnOutput(
            self,
            "CollectionEndpointsdkpandasaoss",
            value=str(self.cfn_collection.attr_collection_endpoint).replace("https://", ""),
        ).override_logical_id("CollectionEndpointsdkpandasaoss")

    @staticmethod
    def _get_encryption_policy(collection_name: str, kms_key_arn: Optional[str] = None) -> str:
        policy: Dict[str, Any] = {
            "Rules": [
                {
                    "ResourceType": "collection",
                    "Resource": [
                        f"collection/{collection_name}",
                    ],
                }
            ],
        }
        if kms_key_arn:
            policy["KmsARN"] = kms_key_arn
        else:
            policy["AWSOwnedKey"] = True
        return json.dumps(policy)

    @staticmethod
    def _get_network_policy(collection_name: str, vpc_endpoints: Optional[List[str]] = None) -> str:
        policy: List[Dict[str, Any]] = [
            {
                "Rules": [
                    {
                        "ResourceType": "dashboard",
                        "Resource": [
                            f"collection/{collection_name}",
                        ],
                    },
                    {
                        "ResourceType": "collection",
                        "Resource": [
                            f"collection/{collection_name}",
                        ],
                    },
                ],
            }
        ]
        if vpc_endpoints:
            policy[0]["SourceVPCEs"] = vpc_endpoints
        else:
            policy[0]["AllowFromPublic"] = True
        return json.dumps(policy)
