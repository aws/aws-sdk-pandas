from aws_cdk import CfnOutput, Stack
from aws_cdk import aws_cleanrooms as cleanrooms
from aws_cdk import aws_glue_alpha as glue
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from constructs import Construct


class CleanRoomsStack(Stack):  # type: ignore
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        bucket: s3.Bucket,
        **kwargs: str,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.collaboration = cleanrooms.CfnCollaboration(
            self,
            "Collaboration",
            name="AWS SDK for pandas - Testing",
            creator_display_name="Collaborator Creator",
            creator_member_abilities=["CAN_QUERY", "CAN_RECEIVE_RESULTS"],
            description="Collaboration Room for AWS SDK for pandas test infrastructure",
            members=[],
            query_log_status="ENABLED",
            analytics_engine="SPARK",
        )

        self.membership = cleanrooms.CfnMembership(
            self,
            "Membership",
            collaboration_identifier=self.collaboration.attr_collaboration_identifier,
            query_log_status="ENABLED",
        )

        self.cleanrooms_service_role = iam.Role(
            self,
            "Service Role",
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("cleanrooms.amazonaws.com").with_conditions(
                    {
                        "StringLike": {
                            "sts:ExternalId": f"arn:aws:*:{self.region}:*:dbuser:*/{self.membership.attr_membership_identifier}*"
                        }
                    }
                ),
                iam.ServicePrincipal("cleanrooms.amazonaws.com").with_conditions(
                    {
                        "ForAnyValue:ArnEquals": {
                            "aws:SourceArn": f"arn:aws:cleanrooms:{self.region}:{self.account}:membership/{self.membership.attr_membership_identifier}"
                        }
                    }
                ),
            ),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3ReadOnlyAccess"),
            ],
        )

        self.database = glue.Database(
            self,
            id="Glue Database",
            database_name="aws_sdk_pandas_cleanrooms",
            location_uri=f"s3://{bucket.bucket_name}",
        )

        self.users_table = glue.Table(
            self,
            "Users Table",
            database=self.database,
            table_name="users",
            columns=[
                glue.Column(name="user_id", type=glue.Type(input_string="int", is_primitive=True)),
                glue.Column(name="city", type=glue.Type(input_string="string", is_primitive=True)),
            ],
            bucket=bucket,
            s3_prefix="users",
            data_format=glue.DataFormat(
                input_format=glue.InputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"),
                output_format=glue.OutputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"),
                serialization_library=glue.SerializationLibrary(
                    "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                ),
            ),
        )

        self.purchases_table = glue.Table(
            self,
            "Purchases Table",
            database=self.database,
            table_name="purchases",
            columns=[
                glue.Column(name="purchase_id", type=glue.Type(input_string="int", is_primitive=True)),
                glue.Column(name="user_id", type=glue.Type(input_string="int", is_primitive=True)),
                glue.Column(name="sale_value", type=glue.Type(input_string="float", is_primitive=True)),
            ],
            bucket=bucket,
            s3_prefix="purchases",
            data_format=glue.DataFormat(
                input_format=glue.InputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"),
                output_format=glue.OutputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"),
                serialization_library=glue.SerializationLibrary(
                    "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                ),
            ),
        )

        self.custom_table = glue.Table(
            self,
            "Custom Table",
            database=self.database,
            table_name="custom",
            columns=[
                glue.Column(name="a", type=glue.Type(input_string="int", is_primitive=True)),
                glue.Column(name="b", type=glue.Type(input_string="string", is_primitive=True)),
            ],
            bucket=bucket,
            s3_prefix="custom",
            data_format=glue.DataFormat(
                input_format=glue.InputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"),
                output_format=glue.OutputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"),
                serialization_library=glue.SerializationLibrary(
                    "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                ),
            ),
        )

        self.users_configured_table = cleanrooms.CfnConfiguredTable(
            self,
            "Users Configured Table",
            allowed_columns=["user_id", "city"],
            analysis_method="DIRECT_QUERY",
            name="users",
            table_reference=cleanrooms.CfnConfiguredTable.TableReferenceProperty(
                glue=cleanrooms.CfnConfiguredTable.GlueTableReferenceProperty(
                    database_name=self.database.database_name,
                    table_name=self.users_table.table_name,
                )
            ),
            analysis_rules=[
                cleanrooms.CfnConfiguredTable.AnalysisRuleProperty(
                    policy=cleanrooms.CfnConfiguredTable.ConfiguredTableAnalysisRulePolicyProperty(
                        v1=cleanrooms.CfnConfiguredTable.ConfiguredTableAnalysisRulePolicyV1Property(
                            aggregation=cleanrooms.CfnConfiguredTable.AnalysisRuleAggregationProperty(
                                aggregate_columns=[
                                    cleanrooms.CfnConfiguredTable.AggregateColumnProperty(
                                        column_names=["user_id"], function="COUNT"
                                    )
                                ],
                                dimension_columns=["city"],
                                join_columns=["user_id"],
                                output_constraints=[
                                    cleanrooms.CfnConfiguredTable.AggregationConstraintProperty(
                                        column_name="user_id", minimum=2, type="COUNT_DISTINCT"
                                    )
                                ],
                                scalar_functions=["LOWER"],
                                join_required="QUERY_RUNNER",
                            ),
                        )
                    ),
                    type="AGGREGATION",
                )
            ],
        )

        self.purchases_configured_table = cleanrooms.CfnConfiguredTable(
            self,
            "Purchases Configured Table",
            allowed_columns=["purchase_id", "user_id", "sale_value"],
            analysis_method="DIRECT_QUERY",
            name="purchases",
            table_reference=cleanrooms.CfnConfiguredTable.TableReferenceProperty(
                glue=cleanrooms.CfnConfiguredTable.GlueTableReferenceProperty(
                    database_name=self.database.database_name,
                    table_name=self.purchases_table.table_name,
                )
            ),
            analysis_rules=[
                cleanrooms.CfnConfiguredTable.AnalysisRuleProperty(
                    policy=cleanrooms.CfnConfiguredTable.ConfiguredTableAnalysisRulePolicyProperty(
                        v1=cleanrooms.CfnConfiguredTable.ConfiguredTableAnalysisRulePolicyV1Property(
                            aggregation=cleanrooms.CfnConfiguredTable.AnalysisRuleAggregationProperty(
                                aggregate_columns=[
                                    cleanrooms.CfnConfiguredTable.AggregateColumnProperty(
                                        column_names=["purchase_id"], function="COUNT"
                                    ),
                                    cleanrooms.CfnConfiguredTable.AggregateColumnProperty(
                                        column_names=["sale_value"], function="AVG"
                                    ),
                                    cleanrooms.CfnConfiguredTable.AggregateColumnProperty(
                                        column_names=["sale_value"], function="SUM"
                                    ),
                                ],
                                dimension_columns=[],
                                join_columns=["user_id"],
                                output_constraints=[
                                    cleanrooms.CfnConfiguredTable.AggregationConstraintProperty(
                                        column_name="user_id", minimum=2, type="COUNT_DISTINCT"
                                    )
                                ],
                                scalar_functions=[],
                                join_required="QUERY_RUNNER",
                            ),
                        )
                    ),
                    type="AGGREGATION",
                )
            ],
        )

        self.analysis_template = cleanrooms.CfnAnalysisTemplate(
            self,
            "AnalysisTemplate",
            format="SQL",
            membership_identifier=self.membership.attr_membership_identifier,
            name="custom_analysis",
            source=cleanrooms.CfnAnalysisTemplate.AnalysisSourceProperty(
                text="SELECT a FROM custom WHERE custom.b = :param1"
            ),
            analysis_parameters=[
                cleanrooms.CfnAnalysisTemplate.AnalysisParameterProperty(
                    name="param1",
                    type="VARCHAR",
                )
            ],
        )

        self.custom_configured_table = cleanrooms.CfnConfiguredTable(
            self,
            "Custom Configured Table",
            allowed_columns=["a", "b"],
            analysis_method="DIRECT_QUERY",
            name="custom",
            table_reference=cleanrooms.CfnConfiguredTable.TableReferenceProperty(
                glue=cleanrooms.CfnConfiguredTable.GlueTableReferenceProperty(
                    database_name=self.database.database_name,
                    table_name=self.custom_table.table_name,
                )
            ),
            analysis_rules=[
                cleanrooms.CfnConfiguredTable.AnalysisRuleProperty(
                    policy=cleanrooms.CfnConfiguredTable.ConfiguredTableAnalysisRulePolicyProperty(
                        v1=cleanrooms.CfnConfiguredTable.ConfiguredTableAnalysisRulePolicyV1Property(
                            custom=cleanrooms.CfnConfiguredTable.AnalysisRuleCustomProperty(
                                allowed_analyses=[self.analysis_template.attr_arn],
                            ),
                        )
                    ),
                    type="CUSTOM",
                )
            ],
        )

        self.users_configured_table_association = cleanrooms.CfnConfiguredTableAssociation(
            self,
            "Users Configured Table Association",
            configured_table_identifier=self.users_configured_table.attr_configured_table_identifier,
            membership_identifier=self.membership.attr_membership_identifier,
            name="users",
            role_arn=self.cleanrooms_service_role.role_arn,
        )

        self.purchases_configured_table_association = cleanrooms.CfnConfiguredTableAssociation(
            self,
            "Purchases Configured Table Association",
            configured_table_identifier=self.purchases_configured_table.attr_configured_table_identifier,
            membership_identifier=self.membership.attr_membership_identifier,
            name="purchases",
            role_arn=self.cleanrooms_service_role.role_arn,
        )

        self.custom_configured_table_association = cleanrooms.CfnConfiguredTableAssociation(
            self,
            "Custom Configured Table Association",
            configured_table_identifier=self.custom_configured_table.attr_configured_table_identifier,
            membership_identifier=self.membership.attr_membership_identifier,
            name="custom",
            role_arn=self.cleanrooms_service_role.role_arn,
        )

        CfnOutput(self, "CleanRoomsMembershipId", value=self.membership.attr_membership_identifier)
        CfnOutput(self, "CleanRoomsAnalysisTemplateArn", value=self.analysis_template.attr_arn)
        CfnOutput(self, "CleanRoomsGlueDatabaseName", value=self.database.database_name)
