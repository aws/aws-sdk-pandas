import json

from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from aws_cdk import aws_kms as kms
from aws_cdk import aws_lakeformation as lf
from aws_cdk import aws_neptune as neptune
from aws_cdk import aws_rds as rds
from aws_cdk import aws_redshift as redshift
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_secretsmanager as secrets
from aws_cdk import aws_ssm as ssm
from aws_cdk import core as cdk


class DatabasesStack(cdk.Stack):  # type: ignore
    def __init__(
        self,
        scope: cdk.Construct,
        construct_id: str,
        vpc: ec2.IVpc,
        bucket: s3.IBucket,
        key: kms.Key,
        **kwargs: str,
    ) -> None:
        """
        AWS Data Wrangler Development Databases Infrastructure.
        Includes Redshift, Aurora PostgreSQL, Aurora MySQL, Microsoft SQL Server, Oracle Database.
        """
        super().__init__(scope, construct_id, **kwargs)

        self.vpc = vpc
        self.key = key
        self.bucket = bucket

        databases_context = self.node.try_get_context("databases")

        self._set_db_infra()
        self._set_catalog_encryption()
        if databases_context["redshift"]:
            self._setup_redshift()
        if databases_context["postgresql"]:
            self._setup_postgresql()
        if databases_context["mysql"]:
            self._setup_mysql()
            self._setup_mysql_serverless()
        if databases_context["sqlserver"]:
            self._setup_sqlserver()
        if databases_context["oracle"]:
            self._setup_oracle()
        if databases_context["neptune"]:
            self._setup_neptune()

    def _set_db_infra(self) -> None:
        self.db_username = "test"
        # fmt: off
        self.db_password_secret = secrets.Secret(
            self,
            "db-password-secret",
            secret_name="aws-data-wrangler/db_password",
            generate_secret_string=secrets.SecretStringGenerator(exclude_characters="/@\"\' \\", password_length=30),
        ).secret_value
        # fmt: on
        self.db_password = self.db_password_secret.to_string()
        self.db_security_group = ec2.SecurityGroup(
            self,
            "aws-data-wrangler-database-sg",
            vpc=self.vpc,
            description="AWS Data Wrangler Test Athena - Database security group",
        )
        self.db_security_group.add_ingress_rule(self.db_security_group, ec2.Port.all_traffic())
        ssm.StringParameter(
            self,
            "db-security-group-parameter",
            parameter_name="/Wrangler/EC2/DatabaseSecurityGroupId",
            string_value=self.db_security_group.security_group_id,
        )
        self.rds_subnet_group = rds.SubnetGroup(
            self,
            "aws-data-wrangler-rds-subnet-group",
            description="RDS Database Subnet Group",
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
        )
        self.rds_role = iam.Role(
            self,
            "aws-data-wrangler-rds-role",
            assumed_by=iam.ServicePrincipal("rds.amazonaws.com"),
            inline_policies={
                "S3": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "s3:Get*",
                                "s3:List*",
                                "s3:Put*",
                                "s3:AbortMultipartUpload",
                            ],
                            resources=[
                                self.bucket.bucket_arn,
                                f"{self.bucket.bucket_arn}/*",
                            ],
                        )
                    ]
                ),
            },
        )
        cdk.CfnOutput(self, "DatabasesUsername", value=self.db_username)
        cdk.CfnOutput(
            self,
            "DatabaseSecurityGroupId",
            value=self.db_security_group.security_group_id,
        )

    def _set_catalog_encryption(self) -> None:
        glue.CfnDataCatalogEncryptionSettings(
            self,
            "aws-data-wrangler-catalog-encryption",
            catalog_id=cdk.Aws.ACCOUNT_ID,
            data_catalog_encryption_settings=glue.CfnDataCatalogEncryptionSettings.DataCatalogEncryptionSettingsProperty(  # noqa: E501
                encryption_at_rest=glue.CfnDataCatalogEncryptionSettings.EncryptionAtRestProperty(
                    catalog_encryption_mode="DISABLED",
                ),
                connection_password_encryption=glue.CfnDataCatalogEncryptionSettings.ConnectionPasswordEncryptionProperty(  # noqa: E501
                    kms_key_id=self.key.key_id,
                    return_connection_password_encrypted=True,
                ),
            ),
        )

    def _setup_redshift(self) -> None:
        port = 5439
        database = "test"
        schema = "public"
        redshift_role = iam.Role(
            self,
            "aws-data-wrangler-redshift-role",
            assumed_by=iam.ServicePrincipal("redshift.amazonaws.com"),
            inline_policies={
                "KMS": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "kms:Encrypt",
                                "kms:Decrypt",
                                "kms:GenerateDataKey",
                            ],
                            resources=[self.key.key_arn],
                        )
                    ]
                ),
                "S3": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "s3:Get*",
                                "s3:List*",
                                "s3:Put*",
                            ],
                            resources=[
                                self.bucket.bucket_arn,
                                f"{self.bucket.bucket_arn}/*",
                            ],
                        )
                    ]
                ),
                "LakeFormation": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "lakeformation:GetDataAccess",
                                "lakeformation:GrantPermissions",
                                "lakeformation:GetWorkUnits",
                                "lakeformation:StartQueryPlanning",
                                "lakeformation:GetWorkUnitResults",
                                "lakeformation:GetQueryState",
                            ],
                            resources=["*"],
                        )
                    ]
                ),
                "Glue": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "glue:SearchTables",
                                "glue:GetConnections",
                                "glue:GetDataCatalogEncryptionSettings",
                                "glue:GetTables",
                                "glue:GetTableVersions",
                                "glue:GetPartitions",
                                "glue:DeleteTableVersion",
                                "glue:BatchGetPartition",
                                "glue:GetDatabases",
                                "glue:GetTags",
                                "glue:GetTable",
                                "glue:GetDatabase",
                                "glue:GetPartition",
                                "glue:GetTableVersion",
                                "glue:GetConnection",
                                "glue:GetUserDefinedFunction",
                                "glue:GetUserDefinedFunctions",
                            ],
                            resources=["*"],
                        )
                    ]
                ),
            },
        )
        lf.CfnPermissions(
            self,
            "CodeBuildTestRoleLFPermissions",
            data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=redshift_role.role_arn
            ),
            resource=lf.CfnPermissions.ResourceProperty(
                table_resource=lf.CfnPermissions.TableResourceProperty(
                    database_name="aws_data_wrangler",
                    table_wildcard={},  # type: ignore
                )
            ),
            permissions=["SELECT", "ALTER", "DESCRIBE", "DROP", "DELETE", "INSERT"],
        )
        redshift.ClusterSubnetGroup(
            self,
            "aws-data-wrangler-redshift-subnet-group",
            description="AWS Data Wrangler Test Athena - Redshift Subnet Group",
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
        )
        redshift_cluster = redshift.Cluster(
            self,
            "aws-data-wrangler-redshift-cluster",
            default_database_name=database,
            master_user=redshift.Login(
                master_username=self.db_username,
                master_password=self.db_password_secret,
            ),
            cluster_type=redshift.ClusterType.SINGLE_NODE,
            publicly_accessible=True,
            port=port,
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            security_groups=[self.db_security_group],
            roles=[redshift_role],
        )
        glue.Connection(
            self,
            "aws-data-wrangler-redshift-glue-connection",
            description="Connect to Redshift.",
            type=glue.ConnectionType.JDBC,
            connection_name="aws-data-wrangler-redshift",
            properties={
                "JDBC_CONNECTION_URL": f"jdbc:redshift://{redshift_cluster.cluster_endpoint.hostname}:{port}/{database}",  # noqa: E501
                "USERNAME": self.db_username,
                "PASSWORD": self.db_password,
            },
            subnet=self.vpc.private_subnets[0],
            security_groups=[self.db_security_group],
        )
        secret = secrets.Secret(
            self,
            "aws-data-wrangler-redshift-secret",
            secret_name="aws-data-wrangler/redshift",
            description="Redshift credentials",
            generate_secret_string=secrets.SecretStringGenerator(
                generate_string_key="dummy",
                secret_string_template=json.dumps(
                    {
                        "username": self.db_username,
                        "password": self.db_password,
                        "engine": "redshift",
                        "host": redshift_cluster.cluster_endpoint.hostname,
                        "port": port,
                        "dbClusterIdentifier": redshift_cluster.cluster_name,
                    }
                ),
            ),
        )
        cdk.CfnOutput(self, "RedshiftSecretArn", value=secret.secret_arn)
        cdk.CfnOutput(self, "RedshiftIdentifier", value=redshift_cluster.cluster_name)
        cdk.CfnOutput(
            self,
            "RedshiftAddress",
            value=redshift_cluster.cluster_endpoint.hostname,
        )
        cdk.CfnOutput(self, "RedshiftPort", value=str(port))
        cdk.CfnOutput(self, "RedshiftDatabase", value=database)
        cdk.CfnOutput(self, "RedshiftSchema", value=schema)
        cdk.CfnOutput(self, "RedshiftRole", value=redshift_role.role_arn)

    def _setup_postgresql(self) -> None:
        port = 3306
        database = "postgres"
        schema = "public"
        pg = rds.ParameterGroup(
            self,
            "aws-data-wrangler-postgresql-params",
            engine=rds.DatabaseClusterEngine.aurora_postgres(
                version=rds.AuroraPostgresEngineVersion.VER_11_13,
            ),
            parameters={
                "apg_plan_mgmt.capture_plan_baselines": "off",
            },
        )
        aurora_pg = rds.DatabaseCluster(
            self,
            "aws-data-wrangler-aurora-cluster-postgresql",
            removal_policy=cdk.RemovalPolicy.DESTROY,
            engine=rds.DatabaseClusterEngine.aurora_postgres(
                version=rds.AuroraPostgresEngineVersion.VER_11_13,
            ),
            cluster_identifier="postgresql-cluster-wrangler",
            instances=1,
            credentials=rds.Credentials.from_password(
                username=self.db_username,
                password=self.db_password_secret,
            ),
            port=port,
            backup=rds.BackupProps(retention=cdk.Duration.days(1)),
            parameter_group=pg,
            s3_import_buckets=[self.bucket],
            s3_export_buckets=[self.bucket],
            instance_props=rds.InstanceProps(
                vpc=self.vpc,
                security_groups=[self.db_security_group],
                publicly_accessible=True,
            ),
            subnet_group=self.rds_subnet_group,
        )
        glue.Connection(
            self,
            "aws-data-wrangler-postgresql-glue-connection",
            description="Connect to Aurora (PostgreSQL).",
            type=glue.ConnectionType.JDBC,
            connection_name="aws-data-wrangler-postgresql",
            properties={
                "JDBC_CONNECTION_URL": f"jdbc:postgresql://{aurora_pg.cluster_endpoint.hostname}:{port}/{database}",
                "USERNAME": self.db_username,
                "PASSWORD": self.db_password,
            },
            subnet=self.vpc.private_subnets[0],
            security_groups=[self.db_security_group],
        )
        secrets.Secret(
            self,
            "aws-data-wrangler-postgresql-secret",
            secret_name="aws-data-wrangler/postgresql",
            description="Postgresql credentials",
            generate_secret_string=secrets.SecretStringGenerator(
                generate_string_key="dummy",
                secret_string_template=json.dumps(
                    {
                        "username": self.db_username,
                        "password": self.db_password,
                        "engine": "postgresql",
                        "host": aurora_pg.cluster_endpoint.hostname,
                        "port": port,
                        "dbClusterIdentifier": aurora_pg.cluster_identifier,
                        "dbname": database,
                    }
                ),
            ),
        )
        cdk.CfnOutput(self, "PostgresqlAddress", value=aurora_pg.cluster_endpoint.hostname)
        cdk.CfnOutput(self, "PostgresqlPort", value=str(port))
        cdk.CfnOutput(self, "PostgresqlDatabase", value=database)
        cdk.CfnOutput(self, "PostgresqlSchema", value=schema)

    def _setup_mysql(self) -> None:
        port = 3306
        database = "test"
        schema = "test"
        aurora_mysql = rds.DatabaseCluster(
            self,
            "aws-data-wrangler-aurora-cluster-mysql",
            removal_policy=cdk.RemovalPolicy.DESTROY,
            engine=rds.DatabaseClusterEngine.aurora_mysql(
                version=rds.AuroraMysqlEngineVersion.VER_5_7_12,
            ),
            cluster_identifier="mysql-cluster-wrangler",
            instances=1,
            default_database_name=database,
            credentials=rds.Credentials.from_password(
                username=self.db_username,
                password=self.db_password_secret,
            ),
            port=port,
            backup=rds.BackupProps(retention=cdk.Duration.days(1)),
            instance_props=rds.InstanceProps(
                vpc=self.vpc,
                security_groups=[self.db_security_group],
                publicly_accessible=True,
            ),
            subnet_group=self.rds_subnet_group,
            s3_import_buckets=[self.bucket],
            s3_export_buckets=[self.bucket],
        )
        glue.Connection(
            self,
            "aws-data-wrangler-mysql-glue-connection",
            description="Connect to Aurora (MySQL).",
            type=glue.ConnectionType.JDBC,
            connection_name="aws-data-wrangler-mysql",
            properties={
                "JDBC_CONNECTION_URL": f"jdbc:mysql://{aurora_mysql.cluster_endpoint.hostname}:{port}/{database}",
                "USERNAME": self.db_username,
                "PASSWORD": self.db_password,
            },
            subnet=self.vpc.private_subnets[0],
            security_groups=[self.db_security_group],
        )
        glue.Connection(
            self,
            "aws-data-wrangler-mysql-glue-connection-ssl",
            description="Connect to Aurora (MySQL) with SSL.",
            type=glue.ConnectionType.JDBC,
            connection_name="aws-data-wrangler-mysql-ssl",
            properties={
                "JDBC_CONNECTION_URL": f"jdbc:mysql://{aurora_mysql.cluster_endpoint.hostname}:{port}/{database}",
                "USERNAME": self.db_username,
                "PASSWORD": self.db_password,
                "JDBC_ENFORCE_SSL": "true",
                "CUSTOM_JDBC_CERT": "s3://rds-downloads/rds-combined-ca-bundle.pem",
            },
            subnet=self.vpc.private_subnets[0],
            security_groups=[self.db_security_group],
        )
        secrets.Secret(
            self,
            "aws-data-wrangler-mysql-secret",
            secret_name="aws-data-wrangler/mysql",
            description="MySQL credentials",
            generate_secret_string=secrets.SecretStringGenerator(
                generate_string_key="dummy",
                secret_string_template=json.dumps(
                    {
                        "username": self.db_username,
                        "password": self.db_password,
                        "engine": "mysql",
                        "host": aurora_mysql.cluster_endpoint.hostname,
                        "port": port,
                        "dbClusterIdentifier": aurora_mysql.cluster_identifier,
                        "dbname": database,
                    }
                ),
            ),
        )
        cdk.CfnOutput(self, "MysqlAddress", value=aurora_mysql.cluster_endpoint.hostname)
        cdk.CfnOutput(self, "MysqlPort", value=str(port))
        cdk.CfnOutput(self, "MysqlDatabase", value=database)
        cdk.CfnOutput(self, "MysqlSchema", value=schema)

    def _setup_mysql_serverless(self) -> None:
        port = 3306
        database = "test"
        schema = "test"
        aurora_mysql = rds.ServerlessCluster(
            self,
            "aws-data-wrangler-aurora-cluster-mysql-serverless",
            removal_policy=cdk.RemovalPolicy.DESTROY,
            engine=rds.DatabaseClusterEngine.aurora_mysql(
                version=rds.AuroraMysqlEngineVersion.VER_5_7_12,
            ),
            cluster_identifier="mysql-serverless-cluster-wrangler",
            default_database_name=database,
            credentials=rds.Credentials.from_password(
                username=self.db_username,
                password=self.db_password_secret,
            ),
            scaling=rds.ServerlessScalingOptions(
                auto_pause=cdk.Duration.minutes(5),
                min_capacity=rds.AuroraCapacityUnit.ACU_1,
                max_capacity=rds.AuroraCapacityUnit.ACU_1,
            ),
            backup_retention=cdk.Duration.days(1),
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE),
            subnet_group=self.rds_subnet_group,
            security_groups=[self.db_security_group],
            enable_data_api=True,
        )
        secret = secrets.Secret(
            self,
            "aws-data-wrangler-mysql-serverless-secret",
            secret_name="aws-data-wrangler/mysql-serverless",
            description="MySQL serverless credentials",
            generate_secret_string=secrets.SecretStringGenerator(
                generate_string_key="dummy",
                secret_string_template=json.dumps(
                    {
                        "username": self.db_username,
                        "password": self.db_password,
                        "engine": "mysql",
                        "host": aurora_mysql.cluster_endpoint.hostname,
                        "port": port,
                        "dbClusterIdentifier": aurora_mysql.cluster_identifier,
                        "dbname": database,
                    }
                ),
            ),
        )
        cdk.CfnOutput(self, "MysqlServerlessSecretArn", value=secret.secret_arn)
        cdk.CfnOutput(self, "MysqlServerlessClusterArn", value=aurora_mysql.cluster_arn)
        cdk.CfnOutput(self, "MysqlServerlessAddress", value=aurora_mysql.cluster_endpoint.hostname)
        cdk.CfnOutput(self, "MysqlServerlessPort", value=str(port))
        cdk.CfnOutput(self, "MysqlServerlessDatabase", value=database)
        cdk.CfnOutput(self, "MysqlServerlessSchema", value=schema)

    def _setup_sqlserver(self) -> None:
        port = 1433
        database = "test"
        schema = "dbo"
        sqlserver = rds.DatabaseInstance(
            self,
            "aws-data-wrangler-sqlserver-instance",
            instance_identifier="sqlserver-instance-wrangler",
            engine=rds.DatabaseInstanceEngine.sql_server_ex(version=rds.SqlServerEngineVersion.VER_15),
            instance_type=ec2.InstanceType.of(ec2.InstanceClass.BURSTABLE3, ec2.InstanceSize.SMALL),
            credentials=rds.Credentials.from_password(
                username=self.db_username,
                password=self.db_password_secret,
            ),
            port=port,
            vpc=self.vpc,
            subnet_group=self.rds_subnet_group,
            security_groups=[self.db_security_group],
            publicly_accessible=True,
            s3_import_role=self.rds_role,
            s3_export_role=self.rds_role,
        )
        glue.Connection(
            self,
            "aws-data-wrangler-sqlserver-glue-connection",
            description="Connect to SQL Server.",
            type=glue.ConnectionType.JDBC,
            connection_name="aws-data-wrangler-sqlserver",
            properties={
                "JDBC_CONNECTION_URL": f"jdbc:sqlserver://{sqlserver.instance_endpoint.hostname}:{port};databaseName={database}",  # noqa: E501
                "USERNAME": self.db_username,
                "PASSWORD": self.db_password,
            },
            subnet=self.vpc.private_subnets[0],
            security_groups=[self.db_security_group],
        )
        secrets.Secret(
            self,
            "aws-data-wrangler-sqlserver-secret",
            secret_name="aws-data-wrangler/sqlserver",
            description="SQL Server credentials",
            generate_secret_string=secrets.SecretStringGenerator(
                generate_string_key="dummy",
                secret_string_template=json.dumps(
                    {
                        "username": self.db_username,
                        "password": self.db_password,
                        "engine": "sqlserver",
                        "host": sqlserver.instance_endpoint.hostname,
                        "port": port,
                        "dbClusterIdentifier": sqlserver.instance_identifier,
                        "dbname": database,
                    }
                ),
            ),
        )
        cdk.CfnOutput(self, "SqlServerAddress", value=sqlserver.instance_endpoint.hostname)
        cdk.CfnOutput(self, "SqlServerPort", value=str(port))
        cdk.CfnOutput(self, "SqlServerDatabase", value=database)
        cdk.CfnOutput(self, "SqlServerSchema", value=schema)

    def _setup_oracle(self) -> None:
        port = 1521
        database = "ORCL"
        schema = "TEST"
        oracle = rds.DatabaseInstance(
            self,
            "aws-data-wrangler-oracle-instance",
            instance_identifier="oracle-instance-wrangler",
            engine=rds.DatabaseInstanceEngine.oracle_ee(version=rds.OracleEngineVersion.VER_19_0_0_0_2021_04_R1),
            instance_type=ec2.InstanceType.of(ec2.InstanceClass.BURSTABLE3, ec2.InstanceSize.SMALL),
            credentials=rds.Credentials.from_password(
                username=self.db_username,
                password=self.db_password_secret,
            ),
            port=port,
            vpc=self.vpc,
            subnet_group=self.rds_subnet_group,
            security_groups=[self.db_security_group],
            publicly_accessible=True,
            s3_import_role=self.rds_role,
            s3_export_role=self.rds_role,
        )
        glue.Connection(
            self,
            "aws-data-wrangler-oracle-glue-connection",
            description="Connect to Oracle.",
            type=glue.ConnectionType.JDBC,
            connection_name="aws-data-wrangler-oracle",
            properties={
                "JDBC_CONNECTION_URL": f"jdbc:oracle:thin://@{oracle.instance_endpoint.hostname}:{port}/{database}",  # noqa: E501
                "USERNAME": self.db_username,
                "PASSWORD": self.db_password,
            },
            subnet=self.vpc.private_subnets[0],
            security_groups=[self.db_security_group],
        )
        secrets.Secret(
            self,
            "aws-data-wrangler-oracle-secret",
            secret_name="aws-data-wrangler/oracle",
            description="Oracle credentials",
            generate_secret_string=secrets.SecretStringGenerator(
                generate_string_key="dummy",
                secret_string_template=json.dumps(
                    {
                        "username": self.db_username,
                        "password": self.db_password,
                        "engine": "oracle",
                        "host": oracle.instance_endpoint.hostname,
                        "port": port,
                        "dbClusterIdentifier": oracle.instance_identifier,
                        "dbname": database,
                    }
                ),
            ),
        )
        cdk.CfnOutput(self, "OracleAddress", value=oracle.instance_endpoint.hostname)
        cdk.CfnOutput(self, "OraclePort", value=str(port))
        cdk.CfnOutput(self, "OracleDatabase", value=database)
        cdk.CfnOutput(self, "OracleSchema", value=schema)

    def _setup_neptune(self, iam_enabled: bool = False, port: int = 8182) -> None:
        cluster = neptune.DatabaseCluster(
            self,
            "DataWrangler",
            vpc=self.vpc,
            instance_type=neptune.InstanceType.R5_LARGE,
            iam_authentication=iam_enabled,
            security_groups=[self.db_security_group],
        )

        cdk.CfnOutput(self, "NeptuneClusterEndpoint", value=cluster.cluster_endpoint.hostname)
        cdk.CfnOutput(self, "NeptuneReaderEndpoint", value=cluster.cluster_read_endpoint.hostname)
        cdk.CfnOutput(self, "NeptunePort", value=str(port))
        cdk.CfnOutput(self, "NeptuneIAMEnabled", value=str(iam_enabled))
