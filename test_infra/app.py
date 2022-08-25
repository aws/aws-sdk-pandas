#!/usr/bin/env python3
from aws_cdk import App
from stacks.base_stack import BaseStack
from stacks.databases_stack import DatabasesStack
from stacks.lakeformation_stack import LakeFormationStack
from stacks.opensearch_stack import OpenSearchStack

app = App()

base = BaseStack(app, "aws-sdk-pandas-base")

DatabasesStack(
    app,
    "aws-sdk-pandas-databases",
    base.get_vpc,
    base.get_bucket,
    base.get_key,
)

LakeFormationStack(app, "aws-sdk-pandas-lakeformation")

OpenSearchStack(
    app,
    "aws-sdk-pandas-opensearch",
    base.get_vpc,
    base.get_bucket,
    base.get_key,
)

app.synth()
