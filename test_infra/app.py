#!/usr/bin/env python3
import os

from aws_cdk import App, Environment
from stacks.base_stack import BaseStack
from stacks.databases_stack import DatabasesStack
from stacks.lakeformation_stack import LakeFormationStack
from stacks.opensearch_stack import OpenSearchStack
from stacks.ray_stack import RayStack

app = App()

env = {"env": Environment(account=os.environ["CDK_DEFAULT_ACCOUNT"], region=os.environ["CDK_DEFAULT_REGION"])}

base = BaseStack(
    app,
    "aws-sdk-pandas-base",
    **env,
)

DatabasesStack(
    app,
    "aws-sdk-pandas-databases",
    base.get_vpc,
    base.get_bucket,
    base.get_key,
    **env,
)

LakeFormationStack(
    app,
    "aws-sdk-pandas-lakeformation",
    **env,
)

OpenSearchStack(
    app,
    "aws-sdk-pandas-opensearch",
    base.get_vpc,
    base.get_bucket,
    base.get_key,
    **env,
)

RayStack(app, "aws-sdk-pandas-ray")


app.synth()
