#!/usr/bin/env python3
import os

from aws_cdk import App, Environment
from stacks.base_stack import BaseStack
from stacks.databases_stack import DatabasesStack
from stacks.glueray_stack import GlueRayStack
from stacks.opensearch_stack import OpenSearchStack

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

OpenSearchStack(
    app,
    "aws-sdk-pandas-opensearch",
    base.get_vpc,
    base.get_bucket,
    base.get_key,
    **env,
)

GlueRayStack(
    app,
    "aws-sdk-pandas-glueray",
    base.get_bucket,
    **env,
)

app.synth()
