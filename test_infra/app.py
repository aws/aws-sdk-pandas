#!/usr/bin/env python3
from aws_cdk import core as cdk
from stacks.base_stack import BaseStack
from stacks.databases_stack import DatabasesStack

app = cdk.App()

base = BaseStack(app, "aws-data-wrangler-base")
DatabasesStack(
    app,
    "aws-data-wrangler-databases",
    base.get_vpc,
    base.get_bucket,
    base.get_key,
)

app.synth()
