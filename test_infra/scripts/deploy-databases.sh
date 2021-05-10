#!/usr/bin/env bash
set -e

pushd ..
cdk bootstrap
cdk deploy aws-data-wrangler-databases
popd
