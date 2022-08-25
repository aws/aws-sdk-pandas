#!/usr/bin/env bash
set -e
STACK=${1}

pushd ..
cdk bootstrap
cdk deploy aws-sdk-pandas-${STACK}
popd