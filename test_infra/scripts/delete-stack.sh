#!/usr/bin/env bash
set -e
STACK=${1}

pushd ..
cdk destroy aws-data-wrangler-${STACK}
popd