#!/usr/bin/env bash
set -e

aws cloudformation deploy \
--template-file template.yaml \
--stack-name aws-data-wrangler-test-arena \
--capabilities CAPABILITY_IAM \
--parameter-overrides $(cat parameters.properties)
