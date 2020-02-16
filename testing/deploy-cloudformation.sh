#!/usr/bin/env bash
set -ex

aws cloudformation deploy \
  --template-file template.yaml \
  --stack-name aws-data-wrangler-test \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides $(cat parameters.properties)

aws cloudformation update-termination-protection \
  --enable-termination-protection \
  --stack-name aws-data-wrangler-test
