#!/usr/bin/env bash
set -ex

cfn-lint -t cloudformation.yaml
rm -rf temp.yaml
cfn-flip -c -l -n cloudformation.yaml temp.yaml
cfn-lint -t temp.yaml
mv temp.yaml cloudformation.yaml

aws cloudformation deploy \
  --template-file cloudformation.yaml \
  --stack-name aws-data-wrangler-test \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides $(cat parameters-dev.properties)

aws cloudformation update-termination-protection \
  --enable-termination-protection \
  --stack-name aws-data-wrangler-test
