#!/usr/bin/env bash
set -e

# Linting and formatting the base stack
cfn-lint -t base.yaml
rm -rf temp.yaml
cfn-flip -c -l -n base.yaml temp.yaml
cfn-lint -t temp.yaml
mv temp.yaml base.yaml

# Deploying
aws cloudformation deploy \
  --template-file base.yaml \
  --stack-name aws-data-wrangler-base \
  --capabilities CAPABILITY_IAM
