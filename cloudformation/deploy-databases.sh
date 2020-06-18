#!/usr/bin/env bash
set -e

# Linting and formatting the base stack
cfn-lint -t databases.yaml
rm -rf temp.yaml
cfn-flip -c -l -n databases.yaml temp.yaml
cfn-lint -t temp.yaml
mv temp.yaml databases.yaml

read -rp "Databases password [123456Ab]: " password
password=${password:-123456Ab}

# Deploying
aws cloudformation deploy \
  --template-file databases.yaml \
  --stack-name aws-data-wrangler-databases \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides DatabasesPassword="$password"
