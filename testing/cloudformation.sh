#!/usr/bin/env bash
set -e

cfn-lint -t cloudformation.yaml
rm -rf temp.yaml
cfn-flip -c -l -n cloudformation.yaml temp.yaml
cfn-lint -t temp.yaml
mv temp.yaml cloudformation.yaml

read -rp "Databases password (e.g. 123456Ab): " password

aws cloudformation deploy \
  --template-file cloudformation.yaml \
  --stack-name aws-data-wrangler \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides DatabasesPassword="$password"

aws cloudformation update-termination-protection \
  --enable-termination-protection \
  --stack-name aws-data-wrangler
