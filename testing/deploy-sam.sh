#!/bin/bash

BUCKET=$(jq -r '.Bucket' parameters.json)
VPC_ID=$(jq -r '.VpcId as $k | "VpcId=\($k)"' parameters.json)
SUBNET_ID=$(jq -r '.SubnetId as $k | "SubnetId=\($k)"' parameters.json)
PASSWORD=$(jq -r '.Password as $k | "Password=\($k)"' parameters.json)

rm -rf .aws-sam
sam build --template sam/template.yaml
sam package --output-template-file .aws-sam/packaged.yaml --s3-bucket ${BUCKET}
sam deploy --template-file .aws-sam/packaged.yaml --stack-name aws-data-wrangler-test-arena \
--capabilities CAPABILITY_IAM \
--parameter-overrides ${VPC_ID} ${SUBNET_ID} ${PASSWORD}
rm -rf .aws-sam