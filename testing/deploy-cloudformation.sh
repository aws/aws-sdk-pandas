#!/bin/bash

VPC_ID=$(jq -r '.VpcId as $k | "VpcId=\($k)"' parameters.json)
SUBNET_ID=$(jq -r '.SubnetId as $k | "SubnetId=\($k)"' parameters.json)
PASSWORD=$(jq -r '.Password as $k | "Password=\($k)"' parameters.json)

aws cloudformation deploy \
--template-file template.yaml \
--stack-name aws-data-wrangler-test-arena \
--capabilities CAPABILITY_IAM \
--parameter-overrides ${VPC_ID} ${SUBNET_ID} ${PASSWORD}
