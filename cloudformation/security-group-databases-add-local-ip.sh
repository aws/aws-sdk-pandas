#!/usr/bin/env bash
set -e

# Get my current IP address
LOCALIP=`host myip.opendns.com resolver1.opendns.com | grep myip | awk '{print $4}'`

# Get security group ID
SGID=`aws cloudformation  describe-stacks --stack-name aws-data-wrangler-databases --query "Stacks[0].Outputs[?OutputKey=='DatabaseSecurityGroupId'].OutputValue" --output text`

# Update Security Group with local ip
aws ec2 authorize-security-group-ingress \
  --group-id ${SGID} \
  --protocol all \
  --port -1 \
  --cidr ${LOCALIP}/32
