#!/usr/bin/env bash
set -e

# Get security group ID
SGID=`aws cloudformation  describe-stacks --stack-name aws-sdk-pandas-databases --query "Stacks[0].Outputs[?OutputKey=='DatabaseSecurityGroupId'].OutputValue" --output text`

# Check to see current setting
aws ec2 describe-security-groups --group-id ${SGID}
