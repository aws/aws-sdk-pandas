#!/usr/bin/env bash
set -e

# Deploying
aws cloudformation delete-stack \
  --stack-name aws-data-wrangler-base
