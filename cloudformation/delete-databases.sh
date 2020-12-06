#!/usr/bin/env bash
set -e

# Deleting
aws cloudformation delete-stack \
  --stack-name aws-data-wrangler-databases
