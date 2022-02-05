#!/usr/bin/env bash
set -ex

cp ../../pyproject.toml .
cp ../../poetry.lock .

# Python 3.6
docker build \
  --pull \
  --tag awswrangler-build-py36 \
  --build-arg base_image=lambci/lambda:build-python3.6 \
  .

# Python 3.7
docker build \
  --pull \
  --tag awswrangler-build-py37 \
  --build-arg base_image=public.ecr.aws/lambda/python:3.7 \
  .

# Python 3.8
docker build \
  --pull \
  --tag awswrangler-build-py38 \
  --build-arg base_image=public.ecr.aws/lambda/python:3.8 \
  .

rm -rf pyproject.toml poetry.lock
