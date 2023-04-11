#!/usr/bin/env bash
set -ex

cp ../../pyproject.toml .
cp ../../poetry.lock .

export DOCKER_BUILDKIT=1

# Python 3.8
docker build \
  --pull \
  --tag awswrangler-build-py38 \
  --build-arg base_image=public.ecr.aws/lambda/python:3.8 \
  .

# Python 3.9
docker build \
  --pull \
  --tag awswrangler-build-py39 \
  --build-arg base_image=public.ecr.aws/lambda/python:3.9 \
  .

rm -rf pyproject.toml poetry.lock
