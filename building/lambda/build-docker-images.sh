#!/usr/bin/env bash
set -ex

cp ../../pyproject.toml .
cp ../../poetry.lock .

export DOCKER_BUILDKIT=1

# Python 3.6
docker build \
  --pull \
  --tag awswrangler-build-py36 \
  --build-arg base_image=public.ecr.aws/lambda/python:3.6 \
  --build-arg python_version=python36 \
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

# Python 3.9
docker build \
  --pull \
  --tag awswrangler-build-py39 \
  --build-arg base_image=public.ecr.aws/lambda/python:3.9 \
  .

rm -rf pyproject.toml poetry.lock
