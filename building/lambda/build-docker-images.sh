#!/usr/bin/env bash
set -ex

cp ../../pyproject.toml .
cp ../../poetry.lock .

export DOCKER_BUILDKIT=1

ARCH=$(arch)

if [ "${ARCH}" != "aarch64" ]; then
  # Python 3.7 (using public.ecr.aws/lambda/python:3.8 is intentional)
  docker build \
    --pull \
    --tag awswrangler-build-py37 \
    --build-arg base_image=public.ecr.aws/lambda/python:3.8 \
    --build-arg python_version=python37 \
    .
fi

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
