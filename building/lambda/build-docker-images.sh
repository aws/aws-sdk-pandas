#!/usr/bin/env bash
set -ex

cp ../../pyproject.toml .
cp ../../poetry.lock .

export DOCKER_BUILDKIT=1

PYTHON_VERSION=${1:-ALL}

# Python 3.8
if [[ $PYTHON_VERSION == "ALL" || $PYTHON_VERSION == "3.8" ]]
then
  docker build \
    --pull \
    --tag awswrangler-build-py38 \
    --build-arg base_image=public.ecr.aws/lambda/python:3.8 \
    .
fi

# Python 3.9
if [[ $PYTHON_VERSION == "ALL" || $PYTHON_VERSION == "3.9" ]]
then
  docker build \
    --pull \
    --tag awswrangler-build-py39 \
    --build-arg base_image=public.ecr.aws/lambda/python:3.9 \
    .
fi

# Python 3.10
if [[ $PYTHON_VERSION == "ALL" || $PYTHON_VERSION == "3.10" ]]
then
  docker build \
    --pull \
    --tag awswrangler-build-py310 \
    --build-arg base_image=public.ecr.aws/lambda/python:3.10 \
    .
fi

# Python 3.11
if [[ $PYTHON_VERSION == "ALL" || $PYTHON_VERSION == "3.11" ]]
then
  docker build \
    --pull \
    --tag awswrangler-build-py311 \
    --build-arg base_image=public.ecr.aws/lambda/python:3.11 \
    .
fi

# Python 3.12
if [[ $PYTHON_VERSION == "ALL" || $PYTHON_VERSION == "3.12" ]]
then
  docker build \
    --pull \
    --tag awswrangler-build-py312 \
    --build-arg base_image=public.ecr.aws/lambda/python:3.12 \
    --file Dockerfile.al2023 \
    .
fi

rm -rf pyproject.toml poetry.lock
