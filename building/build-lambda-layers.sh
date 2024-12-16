#!/usr/bin/env bash
set -ex

VERSION=$(poetry version --short)
DIR_NAME=$(dirname "$PWD")

PYTHON_VERSION=${1:-ALL}

ARCH=$(arch)
[ "${ARCH}" = "aarch64" ] && ARCH_SUFFIX="-arm64" # AWS Lambda, the name arm64 is used instead of aarch64

if [[ $PYTHON_VERSION == "ALL" ]]
then
  echo "Building Lambda Layers for AWS SDK for pandas ${VERSION} (ALL supported Python versions)"
else
  echo "Building Lambda Layers for AWS SDK for pandas ${VERSION} (ONLY Python $PYTHON_VERSION)"
fi

pushd lambda

# Building all related docker images
./build-docker-images.sh $PYTHON_VERSION

# Python 3.8
if [[ $PYTHON_VERSION == "ALL" || $PYTHON_VERSION == "3.8" ]]
then
  docker run \
    --volume "$DIR_NAME":/aws-sdk-pandas/ \
    --workdir /aws-sdk-pandas/building/lambda \
    --rm \
    awswrangler-build-py38 \
    build-lambda-layer.sh "${VERSION}-py3.8${ARCH_SUFFIX}" "ninja-build"
fi

# Python 3.9
if [[ $PYTHON_VERSION == "ALL" || $PYTHON_VERSION == "3.9" ]]
then
  docker run \
    --volume "$DIR_NAME":/aws-sdk-pandas/ \
    --workdir /aws-sdk-pandas/building/lambda \
    --rm \
    awswrangler-build-py39 \
    build-lambda-layer.sh "${VERSION}-py3.9${ARCH_SUFFIX}" "ninja-build"
fi

# Python 3.10
if [[ $PYTHON_VERSION == "ALL" || $PYTHON_VERSION == "3.10" ]]
then
  docker run \
    --volume "$DIR_NAME":/aws-sdk-pandas/ \
    --workdir /aws-sdk-pandas/building/lambda \
    --rm \
    awswrangler-build-py310 \
    build-lambda-layer.sh "${VERSION}-py3.10${ARCH_SUFFIX}" "ninja-build"
fi

# Python 3.11
if [[ $PYTHON_VERSION == "ALL" || $PYTHON_VERSION == "3.11" ]]
then
  docker run \
    --volume "$DIR_NAME":/aws-sdk-pandas/ \
    --workdir /aws-sdk-pandas/building/lambda \
    --rm \
    awswrangler-build-py311 \
    build-lambda-layer.sh "${VERSION}-py3.11${ARCH_SUFFIX}" "ninja-build"
fi

# Python 3.12
if [[ $PYTHON_VERSION == "ALL" || $PYTHON_VERSION == "3.12" ]]
then
  docker run \
    --volume "$DIR_NAME":/aws-sdk-pandas/ \
    --workdir /aws-sdk-pandas/building/lambda \
    --rm \
    awswrangler-build-py312 \
    build-lambda-layer.sh "${VERSION}-py3.12${ARCH_SUFFIX}" "ninja-build"
fi

# Python 3.13
if [[ $PYTHON_VERSION == "ALL" || $PYTHON_VERSION == "3.13" ]]
then
  docker run \
    --volume "$DIR_NAME":/aws-sdk-pandas/ \
    --workdir /aws-sdk-pandas/building/lambda \
    --rm \
    awswrangler-build-py313 \
    build-lambda-layer.sh "${VERSION}-py3.13${ARCH_SUFFIX}" "ninja-build"
fi
