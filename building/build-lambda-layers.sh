#!/usr/bin/env bash
set -ex

VERSION=$(python -c "import awswrangler as wr; print(wr.__version__)")
echo "Building Lambda Layers for AWS Data Wrangler ${VERSION}"
DIR_NAME=$(dirname "$PWD")

pushd lambda

# Building all related docker images
./build-docker-images.sh

# Building Apache Arrow binary artifacts
docker run \
  --volume "$DIR_NAME":/aws-data-wrangler/ \
  --workdir /aws-data-wrangler/building/lambda \
  -it \
  awswrangler-build-py36 \
  build-apache-arrow.sh

# Generating PyArrow Files for Python 3.6
#docker run \
#  --volume "$DIR_NAME":/aws-data-wrangler/ \
#  --workdir /aws-data-wrangler/building/lambda \
#  -it \
#  awswrangler-build-py36 \
#  build-pyarrow.sh

# Building the AWS Lambda Layer for Python 3.6
#docker run \
#  --volume "$DIR_NAME":/aws-data-wrangler/ \
#  --workdir /aws-data-wrangler/building/lambda \
#  -it \
#  awswrangler-build-py36 \
#  build-layer.sh "${VERSION}-py3.6"
