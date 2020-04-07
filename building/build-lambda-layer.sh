#!/usr/bin/env bash
set -ex

# Go back to AWSWRANGLER directory
pushd /aws-data-wrangler

# Build PyArrow files
pushd building
./build-pyarrow.sh
popd

# Preparing directories
mkdir -p dist
rm -rf python
rm -f "awswrangler-layer.zip"
rm -f "dist/awswrangler-layer.zip"

# Building
pip install . -t ./python
rm -rf python/pyarrow*
rm -rf python/boto*
rm -f /aws-data-wrangler/dist/pyarrow_files/pyarrow/libarrow.so
rm -f /aws-data-wrangler/dist/pyarrow_files/pyarrow/libparquet.so
rm -f /aws-data-wrangler/dist/pyarrow_files/pyarrow/libarrow_python.so
cp -r /aws-data-wrangler/dist/pyarrow_files/pyarrow* python/
find python -wholename "*/tests/*" -type f -delete
zip -r9 "awswrangler-layer.zip" ./python
mv "awswrangler-layer.zip" dist/

# Cleaning up the directory again
rm -rf python
