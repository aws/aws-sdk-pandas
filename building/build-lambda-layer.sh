#!/usr/bin/env bash
set -e

# Go back to AWSWRANGLER directory
cd /aws-data-wrangler/

# Build PyArrow files
cd building
./build-pyarrow.sh
cd ..

# Preparing directories
mkdir -p dist
rm -rf python
rm -f "awswrangler-layer.zip"
rm -f "dist/awswrangler-layer.zip"

# Building
pip install . -t ./python
rm -rf python/pyarrow*
cp -r /aws-data-wrangler/dist/pyarrow_files/pyarrow* python/
find python -wholename "*/tests/*" -type f -delete
zip -r9 "awswrangler-layer.zip" ./python
mv "awswrangler-layer.zip" dist/

# Cleaning up the directory again
rm -rf python

cd building
