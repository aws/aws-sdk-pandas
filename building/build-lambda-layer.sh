#!/bin/bash

cd ..

# Preparing directories
mkdir -p dist
rm -rf python
rm -f "awswrangler_layer.zip"
rm -f "dist/awswrangler_layer.zip"

# Building
pip install . -t ./python
rm -rf ./python/*gandiva*
rm -rf ./python/pyarrow/*gandiva*
rm -rf ./python/pyarrow/*flight*
rm -rf ./python/pyarrow/*plasma*
rm -rf ./python/pyarrow/*orc*
zip -r "awswrangler_layer.zip" ./python
mv "awswrangler_layer.zip" dist/

# Cleaning up the directory again
rm -rf python

cd building
