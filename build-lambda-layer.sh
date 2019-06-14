#!/usr/bin/env bash

# Installing PostgreSQL dependencies
yum install postgresql-devel -y

# Upgrading pip
pip install --upgrade pip

# Preparing directories
mkdir -p dist
rm -rf python
rm -f "awswrangler_layer_$1.zip"
rm -f "dist/awswrangler_layer_$1.zip"

# Building
pip install . -t ./python
zip -r "awswrangler_layer_$1.zip" ./python
mv "awswrangler_layer_$1.zip" dist/

# Cleaning up the directory again
rm -rf python
