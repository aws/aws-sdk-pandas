#!/bin/bash

cp ../requirements.txt .
cp ../requirements-dev.txt .
pip install -r requirements.txt
pip install -r requirements-dev.txt
docker build -t awswrangler-building .
