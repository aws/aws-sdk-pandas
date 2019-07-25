#!/bin/bash

cp ../requirements.txt .
cp ../requirements-dev.txt .
docker build -t awswrangler-testing .
