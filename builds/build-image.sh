#!/bin/bash

pipenv lock -r > requirements.txt
pipenv lock --dev -r > requirements-dev.txt
docker build -t awswrangler-builds .
