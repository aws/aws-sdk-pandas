#!/bin/bash

cd ..
rm -fr build dist .egg requests.egg-info
python3.6 setup.py sdist
aws s3 cp dist/ s3://${1}/artifacts/ --recursive --exclude "*" --include "*.tar.gz"
rm -fr build dist .egg requests.egg-info
cd builds