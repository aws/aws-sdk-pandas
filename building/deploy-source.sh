#!/usr/bin/env bash
set -ex

pushd ..
rm -rf *.egg-info dist/*.tar.gz
python3.6 setup.py sdist
aws s3 cp dist/ s3://${1}/artifacts/ --recursive --exclude "*" --include "*.tar.gz"
rm -rf *.egg-info
