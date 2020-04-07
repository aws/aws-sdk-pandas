#!/usr/bin/env bash
set -ex

pip install --upgrade pip
pip install -r requirements.txt
pip install -r requirements-dev.txt
pushd building
./build-image.sh
