#!/usr/bin/env bash
set -ex

pip install --upgrade pip
pip install --upgrade -r requirements.txt
pip install --upgrade -r requirements-dev.txt
pushd building
./build-image.sh
