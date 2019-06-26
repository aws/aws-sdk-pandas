#!/usr/bin/env bash

pip install --upgrade pip
pip install pipenv --upgrade
pipenv install --dev
cd tests
./clean.sh
./build-image.sh
cd ../builds
./clean.sh
./build-image.sh