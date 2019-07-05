#!/bin/bash

pip install --upgrade pip
pip install -r requirements.txt
pip install -r requirements-dev.txt
cd testing
./build-image.sh
cd ../building
./build-image.sh
