#!/bin/bash

pip install --upgrade pip
pip install --upgrade -r requirements.txt
pip install --upgrade -r requirements-dev.txt
cd testing
./build-image.sh
cd ../building
./build-image.sh
