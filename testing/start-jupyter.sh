#!/usr/bin/env bash
set -ex

cd ..
pip install -e .
jupyter lab --ip 0.0.0.0 --no-browser --allow-root