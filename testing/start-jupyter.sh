#!/usr/bin/env bash
set -e

cd ..
pip install -e .
jupyter lab --ip 0.0.0.0 --no-browser --allow-root