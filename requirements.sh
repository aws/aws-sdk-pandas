#!/usr/bin/env bash
set -ex

pip install --upgrade pip
pip install --upgrade -r requirements-dev.txt
pip install --upgrade -e .
