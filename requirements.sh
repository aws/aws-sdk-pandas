#!/usr/bin/env bash
set -ex

pip install --upgrade pip
pip install --upgrade --requirement requirements-dev.txt
pip install --upgrade --editable .
