#!/usr/bin/env bash
set -ex

pip install --upgrade pip
pip install -r requirements-dev.txt
pip install -e .
