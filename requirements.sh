#!/usr/bin/env bash
set -ex

pip install --upgrade pip
pip install --upgrade --upgrade-strategy eager --requirement requirements-dev.txt
pip install --upgrade --upgrade-strategy eager --editable .
