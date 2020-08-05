#!/usr/bin/env bash
set -ex

pip install --upgrade pip
pip --use-feature=2020-resolver install --upgrade --upgrade-strategy eager --requirement requirements-dev.txt
pip --use-feature=2020-resolver install --upgrade --upgrade-strategy eager --editable .
