#!/usr/bin/env bash
set -ex

pip install --upgrade --upgrade-strategy eager pip wheel
pip --use-feature=2020-resolver install --upgrade --upgrade-strategy eager --requirement requirements-dev.txt
