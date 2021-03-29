#!/usr/bin/env bash
versions=${1:-ALL}
posargs=${2:-32}
SECONDS=0

set -e

./validate.sh
mkdir -p test-reports
tox -e ${versions} -- ${posargs}
coverage html --directory test-reports/coverage
rm -rf test-reports/.coverage* test-reports/Running 2> /dev/null

duration=$SECONDS
echo "$(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed."