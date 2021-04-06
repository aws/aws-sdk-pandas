#!/usr/bin/env bash
versions=${1:-ALL}
posargs=${2:-32}
SECONDS=0

set -e

./validate.sh
mkdir -p test-reports
tox -e ${versions} -- ${posargs}
if [ $versions = "ALL" ]; then
    coverage html --directory coverage
    rm -rf .coverage* Running 2> /dev/null
fi

duration=$SECONDS
echo "$(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed."