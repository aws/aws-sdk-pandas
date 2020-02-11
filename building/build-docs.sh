#!/usr/bin/env bash
set -ex

cd ..
sphinx-apidoc --separate -f --no-toc -H "API Reference" -o docs/source/api awswrangler/
make -C docs/ html
doc8 --ignore D001 docs/source
cd building
echo DONE!
