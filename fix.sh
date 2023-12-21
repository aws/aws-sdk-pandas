#!/usr/bin/env bash
set -ex

ruff format .
ruff check --fix .