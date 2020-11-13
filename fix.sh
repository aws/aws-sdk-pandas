#!/usr/bin/env bash
set -ex

isort .
black .
