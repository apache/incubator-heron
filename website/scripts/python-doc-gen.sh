#!/bin/bash

HERON_ROOT_DIR=$(git rev-parse --show-toplevel)
INPUT=$HERON_ROOT_DIR/heron/api/src/python/__init__.py

rm -rf static/api/python

export PYTHONPATH=$HERON_ROOT_DIR/heron/api/src/python

pdoc $INPUT \
  --html \
  --html-dir static/api/python
