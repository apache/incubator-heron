#!/bin/bash

HERON_ROOT_DIR=$(git rev-parse --show-toplevel)
INPUT=pyheron #$HERON_ROOT_DIR/heron/api/src/python

rm -rf static/api/python

pdoc $INPUT \
  --all-submodules \
  --html \
  --html-dir static/api/python
