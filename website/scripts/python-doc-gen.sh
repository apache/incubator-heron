#!/bin/bash

HERON_ROOT_DIR=$(git rev-parse --show-toplevel)
INPUT=heron_py
TMP_DIR=$(mktemp -d)

rm -rf static/api/python

pdoc $INPUT \
  --html \
  --html-dir $TMP_DIR

mv $TMP_DIR/heron_py static/api/python
