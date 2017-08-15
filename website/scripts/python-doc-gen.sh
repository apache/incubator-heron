#!/bin/bash

HERONPY_VERSION=$1
HERON_ROOT_DIR=$(git rev-parse --show-toplevel)
INPUT=heronpy
TMP_DIR=$(mktemp -d)

pip install heronpy==${HERONPY_VERSION}

mkdir -p static/api && rm -rf static/api/python

pdoc heron \
  --html \
  --html-dir $TMP_DIR

mv $TMP_DIR/heron static/api/python
