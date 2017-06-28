#!/bin/bash

HERONPY_VERSION=0.0.1
HERON_ROOT_DIR=$(git rev-parse --show-toplevel)
INPUT=heronpy
TMP_DIR=$(mktemp -d)

pip install heronpy==${HERONPY_VERSION}

mkdir -p static/api && rm -rf static/api/python

pdoc $INPUT \
  --html \
  --html-dir $TMP_DIR

mv $TMP_DIR/heronpy static/api/python
