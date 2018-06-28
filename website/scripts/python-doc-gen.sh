#!/bin/bash
set -e

HERONPY_VERSION=$1
HERON_ROOT_DIR=$(git rev-parse --show-toplevel)
INPUT=heronpy
TMP_DIR=$(mktemp --directory)

VENV="$(mktemp --directory)"
virtualenv "$VENV"
source "$VENV/bin/activate"
# TODO: make this a virtualenv
pip install "heronpy==${HERONPY_VERSION}" "pdoc~=0.3.2"
pip install --ignore-installed six

mkdir -p static/api && rm -rf static/api/python

pdoc heronpy \
  --html \
  --html-dir $TMP_DIR

mv $TMP_DIR/heronpy static/api/python
