#!/bin/bash

HERON_ROOT_DIR=$(git rev-parse --show-toplevel)
GENERATED_HTML_DIR=./public

source $HERON_ROOT_DIR/website/scripts/common.sh

bundle _${BUNDLER_VERSION}_ exec htmlproofer $GENERATED_HTML_DIR \
    --alt-ignore '/.*/' \
    --allow-hash-href \
    --url-ignore ""
