#!/usr/bin/env bash

# TODO: turn this back on once all the javadoc errors are fixed
#set -e

HERON_ROOT_DIR=$(git rev-parse --show-toplevel)
JAVADOC_OUTPUT_DIR=$HERON_ROOT_DIR/website/public/api
GEN_PROTO_DIR=$HERON_ROOT_DIR/bazel-bin/heron/proto/_javac

SRC_FILES=`find $HERON_ROOT_DIR -path "*/com/twitter/*" -name "*.java"`
GEN_FILES=`find $GEN_PROTO_DIR -name "*.java"`

rm -r $JAVADOC_OUTPUT_DIR
mkdir -p $JAVADOC_OUTPUT_DIR
javadoc -quiet -d $JAVADOC_OUTPUT_DIR $GEN_FILES $SRC_FILES

echo "Javdocs generated at $JAVADOC_OUTPUT_DIR"
exit 0
