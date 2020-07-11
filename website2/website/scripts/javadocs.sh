#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
set -e

JAVADOC=javadoc
FLAGS="-quiet -Xdoclint:none"

HERON_ROOT_DIR=$(git rev-parse --show-toplevel)
JAVADOC_OUTPUT_DIR=$HERON_ROOT_DIR/website2/website/static/api/java
GEN_PROTO_DIR=$HERON_ROOT_DIR/bazel-bin/heron/proto/_javac

# The path of the customized landing page for the Javadocs
OVERVIEW_HTML_FILE=$HERON_ROOT_DIR/website2/website/scripts/javadocs-overview.html

# Check if this script is run with Travis flag
if [ $# -eq 1 ] && [ $1 == "--travis" ]; then
    BAZEL_CMD="bazel --bazelrc=$HERON_ROOT_DIR/tools/travis/bazel.rc build"
else
    BAZEL_CMD="bazel build"
fi

(cd $HERON_ROOT_DIR && $BAZEL_CMD \
  `bazel query 'kind("java_library", "heron/...")'` \
  `bazel query 'kind("java_library", "contrib/...")'` \
  `bazel query 'kind("java_test", "heron/...")'` \
  `bazel query 'kind("java_library", "integration_test/...")'`)

HERON_SRC_FILES=`find $HERON_ROOT_DIR -path "*/org/apache/*" -name "*.java" -not -path "$HERON_ROOT_DIR/tools/*" -not -path "$HERON_ROOT_DIR/examples/*"`
BACKTYPE_SRC_FILES=`find $HERON_ROOT_DIR -path "*/backtype/storm/*" -name "*.java"`
APACHE_SRC_FILES=`find $HERON_ROOT_DIR -path "*/org/apache/storm/*" -name "*.java"`
GEN_FILES=`find $GEN_PROTO_DIR -name "*.java"`

rm -rf $JAVADOC_OUTPUT_DIR
mkdir -p $JAVADOC_OUTPUT_DIR

GEN_JARS=`find $HERON_ROOT_DIR/bazel-bin/external/. -name "*\.jar" | tr '\n' ':'`
SCRIBE_JARS=`find $HERON_ROOT_DIR/bazel-bin/. -name "libthrift_scribe_java.jar" | tr '\n' ':'`
PROTO_JARS=`find $HERON_ROOT_DIR/bazel-bin/heron/proto/. -name "*\.jar" | tr '\n' ':'`
CLOSURE_CLASSES="$HERON_ROOT_DIR/bazel-bin/storm-compatibility/v0.10.2/src/java/_javac/storm-compatibility-java/libstorm-compatibility-java_classes/."
CONTRIB_JARS=`find $HERON_ROOT_DIR/bazel-bin/contrib/. -name "*\.jar" | tr '\n' ':'`

export CLASSPATH=$GEN_JARS:$SCRIBE_JARS:$PROTO_JARS:$CLOSURE_CLASSES:$CONTRIB_JARS

$JAVADOC $FLAGS \
  -encoding "UTF-8" \
  -windowtitle "Heron Java API" \
  -doctitle "The Heron Java API" \
  -overview $OVERVIEW_HTML_FILE \
  -d $JAVADOC_OUTPUT_DIR $GEN_FILES $HERON_SRC_FILES $BACKTYPE_SRC_FILES $APACHE_SRC_FILES || true

echo "Javadocs generated at $JAVADOC_OUTPUT_DIR"
exit 0
