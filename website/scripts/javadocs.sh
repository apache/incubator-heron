#!/usr/bin/env bash
#set -e TODO: figure out why this breaks CI and re-enable (https://github.com/twitter/heron/issues/766)

FLAGS="-quiet"

HERON_ROOT_DIR=$(git rev-parse --show-toplevel)
JAVADOC_OUTPUT_DIR=$HERON_ROOT_DIR/website/public/api
JAVADOC_STATIC_OUTPUT_DIR=$HERON_ROOT_DIR/website/static
GEN_PROTO_DIR=$HERON_ROOT_DIR/bazel-bin/heron/proto/_javac
OVERVIEW_HTML_FILE=$HERON_ROOT_DIR/website/scripts/javadocs-overview.html

(cd $HERON_ROOT_DIR && bazel build \
  `bazel query 'kind("java_library", "heron/...")'`\
  `bazel query 'kind("java_test", "heron/...")'` \
  `bazel query 'kind("java_library", "integration-test/...")'`)

HERON_SRC_FILES=`find $HERON_ROOT_DIR -path "*/com/twitter/*" -name "*.java"`
BACKTYPE_SRC_FILES=`find $HERON_ROOT_DIR -path "*/backtype/storm/*" -name "*.java"`
APACHE_SRC_FILES=`find $HERON_ROOT_DIR -path "*/org/apache/storm/*" -name "*.java"`
GEN_FILES=`find $GEN_PROTO_DIR -name "*.java"`

rm -rf $JAVADOC_OUTPUT_DIR $JAVADOC_STATIC_OUTPUT_DIR/api
mkdir -p $JAVADOC_OUTPUT_DIR $JAVADOC_STATIC_OUTPUT_DIR

BIN_JARS=`find $HERON_ROOT_DIR/bazel-heron/_bin/. -name "*\.jar" | tr '\n' ':'`
GEN_JARS=`find $HERON_ROOT_DIR/bazel-genfiles/external/. -name "*\.jar" | tr '\n' ':'`
SCRIBE_JARS=`find $HERON_ROOT_DIR/bazel-bin/. -name "libthrift_scribe_java.jar" | tr '\n' ':'`
PROTO_JARS=`find $HERON_ROOT_DIR/bazel-bin/heron/proto/. -name "*\.jar" | tr '\n' ':'`
CLOJURE_CLASSES="$HERON_ROOT_DIR/bazel-bin/heron/storm/src/java/_javac/storm-compatibility-java/libstorm-compatibility-java_classes/."

export CLASSPATH=$BIN_JARS:$GEN_JARS:$SCRIBE_JARS:$PROTO_JARS:$CLOJURE_CLASSES

javadoc $FLAGS \
  -windowtitle "Heron Java API" \
  -doctitle "The Heron Java API" \
  -overview $OVERVIEW_HTML_FILE \
  -d $JAVADOC_OUTPUT_DIR $GEN_FILES $HERON_SRC_FILES $BACKTYPE_SRC_FILES $APACHE_SRC_FILES

ln -s $JAVADOC_OUTPUT_DIR $JAVADOC_STATIC_OUTPUT_DIR

echo "Javdocs generated at $JAVADOC_OUTPUT_DIR"
exit 0
