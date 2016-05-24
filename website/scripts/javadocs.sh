#!/usr/bin/env bash
set +e # TODO: fix errors and warnings and re-enable

HERON_ROOT_DIR=$(git rev-parse --show-toplevel)
JAVADOC_OUTPUT_DIR=$HERON_ROOT_DIR/website/static/api
GEN_PROTO_DIR=$HERON_ROOT_DIR/bazel-bin/heron/proto/_javac

(cd $HERON_ROOT_DIR && bazel build \
  `bazel query 'kind("java_library", "heron/...")'`\
  `bazel query 'kind("java_test", "heron/...")'` \
  `bazel query 'kind("java_library", "integration-test/...")'`)

HERON_SRC_FILES=`find $HERON_ROOT_DIR -path "*/com/twitter/*" -name "*.java"`
BACKTYPE_SRC_FILES=`find $HERON_ROOT_DIR -path "*/backtype/storm/*" -name "*.java"`
APACHE_SRC_FILES=`find $HERON_ROOT_DIR -path "*/org/apache/storm/*" -name "*.java"`
GEN_FILES=`find $GEN_PROTO_DIR -name "*.java"`

rm -r $JAVADOC_OUTPUT_DIR
mkdir -p $JAVADOC_OUTPUT_DIR

EXT_JARS=`find $HERON_ROOT_DIR/bazel-out/host/genfiles/external/. -name "*\.jar" | tr '\n' ':'`
BIN_JARS=`find $HERON_ROOT_DIR/bazel-heron/_bin/. -name "*\.jar" | tr '\n' ':'`
GEN_JARS=`find $HERON_ROOT_DIR/bazel-genfiles/external/. -name "*\.jar" | tr '\n' ':'`
SCRIBE_JARS=`find $HERON_ROOT_DIR/bazel-bin/. -name "libthrift_scribe_java.jar" | tr '\n' ':'`
PROTO_JARS=`find $HERON_ROOT_DIR/bazel-bin/heron/proto/. -name "*\.jar" | tr '\n' ':'`
CLOSURE_CLASSES="$HERON_ROOT_DIR/bazel-bin/heron/storm/src/java/_javac/storm-compatibility-java/libstorm-compatibility-java_classes/."

export CLASSPATH=$EXT_JARS:$BIN_JARS:$GEN_JARS:$SCRIBE_JARS:$PROTO_JARS:$CLOSURE_CLASSES

javadoc -quiet -d $JAVADOC_OUTPUT_DIR $GEN_FILES $HERON_SRC_FILES $BACKTYPE_SRC_FILES $APACHE_SRC_FILES

echo "Javdocs generated at $JAVADOC_OUTPUT_DIR"
exit 0
