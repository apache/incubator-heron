#!/usr/bin/env bash

JAVADOC=javadoc
FLAGS="-quiet"

HERON_ROOT_DIR=$(git rev-parse --show-toplevel)
# for display on GitHub website
JAVADOC_OUTPUT_DIR=$HERON_ROOT_DIR/website/public/api
# for display on local Hugo server
JAVADOC_OUTPUT_LOCAL_DIR=$HERON_ROOT_DIR/website/static/api
GEN_PROTO_DIR=$HERON_ROOT_DIR/bazel-bin/heron/proto/_javac

cd $HERON_ROOT_DIR
BAZEL_TARGETS=\
$(bazel query 'kind("java_library", "heron/...")' | sort)

BAZEL_TEST_TARGETS=\
$(bazel query 'kind("java_test", "heron/...")' | sort)

BAZEL_INTEGR_TEST_TARGETS=\
$(bazel query 'kind("java_library", "integration-test/...")' | sort)

if [[ -z "${BAZEL_TARGETS}" || -z "${BAZEL_TEST_TARGETS}" || -z "${BAZEL_INTEGR_TEST_TARGETS}" ]]; then
  echo "ERROR: Cannot find enough bazel build jobs"\
  exit 1
fi

PASS_COUNTER=0
FAIL_COUNTER=0
COUNTER=0

for BUILD_JOB in $BAZEL_TARGETS $BAZEL_TEST_TARGETS $BAZEL_INTEGR_TEST_TARGETS; do
  ((COUNTER++))
  bazel build ${BUILD_JOB}
 if [[ $? -eq 0 ]]; then
    ((PASS_COUNTER++))
 else
    ((FAIL_COUNTER++))
    echo "ERROR: Build failed of " ${BUILD_JOB}
 fi
done

echo "*********Summary*********"
echo "${COUNTER} build jobs:" \
     "${PASS_COUNTER} passed;" \
     "${FAIL_COUNTER} failed"

if [[ ${FAIL_COUNTER} -gt 0 ]]; then
   exit 1
fi

HERON_SRC_FILES=`find $HERON_ROOT_DIR -path "*/com/twitter/*" -name "*.java"`
BACKTYPE_SRC_FILES=`find $HERON_ROOT_DIR -path "*/backtype/storm/*" -name "*.java"`
APACHE_SRC_FILES=`find $HERON_ROOT_DIR -path "*/org/apache/storm/*" -name "*.java"`
GEN_FILES=`find $GEN_PROTO_DIR -name "*.java"`

rm -rf $JAVADOC_OUTPUT_DIR
mkdir -p $JAVADOC_OUTPUT_DIR

BIN_JARS=`find $HERON_ROOT_DIR/bazel-heron/_bin/. -name "*\.jar" | tr '\n' ':'`
GEN_JARS=`find $HERON_ROOT_DIR/bazel-genfiles/external/. -name "*\.jar" | tr '\n' ':'`
SCRIBE_JARS=`find $HERON_ROOT_DIR/bazel-bin/. -name "libthrift_scribe_java.jar" | tr '\n' ':'`
PROTO_JARS=`find $HERON_ROOT_DIR/bazel-bin/heron/proto/. -name "*\.jar" | tr '\n' ':'`
CLOSURE_CLASSES="$HERON_ROOT_DIR/bazel-bin/heron/storm/src/java/_javac/storm-compatibility-java/libstorm-compatibility-java_classes/."

export CLASSPATH=$BIN_JARS:$GEN_JARS:$SCRIBE_JARS:$PROTO_JARS:$CLOSURE_CLASSES

$JAVADOC $FLAGS -d $JAVADOC_OUTPUT_DIR $GEN_FILES $HERON_SRC_FILES $BACKTYPE_SRC_FILES $APACHE_SRC_FILES

if [[ $? != 0 ]]; then
  exit 1
fi

# Generated Java API doc needs to be copied to $JAVADOC_OUTPUT_LOCAL_DIR
# for the following two reasons:
# 1. When one is developing website locally using Hugo server, he should
#    be able to click into API doc link and view API doc to
#    check if the correct API link is given.
# 2. ``wget`` needs to verify if links to Java API doc are valid when Hugo is
#    serving the website locally. This means that Hugo should be able to display
#    Java API doc properly.
cp -r $JAVADOC_OUTPUT_DIR $JAVADOC_OUTPUT_LOCAL_DIR

echo "Javdocs generated at $JAVADOC_OUTPUT_DIR"
exit 0
