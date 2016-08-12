#!/bin/bash
#
# Script to locally run the functional integration test.
#

HTTP_SERVER="./bazel-bin/integration-test/src/python/http_server/http-server"
TEST_RUNNER="./bazel-bin/integration-test/src/python/test_runner/test-runner.pex"

JAVA_TESTS_DIR="integration-test/src/java/com/twitter/heron/integration_test/topology/"
PYTHON_TESTS_DIR="integration-test/src/python/integration_test/topology/"

# integration test binaries have to be specified as absolute path
JAVA_INTEGRATION_TESTS_BIN="${PWD}/bazel-genfiles/integration-test/src/java/integration-tests.jar"
PYTHON_INTEGRATION_TESTS_BIN="${PWD}/bazel-bin/integration-test/src/python/integration_test/topology/pyheron_integ_topology.pex"

CORE_PKG="file://${PWD}/bazel-bin/scripts/packages/heron-core.tar.gz"

set -e

# building tar packages
DIR=`dirname $0`
source ${DIR}/detect_os_type.sh
bazel run --config=`platform` -- scripts/packages:heron-client-install.sh --user
bazel build --config=`platform` {heron/...,scripts/packages:tarpkgs,integration-test/src/...}

# run the simple http server
${HTTP_SERVER} 8080 &
http_server_id=$!
trap "kill -9 $http_server_id" SIGINT SIGTERM EXIT

# run the java integration tests
${TEST_RUNNER} \
  -hc heron -tb ${JAVA_INTEGRATION_TESTS_BIN} \
  -rh localhost -rp 8080 \
  -tp ${JAVA_TESTS_DIR} \
  -cl local -rl heron-staging -ev devel -pi ${CORE_PKG}

# run the python integration tests
${TEST_RUNNER} \
  -hc heron -tb ${PYTHON_INTEGRATION_TESTS_BIN} \
  -rh localhost -rp 8080 \
  -tp ${PYTHON_TESTS_DIR} \
  -cl local -rl heron-staging -ev devel -pi ${CORE_PKG}
