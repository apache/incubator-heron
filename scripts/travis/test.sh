#!/bin/bash
#
# Script to kick off the travis CI integration test. Fail-fast if any of tthe below commands fail.
#
set -e

DIR=`dirname $0`
source ${DIR}/common.sh

# integration test binaries have to be specified as absolute path
JAVA_INTEGRATION_TESTS_BIN="${PWD}/bazel-genfiles/integration-test/src/java/integration-tests.jar"
PYTHON_INTEGRATION_TESTS_BIN="${PWD}/bazel-bin/integration-test/src/python/integration_test/topology/pyheron_integ_topology.pex"

# build test related jar
T="heron build integration-test"
start_timer "$T"
python ${DIR}/save-logs.py "heron_build_integration_test.txt" bazel --bazelrc=tools/travis-ci/bazel.rc build --config=ubuntu integration-test/src/...
end_timer "$T"

# install client
T="heron client install"
start_timer "$T"
python ${DIR}/save-logs.py "heron_client_install.txt" bazel --bazelrc=tools/travis-ci/bazel.rc run --config=ubuntu -- scripts/packages:heron-client-install.sh --user
end_timer "$T"

# install tools
T="heron tools install"
start_timer "$T"
python ${DIR}/save-logs.py "heron_tools_install.txt" bazel --bazelrc=tools/travis-ci/bazel.rc run --config=ubuntu -- scripts/packages:heron-tools-install.sh --user
end_timer "$T"

# run local integration test
T="heron integration-test local"
start_timer "$T"
python ./bazel-bin/integration-test/src/python/local_test_runner/local-test-runner
end_timer "$T"

# run the java integration test
T="heron integration-test java"
start_timer "$T"
./bazel-bin/integration-test/src/python/http_server/http-server 8080 &
http_server_id=$!
trap "kill -9 $http_server_id" SIGINT SIGTERM EXIT

./bazel-bin/integration-test/src/python/test_runner/test-runner.pex \
  -hc heron -tb ${JAVA_INTEGRATION_TESTS_BIN} \
  -rh localhost -rp 8080\
  -tp integration-test/src/java/com/twitter/heron/integration_test/topology/ \
  -cl local -rl heron-staging -ev devel
end_timer "$T"

# run the python integration test
T="heron integration-test python"
start_timer "$T"
./bazel-bin/integration-test/src/python/test_runner/test-runner.pex \
  -hc heron -tb ${PYTHON_INTEGRATION_TESTS_BIN} \
  -rh localhost -rp 8080\
  -tp integration-test/src/python/integration_test/topology/ \
  -cl local -rl heron-staging -ev devel
end_timer "$T"

print_timer_summary
