#!/bin/bash
#
# Script to kick off the travis CI integration test. Fail-fast if any of tthe below commands fail.
#
set -e

DIR=`dirname $0`
source ${DIR}/common.sh

# check if the ci environ argument is provided
if [ $# -eq 0 ]; then
  echo "ci environ arg not provided (travis|applatix)"
fi

# Autodiscover the platform
PLATFORM=$(discover_platform)
echo "Using $PLATFORM platform"

# Get the CI environment
CIENV=$(ci_environ $1)

# integration test binaries have to be specified as absolute path
JAVA_INTEGRATION_TESTS_BIN="${HOME}/.herontests/lib/integration-tests.jar"
PYTHON_INTEGRATION_TESTS_BIN="${HOME}/.herontests/lib/heron_integ_topology.pex"

# build test related jar
T="heron build integration_test"
start_timer "$T"
python ${DIR}/save-logs.py "heron_build_integration_test.txt" bazel --bazelrc=tools/$CIENV/bazel.rc build --config=$PLATFORM integration_test/src/...
end_timer "$T"

# install client
T="heron client install"
start_timer "$T"
python ${DIR}/save-logs.py "heron_client_install.txt" bazel --bazelrc=tools/$CIENV/bazel.rc run --config=$PLATFORM -- scripts/packages:heron-client-install.sh --user
end_timer "$T"

# install tools
T="heron tools install"
start_timer "$T"
python ${DIR}/save-logs.py "heron_tools_install.txt" bazel --bazelrc=tools/$CIENV/bazel.rc run --config=$PLATFORM -- scripts/packages:heron-tools-install.sh --user
end_timer "$T"

# install tests
T="heron tests install"
start_timer "$T"
python ${DIR}/save-logs.py "heron_tests_install.txt" bazel --bazelrc=tools/$CIENV/bazel.rc run --config=$PLATFORM -- scripts/packages:heron-tests-install.sh --user
end_timer "$T"

# run local integration test
T="heron integration_test local"
start_timer "$T"
python ./bazel-bin/integration_test/src/python/local_test_runner/local-test-runner
end_timer "$T"

# run the java integration test
T="heron integration_test java"
start_timer "$T"
${HOME}/bin/http-server 8080 &
http_server_id=$!
trap "kill -9 $http_server_id" SIGINT SIGTERM EXIT

${HOME}/bin/test-runner \
  -hc heron -tb ${JAVA_INTEGRATION_TESTS_BIN} \
  -rh localhost -rp 8080\
  -tp ${HOME}/.herontests/data/java \
  -cl local -rl heron-staging -ev devel
end_timer "$T"

# run the python integration test
T="heron integration_test python"
start_timer "$T"
${HOME}/bin/test-runner \
  -hc heron -tb ${PYTHON_INTEGRATION_TESTS_BIN} \
  -rh localhost -rp 8080\
  -tp ${HOME}/.herontests/data/python \
  -cl local -rl heron-staging -ev devel
end_timer "$T"

print_timer_summary
