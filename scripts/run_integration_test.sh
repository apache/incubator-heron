#!/bin/bash
#
# Script to locally run the functional integration test.
# This script assumes that integration tests are already built under ./bazel-bin directory
# ($ bazel build --config=PLATFORM integration-test/src/...)
#

HTTP_SERVER="./bazel-bin/integration-test/src/python/http_server/http-server"
TEST_RUNNER="./bazel-bin/integration-test/src/python/test_runner/test-runner.pex"

declare -a required_files=(${HTTP_SERVER} ${TEST_RUNNER});

for file in ${required_files[@]}; do
  if [ ! -f ${file} ]; then
    echo "Required file ${file} does not exist." >&2
    echo "Make sure that the integration tests are built prior to" >&2
    echo "running this script using the following command." >&2
    echo "  bazel build --config={PLATFORM} integration-test/src/..." >&2
    exit 1;
  fi
done

${HTTP_SERVER} 8080 &
http_server_id=$!
trap "kill -9 $http_server_id" SIGINT SIGTERM EXIT

${TEST_RUNNER} \
  -hc heron -tj bazel-genfiles/integration-test/src/java/integration-tests.jar \
  -rh localhost -rp 8080\
  -tp integration-test/src/java/com/twitter/heron/integration_test/topology/ \
  -cl local -rl heron-staging -ev devel -pi 'file://${HERON_DIST}/heron-core.tar.gz'
