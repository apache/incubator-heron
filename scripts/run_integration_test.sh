#!/bin/bash
#
# Script to locally run the functional integration test.
# This script assumes that integration tests are already built under ./bazel-bin directory
# ($ bazel build --config=PLATFORM integration-test/src/...)
#

declare -a required_files=("./bazel-bin/integration-test/src/python/http_server/http-server" \
                           "./bazel-bin/integration-test/src/python/test_runner/test-runner.pex");

for file in ${required_files[@]}; do
  if [ ! -f ${file} ]; then
    echo "Required files do not exist." >&2
    echo "Make sure that the integration tests are built prior to" >&2
    echo "running this script using the following command." >&2
    echo "  bazel build --config={PLATFORM} integration-test/src/..." >&2
    exit 1;
  fi
done

./bazel-bin/integration-test/src/python/http_server/http-server 8080 &
http_server_id=$!
trap "kill -9 $http_server_id" SIGINT SIGTERM EXIT

./bazel-bin/integration-test/src/python/test_runner/test-runner.pex \
  -hc heron -tj bazel-genfiles/integration-test/src/java/integration-tests.jar \
  -rh localhost -rp 8080\
  -tp integration-test/src/java/com/twitter/heron/integration_test/topology/ \
  -cl local -rl heron-staging -ev devel -pi 'file://${HERON_DIST}/heron-core.tar.gz'
