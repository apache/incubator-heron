#!/bin/bash
#
# Script to locally run the functional integration test.
# This script assumes that integration tests are already built under ./bazel-bin directory
# ($ bazel build --config=PLATFORM integration-test/src/...)
#

./bazel-bin/integration-test/src/python/http_server/http-server 8080 &
http_server_id=$!
trap "kill -9 $http_server_id" SIGINT SIGTERM EXIT

./bazel-bin/integration-test/src/python/test_runner/test-runner.pex \
  -hc heron -tj bazel-genfiles/integration-test/src/java/integration-tests.jar \
  -rh localhost -rp 8080\
  -tp integration-test/src/java/com/twitter/heron/integration_test/topology/ \
  -cl local -rl heron-staging -ev devel -pi 'file://${HERON_DIST}/heron-core.tar.gz'
