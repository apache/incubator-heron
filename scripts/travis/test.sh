#!/bin/bash
#
# Script to kick off the travis CI integration test. Fail-fast if any of tthe below commands fail.
#
set -ex

# build test related jar
bazel --bazelrc=tools/travis-ci/bazel.rc build integration-test/src/...

# install client
bazel --bazelrc=tools/travis-ci/bazel.rc run -- scripts/packages:heron-client-install.sh --user

# run local integration test
python integration-test/src/python/local_test_runner/main.py

# run integration test
./bazel-bin/integration-test/src/python/http_server/http-server 8080 &
http_server_id=$!
trap "kill -9 $http_server_id" SIGINT SIGTERM EXIT

./bazel-bin/integration-test/src/python/test_runner/test-runner.pex \
  -hc heron -tj bazel-genfiles/integration-test/src/java/integration-tests.jar \
  -rh localhost -rp 8080\
  -tp integration-test/src/java/com/twitter/heron/integration_test/topology/ \
  -cl local -rl heron-staging -ev devel -pi 'file://${HERON_DIST}/heron-core.tar.gz'

