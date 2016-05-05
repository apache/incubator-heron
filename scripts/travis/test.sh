#!/bin/bash
#
# Script to kick off the travis CI integration test. Fail-fast if any of tthe below commands fail.
#
set -ex

# build test related jar
bazel --bazelrc=tools/travis-ci/bazel.rc build integration-test/src/java:local-integration-tests_deploy.jar

# install client
bazel --bazelrc=tools/travis-ci/bazel.rc run -- scripts/packages:heron-client-install.sh --user

# run the local integration test
python integration-test/src/python/local_test_runner/main.py

# build integration test tools
bazel --bazelrc=tools/travis-ci/bazel.rc build integration-test/src/{python,java}/...
bazel --bazelrc=tools/travis-ci/bazel.rc build integration-test/src/java:integration-tests_deploy.jar

# run integration test
./bazel-bin/integration-test/src/python/http_server/http-server 8080 &
http_server_id=$!
trap "kill -9 $http_server_id" SIGINT SIGTERM EXIT

./bazel-bin/integration-test/src/python/test_runner/test-runner.pex \
  -hc heron -tj  bazel-bin/integration-test/src/java/integration-tests_deploy.jar \
  -rh localhost -rp 8080\
  -tp integration-test/src/java/com/twitter/heron/integration_test/topology/ \
  -cl local -rl heron-staging -ev devel -pi file://{$HOME}/.heron/dist/heron-core.tar.gz

