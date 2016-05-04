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
