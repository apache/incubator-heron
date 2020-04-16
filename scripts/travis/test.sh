#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Script to kick off the travis CI integration test. Fail-fast if any of tthe below commands fail.
#
set -e

DIR=`dirname $0`
UTILS=${DIR}/../shutils
source ${UTILS}/common.sh

# Autodiscover the platform
PLATFORM=$(discover_platform)
echo "Using $PLATFORM platform"

# integration test binaries have to be specified as absolute path
JAVA_INTEGRATION_TESTS_BIN="${HOME}/.herontests/lib/integration-tests.jar"
JAVA_INTEGRATION_TOPOLOGY_TESTS_BIN="${HOME}/.herontests/lib/integration-topology-tests.jar"
PYTHON_INTEGRATION_TESTS_BIN="${HOME}/.herontests/lib/heron_integ_topology.pex"
SCALA_INTEGRATION_TESTS_BIN="${HOME}/.herontests/lib/scala-integration-tests.jar"

# build test related jar
T="heron build integration_test"
start_timer "$T"
python ${UTILS}/save-logs.py "heron_build_integration_test.txt" bazel --bazelrc=tools/travis/bazel.rc build --config=$PLATFORM integration_test/src/...
end_timer "$T"

# install heron 
T="heron install"
start_timer "$T"
python ${UTILS}/save-logs.py "heron_install.txt" bazel --bazelrc=tools/travis/bazel.rc run --config=$PLATFORM -- scripts/packages:heron-install.sh --user
end_timer "$T"

# install tests
T="heron tests install"
start_timer "$T"
python ${UTILS}/save-logs.py "heron_tests_install.txt" bazel --bazelrc=tools/travis/bazel.rc run --config=$PLATFORM -- scripts/packages:heron-tests-install.sh --user
end_timer "$T"

pathadd ${HOME}/bin/

# run local integration test
T="heron integration_test local"
start_timer "$T"
python ./bazel-bin/integration_test/src/python/local_test_runner/local-test-runner
end_timer "$T"

# initialize http-server for integration tests
T="heron integration_test http-server initialization"
start_timer "$T"
${HOME}/bin/http-server 8080 &
http_server_id=$!
trap "kill -9 $http_server_id" SIGINT SIGTERM EXIT
end_timer "$T"

# run the scala integration test
T="heron integration_test scala"
start_timer "$T"
${HOME}/bin/test-runner \
  -hc heron -tb ${SCALA_INTEGRATION_TESTS_BIN} \
  -rh localhost -rp 8080\
  -tp ${HOME}/.herontests/data/scala \
  -cl local -rl heron-staging -ev devel
end_timer "$T"

# run the java integration test
T="heron integration_test java"
start_timer "$T"
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

# run the java integration topology test
T="heron integration_topology_test java"
start_timer "$T"
${HOME}/bin/topology-test-runner \
  -hc heron -tb ${JAVA_INTEGRATION_TOPOLOGY_TESTS_BIN} \
  -rh localhost -rp 8080\
  -tp ${HOME}/.herontests/data/java/topology_test \
  -cl local -rl heron-staging -ev devel
end_timer "$T"

print_timer_summary

