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
# Script to locally run the integration topology test.
#

HTTP_SERVER="./bazel-bin/integration_test/src/python/http_server/http-server"
TEST_RUNNER="./bazel-bin/integration_test/src/python/topology_test_runner/topology-test-runner.pex"

JAVA_TESTS_DIR="integration_test/src/java/org/apache/heron/integration_topology_test/topology"
PYTHON_TESTS_DIR="integration_test/src/python/integration_test/topology"
SCALA_TESTS_DIR="integration_test/src/scala/org/apache/heron/integration_test/topology"

# integration test binaries have to be specified as absolute path
JAVA_INTEGRATION_TESTS_BIN="${PWD}/bazel-bin/integration_test/src/java/integration-topology-tests.jar"
PYTHON_INTEGRATION_TESTS_BIN="${PWD}/bazel-bin/integration_test/src/python/integration_test/topology/heron_integ_topology.pex"
SCALA_INTEGRATION_TESTS_BIN="${PWD}/bazel-bin/integration_test/src/scala/scala-integration-tests.jar"

CORE_PKG="file://${PWD}/bazel-bin/scripts/packages/heron-core.tar.gz"

set -e

# building tar packages
DIR=`dirname $0`
source ${DIR}/detect_os_type.sh
bazel run --config=`platform` -- scripts/packages:heron-install.sh --user
bazel build --config=`platform` {heron/...,scripts/packages:tarpkgs,integration_test/src/...}

# run the simple http server
${HTTP_SERVER} 8080 &
http_server_id=$!
trap "kill -9 $http_server_id" SIGINT SIGTERM EXIT

# run the scala integration tests
#${TEST_RUNNER} \
#  -hc ~/.heron/bin/heron -tb ${SCALA_INTEGRATION_TESTS_BIN} \
#  -rh localhost -rp 8080 \
#  -tp ${SCALA_TESTS_DIR} \
#  -cl local -rl heron-staging -ev devel -pi ${CORE_PKG}

# run the java integration tests
${TEST_RUNNER} \
  -hc ~/.heron/bin/heron -tb ${JAVA_INTEGRATION_TESTS_BIN} \
  -rh localhost -rp 8080 \
  -tp ${JAVA_TESTS_DIR} \
  -cl local -rl heron-staging -ev devel -pi ${CORE_PKG}

# run the python integration tests
#${TEST_RUNNER} \
#  -hc ~/.heron/bin/heron -tb ${PYTHON_INTEGRATION_TESTS_BIN} \
#  -rh localhost -rp 8080 \
#  -tp ${PYTHON_TESTS_DIR} \
#  -cl local -rl heron-staging -ev devel -pi ${CORE_PKG}
