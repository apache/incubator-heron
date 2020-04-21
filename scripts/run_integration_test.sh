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
# Script to locally run the functional integration test.
#
# Usage:
#   run_integration_test.sh [language] [test pattern]
# Examples:
# Run all tests in all languages:
#   run_integration_test.sh
# Run all tests in a specific language:
#   run_integration_test.sh java
# Run specific tests in a specific language:
#   run_integration_test.sh java IntegrationTest_Streamlet_.*


HTTP_SERVER="./bazel-bin/integration_test/src/python/http_server/http-server"
TEST_RUNNER="./bazel-bin/integration_test/src/python/test_runner/test-runner.pex"

JAVA_TESTS_DIR="integration_test/src/java/org/apache/heron/integration_test/topology"
PYTHON_TESTS_DIR="integration_test/src/python/integration_test/topology"
SCALA_TESTS_DIR="integration_test/src/scala/org/apache/heron/integration_test/topology"

# Parse arguments
LANGUAGE="all"
TESTS_PATTERN="[^\n]+"
if [ "$1" != "" ]; then
  if [ "$1" == "--help" ] || [ "$1" == "-h" ];then
    echo "Usage:"
    echo "  run_integration_test.sh [language] [test name pattern]"
    echo "Exampes:"
    echo "Run all tests in all languages:"
    echo "  run_integration_test.sh"
    echo "Run all tests in a specific language:"
    echo "  run_integration_test.sh java"
    echo "Run specific tests in a specific language:"
    echo "  run_integration_test.sh java IntegrationTest_Streamlet_.*"
    exit 0
  fi

  LANGUAGE=$1

  # Parameter 2 is test pattern
  if [ "$2" != "" ]; then
    TESTS_PATTERN=$2
  fi
fi

echo "Topology language is: " $LANGUAGE
echo "Topology filter pattern is: " $TESTS_PATTERN

# integration test binaries have to be specified as absolute path
JAVA_INTEGRATION_TESTS_BIN="${PWD}/bazel-bin/integration_test/src/java/integration-tests.jar"
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
if [ "$LANGUAGE" = "all" ] || [ "$LANGUAGE" = "scala" ]; then
  echo "Run the Scala integration tests"
  ${TEST_RUNNER} \
    -hc ~/.heron/bin/heron -tb ${SCALA_INTEGRATION_TESTS_BIN} \
    -rh localhost -rp 8080 \
    -tp ${SCALA_TESTS_DIR} \
    -cl local -rl heron-staging -ev devel -pi ${CORE_PKG} \
    -ts ${TESTS_PATTERN}
fi

# run the java integration tests
if [ "$LANGUAGE" = "all" ] || [ "$LANGUAGE" = "java" ]; then
  echo "Run the Java integration tests"
  ${TEST_RUNNER} \
    -hc ~/.heron/bin/heron -tb ${JAVA_INTEGRATION_TESTS_BIN} \
    -rh localhost -rp 8080 \
    -tp ${JAVA_TESTS_DIR} \
    -cl local -rl heron-staging -ev devel -pi ${CORE_PKG} \
    -ts ${TESTS_PATTERN}
fi

# run the python integration tests
if [ "$LANGUAGE" = "all" ] || [ "$LANGUAGE" = "python" ]; then
  echo "Run the Python integration tests"
  ${TEST_RUNNER} \
    -hc ~/.heron/bin/heron -tb ${PYTHON_INTEGRATION_TESTS_BIN} \
    -rh localhost -rp 8080 \
    -tp ${PYTHON_TESTS_DIR} \
    -cl local -rl heron-staging -ev devel -pi ${CORE_PKG} \
    -ts ${TESTS_PATTERN}
fi