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
export log_level=WARN
source ${UTILS}/common.sh

# integration test binaries have to be specified as absolute path
JAVA_INTEGRATION_TESTS_BIN="${HOME}/.herontests/lib/integration-tests.jar"
JAVA_INTEGRATION_TOPOLOGY_TESTS_BIN="${HOME}/.herontests/lib/integration-topology-tests.jar"
PYTHON_INTEGRATION_TESTS_BIN="${HOME}/.herontests/lib/heron_integ_topology.pex"
SCALA_INTEGRATION_TESTS_BIN="${HOME}/.herontests/lib/scala-integration-tests.jar"

# build test related jar
T="heron build integration_test"
start_timer "$T"
${UTILS}/save-logs.py "heron_build_integration_test.txt" bazel build integration_test/src/...
end_timer "$T"

:<<'SKIPPING-FOR-NOW'
# install heron 
T="heron install"
start_timer "$T"
${UTILS}/save-logs.py "heron_install.txt" bazel run -- scripts/packages:heron-install.sh --user
end_timer "$T"
SKIPPING-FOR-NOW

# install tests
T="heron tests install"
start_timer "$T"
${UTILS}/save-logs.py "heron_tests_install.txt" bazel run -- scripts/packages:heron-tests-install.sh --user
end_timer "$T"

pathadd ${HOME}/bin/

:<<'SKIPPING-FOR-NOW'
# run local integration test
T="heron integration_test local"
start_timer "$T"
./bazel-bin/integration_test/src/python/local_test_runner/local-test-runner
end_timer "$T"
SKIPPING-FOR-NOW

# initialize http-server for integration tests
T="heron integration_test http-server initialization"
start_timer "$T"
${HOME}/bin/http-server 8080 &
http_server_id=$!
trap "kill -9 $http_server_id" SIGINT SIGTERM EXIT
end_timer "$T"

function integration_test {
    # integration_test $cluster $test_bin
    local cluster="$1"
    local language="$2"
    local test_bin="$3"
    local host port
    if [ "$cluster" == "local" ]; then
        host=localhost
        port=8080
    else
        #host="$(docker inspect kind-control-plane --format '{{ .NetworkSettings.Networks.kind.IPAddress }}')"
        host="host.docker.internal"
        host="$(docker inspect --format='{{range .NetworkSettings.Networks}}{{.Gateway}}{{end}}' kind-control-plane)"
        #host=172.17.0.1
        port=8080
    fi

    local T="$language integration test on $cluster"
    start_timer "$T"
    "${HOME}/bin/test-runner" \
        --heron-cli-path=heron \
        --tests-bin-path="$test_bin" \
        --http-server-hostname="$host" \
        --http-server-port="$port" \
        --topologies-path="${HOME}/.herontests/data/$language" \
        --cluster="$cluster" \
        --role=heron-staging \
        --env=devel \
        #--cli-config-path="$cluster"
    end_timer "$T"
}

# look at making the whole test script a function, e.g.
#   ./test.sh heron-k8s
#   ./test.sh local
# concurrent runs from different clusters are fine?
for cluster in kubernetes local; do
    if [ "$cluster" == "local" ]; then
        host=localhost
        port=8080
    else
        host="$(docker inspect kind-control-plane --format='{{range .NetworkSettings.Networks}}{{.Gateway}}{{end}}')"
        #host=172.18.0.1
        port=8080
    fi
    integration_test "$cluster" scala "${SCALA_INTEGRATION_TESTS_BIN}"
    integration_test "$cluster" java "${JAVA_INTEGRATION_TESTS_BIN}"
    integration_test "$cluster" python ${PYTHON_INTEGRATION_TESTS_BIN} \

    T="heron integration_topology_test java"
    start_timer "$T"
    "${HOME}/bin/topology-test-runner" \
      --heron-cli-path=heron \
      --tests-bin-path="${JAVA_INTEGRATION_TOPOLOGY_TESTS_BIN}" \
      --http-hostname="$host" \
      --http-port="$port" \
      --topologies-path="${HOME}/.herontests/data/java/topology_test" \
      --cluster="$cluster" \
      --role=heron-staging \
      --env=devel
      #--cli-config-path="$cluster"
    end_timer "$T"
done

print_timer_summary
