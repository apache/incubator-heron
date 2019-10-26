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
source ${DIR}/testutils.sh

# integration test binaries have to be specified as absolute path
PYTHON_INTEGRATION_TESTS_BIN="${HOME}/.herontests/lib/heron_integ_topology.pex"

# run the python integration test
T="heron integration_test python"
start_timer "$T"
${HOME}/bin/http-server 8080 &
http_server_id=$!
trap "kill -9 $http_server_id" SIGINT SIGTERM EXIT

${HOME}/bin/test-runner \
  -hc heron -tb ${PYTHON_INTEGRATION_TESTS_BIN} \
  -rh localhost -rp 8080\
  -tp ${HOME}/.herontests/data/python \
  -cl local -rl heron-staging -ev devel
end_timer "$T"

print_timer_summary
