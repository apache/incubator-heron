#!/bin/bash
#
# Script to kick off the travis CI integration test. Fail-fast if any of tthe below commands fail.
#
set -e

DIR=`dirname $0`
source ${DIR}/testutils.sh

# integration test binaries have to be specified as absolute path
JAVA_INTEGRATION_TESTS_BIN="${HOME}/.herontests/lib/integration-tests.jar"

# run the java integration test
T="heron integration_test java"
start_timer "$T"
${HOME}/bin/http-server 8080 &
http_server_id=$!
trap "kill -9 $http_server_id" SIGINT SIGTERM EXIT

${HOME}/bin/test-runner \
  -hc heron -tb ${JAVA_INTEGRATION_TESTS_BIN} \
  -rh localhost -rp 8080\
  -tp ${HOME}/.herontests/data/java \
  -cl local -rl heron-staging -ev devel
end_timer "$T"

print_timer_summary
