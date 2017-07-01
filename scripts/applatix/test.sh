#!/bin/bash
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

# include HOME directory bin in PATH for heron cli, tools and tests
export PATH=${HOME}/bin:$PATH

# integration test binaries have to be specified as absolute path
JAVA_INTEGRATION_TESTS_BIN="${HOME}/.herontests/lib/integration-tests.jar"
PYTHON_INTEGRATION_TESTS_BIN="${HOME}/.herontests/lib/heron_integ_topology.pex"

# install client
T="heron client install"
start_timer "$T"
python ${UTILS}/save-logs.py "heron_client_install.txt" ./heron-client-install.sh --user
end_timer "$T"

# install tools
T="heron tools install"
start_timer "$T"
python ${UTILS}/save-logs.py "heron_tools_install.txt" ./heron-tools-install.sh --user
end_timer "$T"

# install tests
T="heron tests install"
start_timer "$T"
python ${UTILS}/save-logs.py "heron_tests_install.txt" ./heron-tests-install.sh --user
end_timer "$T"

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

# run the python integration test
T="heron integration_test python"
start_timer "$T"
${HOME}/bin/test-runner \
  -hc heron -tb ${PYTHON_INTEGRATION_TESTS_BIN} \
  -rh localhost -rp 8080\
  -tp ${HOME}/.herontests/data/python \
  -cl local -rl heron-staging -ev devel
end_timer "$T"

print_timer_summary
