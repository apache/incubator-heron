#!/bin/bash
#
# Script to kick off the travis CI integration test. Fail-fast if any of tthe below commands fail.
#

DIR=`dirname $0`
source ${DIR}/common.sh

# integration test binaries have to be specified as absolute path
JAVA_INTEGRATION_TESTS_BIN="${PWD}/bazel-genfiles/integration-test/src/java/integration-tests.jar"
PYTHON_INTEGRATION_TESTS_BIN="${PWD}/bazel-bin/integration-test/src/python/integration_test/topology/pyheron_integ_topology.pex"

# build test related jar
python ${DIR}/save-logs.py "heron_build_integration_test.txt" bazel --bazelrc=tools/travis-ci/bazel.rc build --config=ubuntu integration-test/src/...

# install client
python ${DIR}/save-logs.py "heron_client_install.txt" bazel --bazelrc=tools/travis-ci/bazel.rc run --config=ubuntu -- scripts/packages:heron-client-install.sh --user

# install tools
python ${DIR}/save-logs.py "heron_tools_install.txt" bazel --bazelrc=tools/travis-ci/bazel.rc run --config=ubuntu -- scripts/packages:heron-tools-install.sh --user

# run local integration test
# T="heron integration-test local"
# start_timer "$T"
# python ./bazel-bin/integration-test/src/python/local_test_runner/local-test-runner
# end_timer "$T"

# run the java integration test
./bazel-bin/integration-test/src/python/http_server/http-server 8080 &
http_server_id=$!
trap "kill -9 $http_server_id" SIGINT SIGTERM EXIT

# Run MultiSpoutsMultiTasks
for i in `seq 1 100`; do
  rm -rf ~/.herondata
  ./bazel-bin/integration-test/src/python/test_runner/test-runner.pex \
    -hc heron -tb ${JAVA_INTEGRATION_TESTS_BIN} \
    -rh localhost -rp 8080\
    -tp integration-test/src/java/com/twitter/heron/integration_test/topology/ \
    -cl local -rl heron-staging -ev devel \
    -ts 'IntegrationTest_MultiSpoutsMultiTasks'
  RESULT=$?
  if [ $RESULT -ne 0 ]; then
    # Dump out stream manager log
    echo "DUMPING STMGR LOG"
    tail -n +1 ~/.herondata/topologies/local/*/*MultiSpoutsMultiTasks*/log-files/*stmgr*.INFO
    # Dump out Java program's logs
    echo "DUMPING JAVA PROGRAM LOG"
    tail -n +1 ~/.herondata/topologies/local/*/*MultiSpoutsMultiTasks*/log-files/container*.log.0
    exit 1
  fi
done
