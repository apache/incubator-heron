#!/bin/bash
#
# Script to kick off the travis CI build. We want the build to fail-fast if any
# of the below commands fail so we need to chain them in this script.
#

set -e

DIR=`dirname $0`
source ${DIR}/common.sh

# verify that jars have not been added to the repo
JARS=`find . -name "*.jar"`
if [ "$JARS" ]; then
  echo "ERROR: The following jars were found in the repo, "\
    "which is not permitted. Instead add the jar to WORKSPACE as a maven_jar."
  echo $JARS
  exit 1
fi

# verify that eggs have not been added to the repo
# ./third_party/pex/wheel-0.23.0-py2.7.egg should be the only one
set +e
EGGS=`find . -name "*.egg" | grep -v "third_party/pex/wheel"`
set -e
if [ "$EGGS" ]; then
  echo 'ERROR: The following eggs were found in the repo, '\
    'which is not permitted. Python dependencies should be '\
    'added using the "reqs" attribute:'
  echo $EGGS
  exit 1
fi

# verify that wheels have not been added to the repo
# ./third_party/pex/setuptools-18.0.1-py2.py3-none-any.whl should be the only one
set +e
WHEELS=`find . -name "*.whl" | grep -v "third_party/pex/setuptools"`
set -e
if [ "$WHEELS" ]; then
  echo 'ERROR: The following wheels were found in the repo, '\
  'which is not permitted. Python dependencies should be added using '\
  'the "reqs" attribute:'
  echo $WHEELS
  exit 1
fi

set +x

# Run this manually, since if it fails when run
# as -workspace_status_command we don't get good output
./scripts/release/status.sh

# append the bazel default bazelrc to travis-ci/bazel.rc
# for using rules provided by bazel
# cat ~/.bazelrc >> tools/travis-ci/bazel.rc
./bazel_configure.py

# build heron
T="heron build"
start_timer "$T"
python ${DIR}/save-logs.py "heron_build.txt" bazel\
  --bazelrc=tools/travis-ci/bazel.rc build --config=ubuntu heron/...
end_timer "$T"

# run heron unit tests
T="heron test non-flaky"
start_timer "$T"
python ${DIR}/save-logs.py "heron_test_non_flaky.txt" bazel\
  --bazelrc=tools/travis-ci/bazel.rc test\
  --test_summary=detailed --test_output=errors\
  --config=ubuntu --test_tag_filters=-flaky heron/...
end_timer "$T"

# flaky tests are often due to test port race conditions,
# which should be fixed. For now, run them serially
T="heron test flaky"
start_timer "$T"
python ${DIR}/save-logs.py "heron_test_flaky.txt" bazel\
  --bazelrc=tools/travis-ci/bazel.rc test\
  --test_summary=detailed --test_output=errors\
  --config=ubuntu --test_tag_filters=flaky --jobs=0 heron/...
end_timer "$T"

# build packages
T="heron build tarpkgs"
start_timer "$T"
python ${DIR}/save-logs.py "heron_build_tarpkgs.txt" bazel\
  --bazelrc=tools/travis-ci/bazel.rc build\
  --config=ubuntu scripts/packages:tarpkgs
end_timer "$T"

T="heron build binpkgs"
start_timer "$T"
python ${DIR}/save-logs.py "heron_build_binpkgs.txt" bazel\
  --bazelrc=tools/travis-ci/bazel.rc build\
  --config=ubuntu scripts/packages:binpkgs
end_timer "$T"

print_timer_summary
