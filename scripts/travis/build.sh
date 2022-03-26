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
# Script to kick off the travis CI build. We want the build to fail-fast if any
# of the below commands fail so we need to chain them in this script.
#
set -e

DIR=`dirname $0`
UTILS=${DIR}/../shutils
source ${UTILS}/common.sh

# verify that jars have not been added to the repo
JARS=`find . -name "*.jar"`
if [ "$JARS" ]; then
  echo "ERROR: The following jars were found in the repo, "\
    "which is not permitted. Instead add the jar to WORKSPACE as a maven_jar."
  echo $JARS
  exit 1
fi

# verify that eggs have not been added to the repo
set +e
#EGGS=`find . -name "*.egg"`
set -e
if [ "$EGGS" ]; then
  echo 'ERROR: The following eggs were found in the repo, '\
    'which is not permitted. Python dependencies should be '\
    'added using the "reqs" attribute:'
  echo $EGGS
  exit 1
fi

# verify that wheels have not been added to the repo
set +e
#WHEELS=`find . -name "*.whl"`
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

./bazel_configure.py

# build heron
T="heron build"
start_timer "$T"
${UTILS}/save-logs.py "heron_build.txt" bazel\
  build --config=stylecheck heron/... \
  heronpy/... examples/... storm-compatibility-examples/v0.10.2/... \
  eco-storm-examples/... eco-heron-examples/... contrib/...
end_timer "$T"

# run heron unit tests
T="heron test non-flaky"
start_timer "$T"
${UTILS}/save-logs.py "heron_test_non_flaky.txt" bazel \
  test --config=stylecheck \
  --test_summary=detailed --test_output=errors \
  --test_tag_filters=-flaky heron/... \
  heronpy/... examples/... storm-compatibility-examples/v0.10.2/... \
  eco-storm-examples/... eco-heron-examples/... contrib/... 
end_timer "$T"

# flaky tests are often due to test port race conditions,
# which should be fixed. For now, run them serially
T="heron test flaky"
start_timer "$T"
${UTILS}/save-logs.py "heron_test_flaky.txt" bazel \
  test --config=stylecheck \
  --test_summary=detailed --test_output=errors \
  --test_tag_filters=flaky --jobs=1 heron/... \
  heronpy/... examples/... storm-compatibility-examples/v0.10.2/... \
  eco-storm-examples/... eco-heron-examples/...
end_timer "$T"

# build packages
T="heron build tarpkgs"
start_timer "$T"
${UTILS}/save-logs.py "heron_build_tarpkgs.txt" bazel\
  build\
  --config=stylecheck scripts/packages:tarpkgs
end_timer "$T"

T="heron build binpkgs"
start_timer "$T"
${UTILS}/save-logs.py "heron_build_binpkgs.txt" bazel\
  build\
  --config=stylecheck scripts/packages:binpkgs
end_timer "$T"

T="heron build docker images"
start_timer "$T"
${UTILS}/save-logs.py "heron_build_binpkgs.txt" bazel\
  build\
  --config=stylecheck scripts/images:heron.tar
end_timer "$T"


print_timer_summary
