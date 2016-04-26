#!/bin/bash
#
# Script to kick off the travis CI build. We want the build to fail-fast if any
# of the below commands fail so we need to chain them in this script.
#

set -ex

# verify that jars have not been added to the repo
JARS=`find . -name "*.jar"`
if [ "$JARS" ]; then
  echo "ERROR: The following jars were found in the repo, which is no permitted. Instead add the jar to WORKSPACE as a maven_jar."
  echo $JARS
  exit 1
fi

# Run this manually, since if it fails when run as -workspace_status_command we don't get good output
./scripts/release/status.sh

# append the bazel default bazelrc to travis-ci/bazel.rc for using rules provided by bazel
cat ~/.bazelrc >> tools/travis-ci/bazel.rc
./bazel_configure.py

# build heron
bazel --bazelrc=tools/travis-ci/bazel.rc build heron/...

# run heron unit tests
bazel --bazelrc=tools/travis-ci/bazel.rc test  heron/...

# build packages
bazel --bazelrc=tools/travis-ci/bazel.rc build scripts/packages:tarpkgs
bazel --bazelrc=tools/travis-ci/bazel.rc build scripts/packages:binpkgs

