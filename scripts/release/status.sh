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

# This script will be run when the bazel build process starts to
# generate key-value information that represents the status of the
# workspace. The output should be like
#
# KEY1 VALUE1
# KEY2 VALUE2
#
# If the script exits with non-zero code, it's considered as a failure
# and the output will be discarded.

set -eu

function die {
    echo >&2 "$@"
    exit 1
}

function disable_e_and_execute {
  set +e
  $@
  ret_status=$?
  set -e
  echo $ret_status
}

# get the release tag version or the branch name
if [ -z ${HERON_BUILD_VERSION+x} ];
then
  # variable HERON_BUILD_VERSION is not available, use git branch as build version
  if [ -d .git ];
  then
    cmd="git rev-parse --abbrev-ref HEAD"
    build_version=$($cmd) || die "Failed to run command to check head: $cmd"

    if [ "${build_version}" = "HEAD" ];
    then
      cmd="git describe --tags --always"
      build_version=$($cmd) || die "Failed to run command to get git release: $cmd"
    fi
  else
    # not git managed, use current dir as build version
    current_dir=$(pwd)
    build_version=$(basename "$current_dir")
  fi
else
  # variable HERON_BUILD_VERSION is available, use it.
  build_version=${HERON_BUILD_VERSION}
fi
echo "HERON_BUILD_VERSION ${build_version}"

# The code below presents an implementation that works for git repository
if [ -d .git ];
then
  if [ -z ${HERON_GIT_REV+x} ];
  then
    cmd="git rev-parse HEAD"
    git_rev=$($cmd) || die "Failed to get git revision: $cmd"
  else
    git_rev=${HERON_GIT_REV}
  fi
else
  git_rev=$build_version
fi

echo "HERON_BUILD_SCM_REVISION ${git_rev}"

if [ -z ${HERON_BUILD_HOST+x} ];
then
  build_host=$(hostname)
else
  build_host=${HERON_BUILD_HOST}
fi
echo "HERON_BUILD_HOST ${build_host}"

if [ -z ${HERON_BUILD_TIME+x} ];
then
  build_time=$(LC_ALL=en_EN.utf8 date)
else
  build_time=${HERON_BUILD_TIME}
fi
echo "HERON_BUILD_TIME ${build_time}"

if [ -z ${HERON_BUILD_TIMESTAMP+x} ];
then
  build_timestamp=$(date +%s000)
else
  build_timestamp=${HERON_BUILD_TIMESTAMP}
fi
echo "HERON_BUILD_TIMESTAMP ${build_timestamp}"

if [ -z ${HERON_BUILD_USER+x} ];
then
  build_user=${USER:-root}
else
  build_user=${HERON_BUILD_USER}
fi
echo "HERON_BUILD_USER ${build_user}"

# Check whether there are any uncommited changes
if [ -d .git ];
then
  if [ -z ${HERON_TREE_STATUS+x} ];
  then
    status=$(disable_e_and_execute "git diff-index --quiet HEAD --")
    if [[ $status == 0 ]];
    then
      tree_status="Clean"
    else
      tree_status="Modified"
    fi
  else
    tree_status=${HERON_TREE_STATUS}
  fi
else
  tree_status="Clean"
fi
echo "HERON_BUILD_RELEASE_STATUS ${tree_status}"
