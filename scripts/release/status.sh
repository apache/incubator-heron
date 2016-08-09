#!/bin/bash

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
  cmd="git rev-parse --abbrev-ref HEAD"
  build_version=$($cmd) || die "Failed to run command to check head: $cmd"

  if [ "${build_version}" = "HEAD" ];
  then
    cmd="git describe --tags --always"
    build_version=$($cmd) || die "Failed to run command to get git release: $cmd"
  fi
else
  build_version=${HERON_BUILD_VERSION}
fi
echo "HERON_BUILD_VERSION ${build_version}"

# The code below presents an implementation that works for git repository
if [ -z ${HERON_GIT_REV+x} ];
then
  cmd="git rev-parse HEAD"
  git_rev=$($cmd) || die "Failed to get git revision: $cmd"
else
  git_rev=${HERON_GIT_REV}
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
  build_user=${USER}
else
  build_user=${HERON_BUILD_USER}
fi
echo "HERON_BUILD_USER ${build_user}"

# Check whether there are any uncommited changes
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
echo "HERON_BUILD_RELEASE_STATUS ${tree_status}"
