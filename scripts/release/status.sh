#!/bin/bash

# This script will be run bazel when building process starts to
# generate key-value information that represents the status of the
# workspace. The output should be like
#
# KEY1 VALUE1
# KEY2 VALUE2
#
# If the script exits with non-zero code, it's considered as a failure
# and the output will be discarded.

# get the release tag version or the branch name
git_release=$(git rev-parse --abbrev-ref HEAD)
if [[ $? != 0 ]];
then
    exit 1
fi

if [ "${git_release}" = "HEAD" ]; 
then
  git_release=$(git describe --tags)
  if [[ $? != 0 ]];
  then
    exit 1
  fi
fi
echo "BUILD_SCM_RELEASE ${git_release}"

# The code below presents an implementation that works for git repository
git_rev=$(git rev-parse HEAD)
if [[ $? != 0 ]];
then
    exit 1
fi
echo "BUILD_SCM_REVISION ${git_rev}"

echo "BUILD_COMMIT_URL https://github.com/twitter/heron/commit/${git_rev}"

commit_msg=$(git log -1 --oneline | cut -f 2- -d ' ')
if [[ $? != 0 ]];
then
    exit 1
fi
echo "BUILD_COMMIT_MSG ${commit_msg}"

build_time=$(date)
echo "BUILD_TIME ${build_time}"

# Check whether there are any uncommited changes
git diff-index --quiet HEAD --
if [[ $? == 0 ]];
then
    tree_status="Clean"
else
    tree_status="Modified"
fi
echo "BUILD_RELEASE_STATUS ${tree_status}"

