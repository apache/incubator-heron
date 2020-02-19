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
set -o errexit

realpath() {
  echo "$(cd "$(dirname "$1")"; pwd)/$(basename "$1")"
}

DOCKER_DIR=$(dirname $(dirname $(realpath $0)))
PROJECT_DIR=$(dirname $DOCKER_DIR)
SCRATCH_DIR="$HOME/.heron-compile"
SRC_TAR="$SCRATCH_DIR/src.tar.gz"

heron_git_release() {
  local git_release=$(git rev-parse --abbrev-ref HEAD)
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
  echo $git_release
}

heron_git_rev() {
  local git_rev=$(git rev-parse HEAD)
  if [[ $? != 0 ]];
  then
    exit 1
  fi
  echo $git_rev
}

heron_build_host() {
  local build_host=$(hostname)
  echo $build_host
}

heron_build_user() {
  local build_user=$USER
  echo $build_user
}

heron_build_time() {
  local build_time=$(LC_ALL=en_EN.utf8 date)
  echo $build_time
}

heron_build_timestamp() {
  local build_timestamp=$(date +%s000)
  echo $build_timestamp
}

heron_tree_status() {
  local tree_status=""
  git diff-index --quiet HEAD --
  if [[ $? == 0 ]];
  then
    tree_status="Clean"
  else
    tree_status="Modified"
  fi
  echo $tree_status
}

cleanup() {
  if [ -f $SRC_TAR ]; then
    echo "Cleaning up scratch dir"
    rm -rf $SCRATCH_DIR
  fi
}

trap cleanup EXIT

generate_source() {
  echo "Generating source tarball"
  tar --exclude-from=$DOCKER_DIR/.tarignore -C $PROJECT_DIR -czf $SRC_TAR .
}

verify_source_exists() {
  if [ ! -f $1 ]; then
    echo "The source provided $1 does not exist"
    exit 1
  fi
}

setup_scratch_dir() {
  mkdir -p $1/artifacts
  cp -r $DOCKER_DIR/* $1
}

setup_output_dir() {
  echo "Creating output directory $1"
  mkdir -p $1
}

run_build() {
  TARGET_PLATFORM=$1
  HERON_VERSION=$2
  SOURCE_TARBALL=$3

  setup_scratch_dir $SCRATCH_DIR

  if [ -z "$SOURCE_TARBALL" ]; then
    generate_source
    SOURCE_TARBALL=$SRC_TAR
  else
    SOURCE_TARBALL=$(realpath $3)
  fi
  verify_source_exists $SOURCE_TARBALL

  export TARGET_PLATFORM=${TARGET_PLATFORM}
  export HERON_VERSION=${HERON_VERSION} 
  export SCRATCH_DIR=${SCRATCH_DIR}
  export SOURCE_TARBALL=${SOURCE_TARBALL}

  export HERON_BUILD_VERSION="${HERON_BUILD_VERSION:-$(heron_git_release)}"
  export HERON_GIT_REV="${HERON_GIT_REV:-$(heron_git_rev)}"
  export HERON_BUILD_HOST="${HERON_GIT_HOST:-$(heron_build_host)}"
  export HERON_BUILD_USER="${HERON_BUILD_USER:-$(heron_build_user)}"
  export HERON_BUILD_TIME="${HERON_BUILD_TIME:-$(heron_build_time)}"
  export HERON_TREE_STATUS="${HERON_TREE_STATUS:-$(heron_tree_status)}"

  if [ $TARGET_PLATFORM = "darwin" ]; then
    docker/scripts/test-platform.sh
  else
    docker/scripts/test-docker.sh
  fi
}

case $# in
  2)
    run_build $1 $2 ""
    ;;

  3)
    run_build $1 $2 $3
    ;;

  *)
    echo "Usage: $0 <platform> <version_string> [source-tarball]"
    echo "  "
    echo "Script to test heron artifacts for different platforms"
    echo "  "
    echo "Platforms Supported: darwin, ubuntu18.04, centos7"
    echo "  "
    echo "Example:"
    echo "  ./test-unittest.sh centos7 0.20.2"
    echo "  "
    echo "NOTE: If running on OSX, the output directory will need to "
    echo "      be under /Users so virtualbox has access to."
    exit 1
    ;;
esac
