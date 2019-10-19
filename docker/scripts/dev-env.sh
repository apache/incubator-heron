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

# This is a script to start a docker container that has all
# tools needed to compire Heron. Developer should be able to
# compile Heron in the container without any setup works.
# usage:
# To create a clean development environment with docker and run it,
# execute the following scripts in the source directory of Heron:
#   sh docker/scripts/dev-env.sh
#
# After the container is started, build Heron with bazel
# (ubuntu config is used in the example):
#   ./bazel_configure.py
#   bazel build --config=ubuntu heron/...
#
# To enter an existing container with a new shell, find the container
# id with this command:
#   docker ps -a
# And then :
#   docker exec -it CONTAINER_ID bash

set -o nounset
set -o errexit

# Default platform is ubuntu18.04. Other available platforms
# include centos7, debian9
TARGET_PLATFORM=${1:-"ubuntu18.04"}
SCRATCH_DIR=${2:-"$HOME/.heron-docker"}
REPOSITORY="heron-dev"


realpath() {
  echo "$(cd "$(dirname "$1")"; pwd)/$(basename "$1")"
}

DOCKER_DIR=$(dirname $(dirname $(realpath $0)))
PROJECT_DIR=$(dirname $DOCKER_DIR)

verify_dockerfile_exists() {
  if [ ! -f $1 ]; then
    echo "The Dockerfiler $1 does not exist"
    exit 1
  fi
}

dockerfile_path_for_platform() {
  echo "$DOCKER_DIR/compile/Dockerfile.$1"
}

copy_extra_files() {
  mkdir -p $SCRATCH_DIR/scripts
  cp $PROJECT_DIR/tools/docker/bazel.rc $SCRATCH_DIR/bazelrc
  cp $DOCKER_DIR/scripts/compile-docker.sh $SCRATCH_DIR/scripts/compile-platform.sh
}

DOCKER_FILE=$(dockerfile_path_for_platform $TARGET_PLATFORM)
verify_dockerfile_exists $DOCKER_FILE
copy_extra_files

echo "Building docker image for Heron development environment on $TARGET_PLATFORM"
docker build -t $REPOSITORY:$TARGET_PLATFORM -f $DOCKER_FILE $SCRATCH_DIR

echo "Creating and starting container and mapping the current dir to /heron"
docker container run -it \
    -e TARGET_PLATFORM=$TARGET_PLATFORM \
    -e SCRATCH_DIR="/scratch" \
    -v $PROJECT_DIR:/heron \
    -w "/heron" \
    -p 8888:8888 \
    -p 8889:8889 \
    -t $REPOSITORY:$TARGET_PLATFORM bash
