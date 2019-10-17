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
# To create a clean development environment, enter the source
# code directory of Heron and then execute the following scripts: 
#   sh docker/scripts/docker-dev-env.sh
# To enter an existing container, find the container id with:
#   docker ps -a
# Then enter the container by
#   docker start -ai CONTAINER_ID
# After the container is started, enter the source code directory
# and build with bazel (assuming it's a ubuntu container):
#   cd heron
#   bazel build --config=ubuntu heron/...

set -o nounset
set -o errexit

# Default platform is ubuntu18.04. Other available platforms
# include centos7
TARGET_PLATFORM=${1:-"ubuntu18.04"}
SCRATCH_DIR="docker"


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
  echo "$SCRATCH_DIR/compile/Dockerfile.$1"
}

DOCKER_FILE=$(dockerfile_path_for_platform $TARGET_PLATFORM)
verify_dockerfile_exists $DOCKER_FILE

echo $DOCKER_FILE

echo "Building docker container for development environment"
docker build -t heron-dev:$TARGET_PLATFORM -f $DOCKER_FILE $SCRATCH_DIR

echo "Running container and mapping the current dir to /heron"
docker run -i \
    -e TARGET_PLATFORM=$TARGET_PLATFORM \
    -e SCRATCH_DIR="/scratch" \
    -v $PROJECT_DIR:/heron \
    -t heron-dev:$TARGET_PLATFORM bash
