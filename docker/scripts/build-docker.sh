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
SCRATCH_DIR="$HOME/.heron-docker"

cleanup() {
  if [ -d $SCRATCH_DIR ]; then
    echo "Cleaning up scratch dir"
    rm -rf $SCRATCH_DIR
  fi
}

trap cleanup EXIT


setup_scratch_dir() {
  if [ ! -f "$1" ]; then

    mkdir $1
    mkdir $1/artifacts
  fi

  cp -R $DOCKER_DIR/dist $1/
}

run_build() {
  TARGET_PLATFORM=$1
  HERON_VERSION=$2
  OUTPUT_DIRECTORY=$(realpath $3)
  DOCKER_FILE="$SCRATCH_DIR/dist/Dockerfile.dist.$TARGET_PLATFORM"
  DOCKER_TAG="heron/heron:$HERON_VERSION"
  DOCKER_LATEST_TAG="heron/heron:latest"

  setup_scratch_dir $SCRATCH_DIR

  # need to copy artifacts locally
  CORE_FILE="$OUTPUT_DIRECTORY/heron-core-$HERON_VERSION-$TARGET_PLATFORM.tar.gz"
  CORE_OUT_FILE="$SCRATCH_DIR/artifacts/heron-core.tar.gz"

  ALL_FILE="$OUTPUT_DIRECTORY/heron-install.sh"
  ALL_OUT_FILE="$SCRATCH_DIR/artifacts/heron-install.sh"

  cp $CORE_FILE $CORE_OUT_FILE
  cp $ALL_FILE $ALL_OUT_FILE
  export HERON_VERSION

  echo "Building docker image with tag:$DOCKER_TAG"
  docker build --squash -t "$DOCKER_TAG" -t "$DOCKER_LATEST_TAG" -f "$DOCKER_FILE" "$SCRATCH_DIR"
  # save the image as a tar file
  DOCKER_IMAGE_FILE="$OUTPUT_DIRECTORY/heron-docker-$HERON_VERSION-$TARGET_PLATFORM.tar"

  echo "Saving docker image to $DOCKER_IMAGE_FILE"
  docker save -o $DOCKER_IMAGE_FILE $DOCKER_TAG
  gzip $DOCKER_IMAGE_FILE
} 

case $# in
  3)
    run_build $1 $2 $3
    ;;

  *)
    echo "  "
    echo "Script to build heron docker image for different platforms"
    echo "  Input - directory containing the artifacts from the directory <artifact-directory>"
    echo "  Output - docker image tar file saved in the directory <artifact-directory> "
    echo "  "
    echo "Usage: $0 <platform> <version_string> <artifact-directory> "
    echo "  "
    echo "Platforms Supported: ubuntu14.04, ubuntu16.04, centos7, debian8"
    echo "  "
    echo "Example:"
    echo "  ./build-docker.sh ubuntu14.04 0.12.0 ~/ubuntu"
    echo "  "
    exit 1
    ;;
esac
