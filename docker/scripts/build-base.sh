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

  cp -R $DOCKER_DIR/base $1/
}

setup_output_dir() {
  echo "Creating output directory $1"
  mkdir -p $1
}

run_build() {
  TARGET_PLATFORM=$1
  HERON_VERSION=$2
  OUTPUT_DIRECTORY=$(realpath $3)
  DOCKER_FILE="$SCRATCH_DIR/base/Dockerfile.base.$TARGET_PLATFORM"

  setup_scratch_dir $SCRATCH_DIR
  setup_output_dir $OUTPUT_DIRECTORY

  DOCKER_TAG="heron/base:$HERON_VERSION"
  DOCKER_LATEST_TAG="heron/base:latest"

  if [ "$TARGET_PLATFORM" == "debian11" ]; then
    DOCKER_TAG="apache/heron:$HERON_VERSION"
    DOCKER_LATEST_TAG="apache/heron:latest"
    DOCKER_IMAGE_FILE="$OUTPUT_DIRECTORY/base-$HERON_VERSION.tar"
  else
    DOCKER_TAG="apache/heron-$TARGET_PLATFORM:$HERON_VERSION"
    DOCKER_LATEST_TAG="apache/heron-$TARGET_PLATFORM:latest"
    DOCKER_IMAGE_FILE="$OUTPUT_DIRECTORY/base-$TARGET_PLATFORM-$HERON_VERSION.tar"
  fi

  export HERON_VERSION

  echo "Building heron base docker image with tag:$DOCKER_TAG"
  docker buildx build -t "$DOCKER_TAG" -t "$DOCKER_LATEST_TAG" -f "$DOCKER_FILE" "$SCRATCH_DIR"

  echo "Saving docker image to $DOCKER_IMAGE_FILE"
  docker save -o $DOCKER_IMAGE_FILE $DOCKER_TAG
}

case $# in
  3)
    run_build $1 $2 $3
    ;;

  *)
    echo "  "
    echo "Script to build heron base docker image for different platforms"
    echo "  Input - platform, version tag "
    echo "  Output - docker image tar file saved in the directory <output-directory> "
    echo "  "
    echo "Usage: $0 <platform> <version_string> <output-directory>"
    echo "  "
    echo "Platforms Supported: darwin, debian11, ubuntu20.04, rocky8"
    echo "  "
    echo "Example:"
    echo "  ./build-base.sh ubuntu20.04 0.12.0 ~/ubuntu"
    echo "  "
    exit 1
    ;;
esac
