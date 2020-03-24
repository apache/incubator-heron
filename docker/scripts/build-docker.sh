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

  EXTRA_ARGS=${@:4}
    # Boolean for Docker experimental parameter --squash
  DOCKER_SQUASH=false
  for element in $EXTRA_ARGS; do
    if [ $element = '-s' ] || [ $element = '--squash' ]; then
      DOCKER_SQUASH=true
      break
    fi
  done
  echo $DOCKER_SQUASH

  DOCKER_FILE="$SCRATCH_DIR/dist/Dockerfile.dist.$TARGET_PLATFORM"
  DOCKER_TAG="heron/heron:$HERON_VERSION"
  DOCKER_LATEST_TAG="heron/heron:latest"

  setup_scratch_dir $SCRATCH_DIR

  ALL_FILE="$OUTPUT_DIRECTORY/heron-install-$HERON_VERSION-$TARGET_PLATFORM.sh"
  ALL_OUT_FILE="$SCRATCH_DIR/artifacts/heron-install.sh"

  cp $ALL_FILE $ALL_OUT_FILE
  export HERON_VERSION

  echo "Building docker image with tag:$DOCKER_TAG"
  if [ $DOCKER_SQUASH = true ]; then
    docker build --squash -t "$DOCKER_TAG" -t "$DOCKER_LATEST_TAG" -f "$DOCKER_FILE" "$SCRATCH_DIR"
  else
    docker build -t "$DOCKER_TAG" -t "$DOCKER_LATEST_TAG" -f "$DOCKER_FILE" "$SCRATCH_DIR"
  fi
  # save the image as a tar file
  DOCKER_IMAGE_FILE="$OUTPUT_DIRECTORY/heron-docker-$HERON_VERSION-$TARGET_PLATFORM.tar"

  echo "Saving docker image to $DOCKER_IMAGE_FILE"
  docker save -o $DOCKER_IMAGE_FILE $DOCKER_TAG
  gzip $DOCKER_IMAGE_FILE
} 

case $# in
  0|1|2)
    echo "  "
    echo "Script to build heron docker image for different platforms"
    echo "  Input - directory containing the artifacts from the directory <artifact-directory>"
    echo "  Output - docker image tar file saved in the directory <artifact-directory> "
    echo "  "
    echo "Usage: $0 <platform> <version_string> <artifact-directory> [-s|--squash]"
    echo "  "
    echo "Argument options:"
    echo "  <platform>: darwin, debian9, debian10, ubuntu14.04, ubuntu18.04, centos7"
    echo "  <version_string>: Version of Heron build, e.g. v0.17.5.1-rc"
    echo "  <artifact-directory>: Location of compiled Heron artifact"
    echo "  [-s|--squash]: Enables using Docker experimental feature --squash"
    echo "  "
    echo "Example:"
    echo "  ./build-docker.sh ubuntu18.04 0.12.0 ~/ubuntu"
    echo "  "
    exit 1
    ;;

  *)
    run_build $1 $2 $3 ${@:4}
    ;;
esac
