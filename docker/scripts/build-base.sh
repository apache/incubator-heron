#!/bin/bash
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

  if [ "$TARGET_PLATFORM" == "debian8" ]; then
    DOCKER_TAG="heron/base:$HERON_VERSION"
    DOCKER_LATEST_TAG="heron/base:latest"
    DOCKER_IMAGE_FILE="$OUTPUT_DIRECTORY/base-$HERON_VERSION.tar"
  else
    DOCKER_TAG="heron/heron-$TARGET_PLATFORM:$HERON_VERSION"
    DOCKER_LATEST_TAG="heron/base-$TARGET_PLATFORM:latest"
    DOCKER_IMAGE_FILE="$OUTPUT_DIRECTORY/base-$TARGET_PLATFORM-$HERON_VERSION.tar"
  fi

  export HERON_VERSION

  echo "Building heron base docker image with tag:$DOCKER_TAG"
  docker build --squash -t "$DOCKER_TAG" -t "$DOCKER_LATEST_TAG" -f "$DOCKER_FILE" "$SCRATCH_DIR"

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
    echo "Platforms Supported: debian8, ubuntu14.04, centos7"
    echo "  "
    echo "Example:"
    echo "  ./build-base.sh ubuntu14.04 0.12.0 ~/ubuntu"
    echo "  "
    exit 1
    ;;
esac
