#!/bin/bash
set -o errexit

realpath() {
  echo "$(cd "$(dirname "$1")"; pwd)/$(basename "$1")"
}

DOCKER_DIR=$(dirname $(realpath $0))
PROJECT_DIR=$(dirname $DOCKER_DIR )
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

  cp $DOCKER_DIR/* $1
}

build_exec_image() {
  TARGET_PLATFORM=$1
  HERON_VERSION=$2
  OUTPUT_DIRECTORY=$(realpath $3)

  DOCKER_FILE="$SCRATCH_DIR/Dockerfile.dist.$TARGET_PLATFORM"
  DOCKER_TAG="heron:$HERON_VERSION-$TARGET_PLATFORM"

  setup_scratch_dir $SCRATCH_DIR

  # need to copy artifacts locally
  TOOLS_FILE="$OUTPUT_DIRECTORY/heron-tools-install-$HERON_VERSION-$TARGET_PLATFORM.sh"
  TOOLS_OUT_FILE="$SCRATCH_DIR/artifacts/heron-tools-install.sh"
  
  CORE_FILE="$OUTPUT_DIRECTORY/heron-core-$HERON_VERSION-$TARGET_PLATFORM.tar.gz"
  CORE_OUT_FILE="$SCRATCH_DIR/artifacts/heron-core.tar.gz"
 
  cp $TOOLS_FILE $TOOLS_OUT_FILE
  cp $CORE_FILE $CORE_OUT_FILE
 
  export HERON_VERSION

  # build the image
  echo "Building docker image with tag:$DOCKER_TAG"
  docker build --build-arg heronVersion=$HERON_VERSION -t "$DOCKER_TAG" -f "$DOCKER_FILE" "$SCRATCH_DIR"

  # save the image as a tar file
  DOCKER_IMAGE_FILE="$OUTPUT_DIRECTORY/heron-docker-$HERON_VERSION-$TARGET_PLATFORM.tar"
  
  echo "Saving docker image to $DOCKER_IMAGE_FILE"
  docker save -o $DOCKER_IMAGE_FILE $DOCKER_TAG
  gzip $DOCKER_IMAGE_FILE
}

case $# in
  3)
    build_exec_image $1 $2 $3
    ;;

  *)
    echo "Usage: $0 <platform> <version_string> <output-directory> "
    echo "  "
    echo "Platforms Supported: ubuntu14.04, ubuntu15.10, ubuntu16.04 centos7"
    echo "  "
    echo "Example:"
    echo "  ./build-exec-docker.sh ubuntu14.04 0.12.0 ."
    echo "  "
    exit 1
    ;;
esac
