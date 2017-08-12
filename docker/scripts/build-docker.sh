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

  cp $DOCKER_DIR/dist/* $1
}

run_build() {
  TARGET_PLATFORM=$1
  HERON_VERSION=$2
  OUTPUT_DIRECTORY=$(realpath $3)
  DOCKER_FILE="$SCRATCH_DIR/Dockerfile.dist.$TARGET_PLATFORM"
  DOCKER_TAG="streamlio/heron-$TARGET_PLATFORM:$HERON_VERSION"

  setup_scratch_dir $SCRATCH_DIR

  # need to copy artifacts locally
  TOOLS_FILE="$OUTPUT_DIRECTORY/heron-tools-install-$HERON_VERSION-$TARGET_PLATFORM.sh"
  TOOLS_OUT_FILE="$SCRATCH_DIR/artifacts/heron-tools-install.sh"

  CLIENT_FILE="$OUTPUT_DIRECTORY/heron-client-install-$HERON_VERSION-$TARGET_PLATFORM.sh"
  CLIENT_OUT_FILE="$SCRATCH_DIR/artifacts/heron-client-install.sh"

  CORE_FILE="$OUTPUT_DIRECTORY/heron-core-$HERON_VERSION-$TARGET_PLATFORM.tar.gz"
  CORE_OUT_FILE="$SCRATCH_DIR/artifacts/heron-core.tar.gz"

  SCRIPT_FILE="$DOCKER_DIR/start-services.sh"
  SCRIPT_OUT_FILE="$SCRATCH_DIR/artifacts/start-services.sh"

  cp $TOOLS_FILE $TOOLS_OUT_FILE
  cp $CLIENT_FILE $CLIENT_OUT_FILE
  cp $CORE_FILE $CORE_OUT_FILE
  cp $SCRIPT_FILE $SCRIPT_OUT_FILE

  export HERON_VERSION

  echo "Building docker image with tag:$DOCKER_TAG"
  docker build -t "$DOCKER_TAG" -f "$DOCKER_FILE" "$SCRATCH_DIR"

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
    echo "  Input - directory containing the artifacts from the <output-directory>"
    echo "  Output - docker image tar file saved in the <output-directory> "
    echo "  "
    echo "Usage: $0 <platform> <version_string> <output-directory> "
    echo "  "
    echo "Platforms Supported: ubuntu, centos"
    echo "  "
    echo "Example:"
    echo "  ./build-docker.sh ubuntu 0.12.0 ~/ubuntu"
    echo "  "
    echo "  ./build-docker.sh centos 0.15.0 ."
    echo "  "
    exit 1
    ;;
esac
