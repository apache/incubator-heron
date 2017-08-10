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
  DOCKER_TAG="heron:$HERON_VERSION-$TARGET_PLATFORM"

  setup_scratch_dir $SCRATCH_DIR

  #need to copy artifacts locally
  echo "Building docker image with tag:$DOCKER_TAG"
  cp -pr "$OUTPUT_DIRECTORY"/*$HERON_VERSION* "$SCRATCH_DIR/artifacts"
  mv $SCRATCH_DIR/artifacts/heron-tools-install-$HERON_VERSION-$TARGET_PLATFORM.sh $SCRATCH_DIR/artifacts/heron-tools-install.sh
  mv $SCRATCH_DIR/artifacts/heron-core-$HERON_VERSION-$TARGET_PLATFORM.tar.gz $SCRATCH_DIR/artifacts/heron-core.tar.gz

  export HERON_VERSION
  docker build -t "$DOCKER_TAG" -f "$DOCKER_FILE" "$SCRATCH_DIR"
}

case $# in
  3)
    run_build $1 $2 $3
    ;;

  *)
    echo "Usage: $0 <platform> <version_string> <output-directory> "
    echo "  "
    echo "Example:"
    echo "  ./build-docker.sh ubuntu14.04 0.12.0 ."
    echo "  "
    exit 1
    ;;
esac
