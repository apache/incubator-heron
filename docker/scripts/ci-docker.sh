#!/bin/sh
set -o errexit

realpath() {
  echo "$(cd "$(dirname "$1")"; pwd)/$(basename "$1")"
}

DOCKER_DIR=$(dirname $(dirname $(realpath $0)))
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

  cp -r $DOCKER_DIR/* $1
}

build_exec_image() {
  INPUT_TARGET_PLATFORM=$1
  HERON_VERSION=$2
  DOCKER_TAG_PREFIX=$3
  OUTPUT_DIRECTORY=$(realpath $4)

  if [ "$INPUT_TARGET_PLATFORM" == "latest" ]; then
    TARGET_PLATFORM="ubuntu14.04"
    DOCKER_TAG="$DOCKER_TAG_PREFIX/heron:$HERON_VERSION"
    DOCKER_LATEST_TAG="$DOCKER_TAG_PREFIX/heron:latest"
    DOCKER_IMAGE_FILE="$OUTPUT_DIRECTORY/heron-$HERON_VERSION.tar"
  else
    TARGET_PLATFORM="$INPUT_TARGET_PLATFORM"
    DOCKER_TAG="$DOCKER_TAG_PREFIX/heron-$TARGET_PLATFORM:$HERON_VERSION"
    DOCKER_LATEST_TAG="$DOCKER_TAG_PREFIX/heron-$TARGET_PLATFORM:latest"
    DOCKER_IMAGE_FILE="$OUTPUT_DIRECTORY/heron-$TARGET_PLATFORM-$HERON_VERSION.tar"
  fi

  DOCKER_FILE="$SCRATCH_DIR/dist/Dockerfile.dist.$TARGET_PLATFORM"

  setup_scratch_dir $SCRATCH_DIR

  # need to copy artifacts locally
  TOOLS_FILE="$OUTPUT_DIRECTORY/heron-tools-install.sh"
  TOOLS_OUT_FILE="$SCRATCH_DIR/artifacts/heron-tools-install.sh"

  CORE_FILE="$OUTPUT_DIRECTORY/heron-core.tar.gz"
  CORE_OUT_FILE="$SCRATCH_DIR/artifacts/heron-core.tar.gz"

  CLIENT_FILE="$OUTPUT_DIRECTORY/heron-client-install.sh"
  CLIENT_OUT_FILE="$SCRATCH_DIR/artifacts/heron-client-install.sh"

  cp $TOOLS_FILE $TOOLS_OUT_FILE
  cp $CORE_FILE $CORE_OUT_FILE
  cp $CLIENT_FILE $CLIENT_OUT_FILE

  export HERON_VERSION

  # build the image
  echo "Building docker image with tag:$DOCKER_TAG"
  if [ "$HERON_VERSION" == "nightly" ]; then
    docker build -t "$DOCKER_TAG" -f "$DOCKER_FILE" "$SCRATCH_DIR"
  else
    docker build -t "$DOCKER_TAG" -t "$DOCKER_LATEST_TAG" -f "$DOCKER_FILE" "$SCRATCH_DIR"
  fi

  # save the image as a tar file
  echo "Saving docker image to $DOCKER_IMAGE_FILE"
  docker save -o $DOCKER_IMAGE_FILE $DOCKER_TAG
}

publish_exec_image() {
  INPUT_TARGET_PLATFORM=$1
  HERON_VERSION=$2
  DOCKER_TAG_PREFIX=$3
  INPUT_DIRECTORY=$(realpath $4)

  if [ "$INPUT_TARGET_PLATFORM" == "latest" ]; then
    TARGET_PLATFORM="ubuntu14.04"
    DOCKER_TAG="$DOCKER_TAG_PREFIX/heron:$HERON_VERSION"
    DOCKER_LATEST_TAG="$DOCKER_TAG_PREFIX/heron:latest"
    DOCKER_IMAGE_FILE="$INPUT_DIRECTORY/heron-$HERON_VERSION.tar"
  else
    TARGET_PLATFORM="$INPUT_TARGET_PLATFORM"
    DOCKER_TAG="$DOCKER_TAG_PREFIX/heron-$TARGET_PLATFORM:$HERON_VERSION"
    DOCKER_LATEST_TAG="$DOCKER_TAG_PREFIX/heron-$TARGET_PLATFORM:latest"
    DOCKER_IMAGE_FILE="$INPUT_DIRECTORY/heron-$TARGET_PLATFORM-$HERON_VERSION.tar"
  fi

  # publish the image to docker hub
  if [ "$HERON_VERSION" == "nightly" ]; then
    docker load -i $DOCKER_IMAGE_FILE
    docker push "$DOCKER_TAG"
  else
    docker load -i $DOCKER_IMAGE_FILE
    docker push "$DOCKER_TAG"
    docker push "$DOCKER_LATEST_TAG"
  fi
}

docker_image() {
  OPERATION=$1

  if [ "$OPERATION" == "build" ]; then
    build_exec_image $2 $3 $4 $5
  elif [ "$OPERATION" == "publish" ]; then
    publish_exec_image $2 $3 $4 $5
  else
    echo "invalid operation"
  fi
}

case $# in
  5)
    docker_image $1 $2 $3 $4 $5
    ;;

  *)
    echo "Usage: $0 <operation> <platform> <version_string> <tag-prefix> <input-output-directory> "
    echo "  "
    echo "Platforms Supported: latest ubuntu14.04, ubuntu15.10, ubuntu16.04 centos7"
    echo "  "
    echo "Example:"
    echo "  $0 build ubuntu14.04 0.12.0 heron ."
    echo "  $0 publish ubuntu14.04 0.12.0 streamlio ~/ubuntu"
    echo "  "
    exit 1
    ;;
esac
