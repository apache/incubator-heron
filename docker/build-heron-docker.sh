#!/bin/bash
set -o errexit

realpath() {
  echo "$(cd "$(dirname "$1")"; pwd)/$(basename "$1")"
}

run_build() {
  DOCKER_DIR=$(dirname $(realpath $0))

  TARGET_PLATFORM=$1
  HERON_VERSION=$2
  OUTPUT_DIRECTORY=$3

  if ! [[ "$TARGET_PLATFORM" =~ "ubuntu" ]]; then
    echo "ubuntu14.04 and ubuntu16.04 are the only platforms supported"
    exit 1
  fi

  $DOCKER_DIR/build-artifacts.sh $TARGET_PLATFORM $HERON_VERSION $OUTPUT_DIRECTORY

  $DOCKER_DIR/build-docker.sh $TARGET_PLATFORM $HERON_VERSION $OUTPUT_DIRECTORY

  echo "Finshed building docker image heron:$HERON_VERSION-$TARGET_PLATFORM"
}

case $# in
  3)
    run_build $1 $2 $3
    ;;

  *)
    echo "Usage: $0 <platform> <version_string> <output-directory> "
    echo "  "
    echo "Example:"
    echo "  ./build-heron-docker.sh ubuntu14.04 0.12.0 ~/heron-release"
    echo "  "
    exit 1
    ;;
esac
