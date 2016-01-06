#!/bin/bash
set -o errexit

realpath() {
  echo "$(cd "$(dirname "$1")"; pwd)/$(basename "$1")"
}

DOCKER_DIR=$(realpath $(dirname $0))
PROJECT_DIR=$(dirname $DOCKER_DIR )
SRC_TAR="$DOCKER_DIR/src.tar.gz"

cleanup() {
  if [ -f $SRC_TAR ]; then
    echo "Cleaning up generate source tarball"
    rm -rf $SRC_TAR
  fi
}

trap cleanup EXIT

generate_source() {
  echo "Generating source tarball"
  tar -C $PROJECT_DIR --exclude-from=$DOCKER_DIR/.tarignore -czf $SRC_TAR .
}

run_build() {
  PLATFORM=$1
  HERON_VERSION=$2
  SOURCE_TARBALL=$(realpath $3)
  OUTPUT_DIRECTORY=$(realpath $4)

  echo "Building heron-compiler container"
  docker build -t heron-compiler -f $DOCKER_DIR/Dockerfile.$PLATFORM $DOCKER_DIR

  echo "Running build in container"
  docker run \
    --rm \
    -e HERON_VERSION=$HERON_VERSION \
    -v "$SRC_TAR:/src.tar.gz:ro" \
    -v "$OUTPUT_DIRECTORY:/dist" \
    -it "heron-compiler" /compile.sh
}

case $# in
  3)
    generate_source
    run_build $1 $2 $SRC_TAR $3
    ;;

  4)
    run_build $1 $2 $3 $4
    ;;

  *)
    echo "Usage: $0 <platform> <version_string> [source-tarball] <output-directory> "
    echo "  "
    echo "Platforms: ubuntu, centos"
    echo "  "
    echo "Example:"
    echo "  ./build.sh ubuntu 0.1.0-SNAPSHOT ."
    echo "  "
    echo "NOTE: If running on OSX, the output directory will need to "
    echo "      be under /Users so virtualbox has access to."
    exit 1
    ;;
esac
