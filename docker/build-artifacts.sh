#!/bin/bash
set -o errexit

realpath() {
  echo "$(cd "$(dirname "$1")"; pwd)/$(basename "$1")"
}

DOCKER_DIR=$(dirname $(realpath $0))
PROJECT_DIR=$(dirname $DOCKER_DIR )
SRC_TAR="$DOCKER_DIR/src.tar.gz"

cleanup() {
  if [ -f $SRC_TAR ]; then
    echo "Cleaning up generate source tarball"
    rm -rf $SRC_TAR

    echo "Removing bazelrc"
    rm $DOCKER_DIR/bazelrc
  fi
}

trap cleanup EXIT

generate_source() {
  echo "Generating source tarball"
  tar --exclude-from=$DOCKER_DIR/.tarignore -C $PROJECT_DIR -czf $SRC_TAR .
}

verify_dockerfile_exists() {
  if [ ! -f $1 ]; then
    echo "The Dockerfiler $1 does not exist"
    exit 1
  fi
}

verify_source_exists() {
  if [ ! -f $1 ]; then
    echo "The source provided $1 does not exist"
    exit 1
  fi
}

dockerfile_path_for_platform() {
  echo "$DOCKER_DIR/Dockerfile.$1"
}

copy_bazel_rc() {
  cp $PROJECT_DIR/tools/docker/bazelrc $DOCKER_DIR/bazelrc
}

run_build() {
  PLATFORM=$1
  HERON_VERSION=$2
  OUTPUT_DIRECTORY=$(realpath $4)
  DOCKER_FILE=$(dockerfile_path_for_platform $PLATFORM)

  verify_dockerfile_exists $DOCKER_FILE
  copy_bazel_rc

  if [ -z "$3" ]; then
    generate_source
    SOURCE_TARBALL=$SRC_TAR
  else
    SOURCE_TARBALL=$(realpath $3)
  fi
  verify_source_exists $SOURCE_TARBALL

  echo "Building heron-compiler container"
  docker build -t heron-compiler -f $DOCKER_FILE $DOCKER_DIR

  echo "Running build in container"
  docker run \
    --rm \
    -e HERON_VERSION=$HERON_VERSION \
    -v "$SOURCE_TARBALL:/src.tar.gz:ro" \
    -v "$OUTPUT_DIRECTORY:/dist" \
    -it "heron-compiler" /compile.sh
}

case $# in
  3)
    run_build $1 $2 "" $3
    ;;

  4)
    run_build $1 $2 $3 $4
    ;;

  *)
    echo "Usage: $0 <platform> <version_string> [source-tarball] <output-directory> "
    echo "  "
    echo "Platforms: ubuntu14.04, centos7"
    echo "  "
    echo "Example:"
    echo "  ./build.sh ubuntu14.04 0.1.0-SNAPSHOT ."
    echo "  "
    echo "NOTE: If running on OSX, the output directory will need to "
    echo "      be under /Users so virtualbox has access to."
    exit 1
    ;;
esac
