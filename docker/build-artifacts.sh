#!/bin/bash
set -o errexit

realpath() {
  echo "$(cd "$(dirname "$1")"; pwd)/$(basename "$1")"
}

DOCKER_DIR=$(dirname $(realpath $0))
PROJECT_DIR=$(dirname $DOCKER_DIR )
SCRATCH_DIR="$HOME/.heron-compile"
SRC_TAR="$SCRATCH_DIR/src.tar.gz"

heron_git_release() {
  local git_release=$(git rev-parse --abbrev-ref HEAD)
  if [[ $? != 0 ]];
  then
    exit 1 
  fi
  if [ "${git_release}" = "HEAD" ];
  then
    git_release=$(git describe --tags)
    if [[ $? != 0 ]];
    then
      exit 1
    fi
  fi
  echo $git_release 
}

heron_git_rev() {
  local git_rev=$(git rev-parse HEAD)
  if [[ $? != 0 ]];
  then
    exit 1
  fi
  echo $git_rev
}

heron_commit_msg() {
  local commit_msg=$(git log -1 --oneline | cut -f 2- -d ' ')
  if [[ $? != 0 ]];
  then
    exit 1
  fi
  echo $commit_msg
}

heron_build_host() {
  local build_host=$(hostname)
  echo $build_host
}

heron_build_user() {
  local build_user=$USER
  echo $build_user
}

heron_build_time() {
  local build_time=$(date)
  echo $build_time
}

heron_build_timestamp() {
  local build_timestamp=$(date +%s000)
  echo $build_timestamp
}

heron_tree_status() {
  local tree_status=""
  git diff-index --quiet HEAD --
  if [[ $? == 0 ]];
  then
    tree_status="Clean"
  else
    tree_status="Modified"
  fi
  echo $tree_status
}

cleanup() {
  if [ -f $SRC_TAR ]; then
    echo "Cleaning up scratch dir"
    rm -rf $SCRATCH_DIR
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
  echo "$SCRATCH_DIR/Dockerfile.$1"
}

copy_bazel_rc_to() {
  cp $PROJECT_DIR/tools/docker/bazel.rc $1
}

setup_scratch_dir() {
  if [ ! -f "$1" ]; then
    mkdir $1
  fi

  cp $DOCKER_DIR/* $1
}

setup_output_dir() {
  if [ ! -d $1 ]; then
    echo "Creating output directory $1"
    mkdir $1
  fi
}

run_build() {
  PLATFORM=$1
  HERON_VERSION=$2
  OUTPUT_DIRECTORY=$(realpath $4)
  SOURCE_TARBALL=$3
  DOCKER_FILE=$(dockerfile_path_for_platform $PLATFORM)

  setup_scratch_dir $SCRATCH_DIR
  setup_output_dir $OUTPUT_DIRECTORY
  verify_dockerfile_exists $DOCKER_FILE
  copy_bazel_rc_to  $SCRATCH_DIR/bazelrc

  if [ -z "$SOURCE_TARBALL" ]; then
    generate_source
    SOURCE_TARBALL=$SRC_TAR
  else
    SOURCE_TARBALL=$(realpath $3)
  fi
  verify_source_exists $SOURCE_TARBALL

  echo "Building heron-compiler container"
  docker build -t heron-compiler:$PLATFORM -f $DOCKER_FILE $SCRATCH_DIR

  echo "Running build in container"
  docker run \
    --rm \
    -e HERON_VERSION=$HERON_VERSION \
    -e HERON_GIT_RELEASE="$(heron_git_release)" \
    -e HERON_GIT_REV="$(heron_git_rev)" \
    -e HERON_GIT_COMMIT_MSG="$(heron_commit_msg)" \
    -e HERON_BUILD_HOST="$(heron_build_host)" \
    -e HERON_BUILD_USER="$(heron_build_user)" \
    -e HERON_BUILD_TIME="$(heron_build_time)" \
    -e HERON_TREE_STATUS="$(heron_tree_status)" \
    -v "$SOURCE_TARBALL:/src.tar.gz:ro" \
    -v "$OUTPUT_DIRECTORY:/dist" \
    -it heron-compiler:$PLATFORM /compile.sh
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
    echo "  ./build-artifacts.sh ubuntu14.04 0.12.0 ."
    echo "  "
    echo "NOTE: If running on OSX, the output directory will need to "
    echo "      be under /Users so virtualbox has access to."
    exit 1
    ;;
esac
