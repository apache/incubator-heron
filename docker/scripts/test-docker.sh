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
set -o nounset
set -o errexit

realpath() {
  echo "$(cd "$(dirname "$1")"; pwd)/$(basename "$1")"
}

DOCKER_DIR=$(dirname $(dirname $(realpath $0)))
PROJECT_DIR=$(dirname $DOCKER_DIR)

verify_dockerfile_exists() {
  if [ ! -f $1 ]; then
    echo "The Dockerfiler $1 does not exist"
    exit 1
  fi
}

dockerfile_path_for_platform() {
  echo "$SCRATCH_DIR/test/Dockerfile.$1"
}

copy_bazel_rc_to() {
  cp $PROJECT_DIR/tools/docker/bazel.rc $1
}

DOCKER_FILE=$(dockerfile_path_for_platform $TARGET_PLATFORM)
verify_dockerfile_exists $DOCKER_FILE
copy_bazel_rc_to  $SCRATCH_DIR/bazelrc

echo "Building heron-compiler container"
docker build -t heron-compiler:$TARGET_PLATFORM -f $DOCKER_FILE $SCRATCH_DIR

echo "Running build in container"
docker run \
    --rm \
    -e TARGET_PLATFORM=$TARGET_PLATFORM \
    -e SCRATCH_DIR="/scratch" \
    -e SOURCE_TARBALL="/src.tar.gz" \
    -e HERON_VERSION=$HERON_VERSION \
    -e HERON_GIT_REV="${HERON_GIT_REV}" \
    -e HERON_BUILD_VERSION="${HERON_BUILD_VERSION}" \
    -e HERON_BUILD_HOST="${HERON_BUILD_HOST}" \
    -e HERON_BUILD_USER="${HERON_BUILD_USER}" \
    -e HERON_BUILD_TIME="${HERON_BUILD_TIME}" \
    -e HERON_TREE_STATUS="${HERON_TREE_STATUS}" \
    -v "$SOURCE_TARBALL:/src.tar.gz:ro" \
    -t heron-compiler:$TARGET_PLATFORM /test-platform.sh
