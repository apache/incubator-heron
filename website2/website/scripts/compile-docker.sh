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
echo "docker dir: $DOCKER_DIR"
PROJECT_DIR=$(dirname $DOCKER_DIR)
echo "project dir: $PROJECT_DIR"

verify_dockerfile_exists() {
  if [ ! -f $1 ]; then
    echo "The Dockerfile $1 does not exist"
    exit 1
  fi
}

dockerfile_path_for_platform() {
  echo "$PROJECT_DIR/website/scripts/Dockerfile.$1"
}

copy_bazel_rc_to() {
  cp $PROJECT_DIR/../tools/docker/bazel.rc $1
}



TARGET_PLATFORM="ubuntu18.04"
DOCKER_FILE=$(dockerfile_path_for_platform $TARGET_PLATFORM)
verify_dockerfile_exists $DOCKER_FILE
copy_bazel_rc_to  $PROJECT_DIR/website/scripts/bazelrc

echo "docker file"
echo $DOCKER_FILE

echo "Building heron-compiler container"
docker build \
  --build-arg UNAME=$USER \
  --build-arg UID=$(id -u ${USER}) \
  --build-arg GID=$(id -g ${USER}) \
  -t heron-compiler:$TARGET_PLATFORM -f $DOCKER_FILE .


docker run \
  --rm \
  -u `id -u`:`id -g` \
  -v $PROJECT_DIR/..:/home/$USER/heron \
  -v /etc/passwd:/etc/passwd \
  -t heron-compiler:$TARGET_PLATFORM  make -C /home/$USER/heron/website2/website/ buildsite

