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

# Build packages to be released
# parameters:
# 1. version tag, e.g. v0.20.1-incubating
# 2. build os, e.g. darwin, rocky8
# 3. output dir

# Related environment variables
# HERON_BUILD_USER
# HERON_BUILD_HOST

set -e
set -o pipefail

if [ "$#" -ne 3 ]; then
    echo "ERROR: Wrong number of arguments. Usage '$0 VERSION_TAG BUILD_OS OUTPUT_DIR'"
    exit 1
fi
VERSION_TAG=$1
BUILD_OS=$2
OUTPUT_DIR=$3

TEMP_RELEASE_DIR=~/heron-release

# Clear out the pex cache
rm -rf /var/lib/jenkins/.pex/build/*


# Build with docker
chmod +x scripts/release/status.sh
chmod +x heron/downloaders/src/shell/heron-downloader.sh
chmod +x heron/tools/apiserver/src/shell/heron-apiserver.sh

bash scripts/release/status.sh

# Create a temporary directory for generated files
mkdir -p $TEMP_RELEASE_DIR
rm -f $TEMP_RELEASE_DIR/*.*
./docker/scripts/build-artifacts.sh $BUILD_OS $VERSION_TAG $TEMP_RELEASE_DIR

# Cherry-pick files to output dir
mkdir -p $OUTPUT_DIR
cp $TEMP_RELEASE_DIR/heron-${VERSION_TAG}-${BUILD_OS}.tar.gz $OUTPUT_DIR
cp $TEMP_RELEASE_DIR/heron-install-${VERSION_TAG}-${BUILD_OS}.sh $OUTPUT_DIR

# Remove temporary directory
rm -rf $TEMP_RELEASE_DIR
