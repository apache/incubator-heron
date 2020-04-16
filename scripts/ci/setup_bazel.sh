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

# Build all artifacts and put into an artifacts/ folder
# parameters:
# 1. version tag, e.g. v0.20.1-incubating
# 2. output dir

set -e
set -o pipefail

if [ "$#" -ne 1 ]; then
    echo "ERROR: Wrong number of arguments. Usage '$0 OS[linux|darwin]'"
    exit 1
fi
BAZEL_OS=$1
BAZEL_VERSION=3.0.0

# Install Bazel
BAZEL_INSTALLER=bazel-$BAZEL_VERSION-installer-$BAZEL_OS-x86_64.sh
BAZEL_INSTALLER_LOC="https://github.com/bazelbuild/bazel/releases/download/$BAZEL_VERSION/$BAZEL_INSTALLER"

wget -O /tmp/$BAZEL_INSTALLER $BAZEL_INSTALLER_LOC
chmod +x /tmp/$BAZEL_INSTALLER
/tmp/$BAZEL_INSTALLER --user

chmod +x ./bazel_configure.py
./bazel_configure.py
$HOME/bin/bazel clean
