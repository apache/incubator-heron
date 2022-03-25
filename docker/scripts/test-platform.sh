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

# By default bazel creates files with mode 0555 which means they are only able to be read and not written to. This
# causes cp to fail when trying to overwrite the file. This makes sure that if the file exists we can overwrite it.
function copyFileToDest() {
  if [ -f $2 ]; then
    chmod 755 $2
  fi

  cp $1 $2
}

echo "Building heron with version $HERON_VERSION for platform $TARGET_PLATFORM"

mkdir -p $SCRATCH_DIR
cd $SCRATCH_DIR

echo "Extracting source"
tar -C . -xzf $SOURCE_TARBALL

bazel version
./bazel_configure.py
bazel clean

echo "UnitTest"
bazel test -c opt \
    --test_output=all \
    --test_summary=detailed \
    heron/... \
    heronpy/... \
    examples/... \
    storm-compatibility-examples/v0.10.2/... \
    eco-storm-examples/... \
    eco-heron-examples/... \
    contrib/...

