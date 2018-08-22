#!/bin/sh
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
set -e

# Check if any unshaded third party library is inside heron-instance.jar

HERON_CORE="heron-core.tar.gz"
HERON_CORE_LOC="bazel-bin/scripts/packages/${HERON_CORE}"
HERON_INSTANCE="heron-core/lib/instance/heron-instance.jar"
TMP_DIR=$(mktemp -d)

tar -xf $HERON_CORE_LOC -C $TMP_DIR 2>/dev/null
cd $TMP_DIR
THIRD_PARTIES="$(jar -tf $HERON_INSTANCE | grep -v 'org/apache' | grep -oE '(com|org)/[^/]+/' | sort | uniq)"
if [ -z "$THIRD_PARTIES" ]; then
  exit 0
else
  echo "heron-instance.jar should not contain any unshade third party libraries:"
  echo "Found: ${THIRD_PARTIES}"
  exit 1
fi
