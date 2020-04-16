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
#
# Compiles the scala classes below with Heron and Storm APIs to verify compile compatibility. Once
# We have scala/bazel support we should remove this script and replace with bazel targets.
#

set -ex

dir=$(dirname $0)
root="$dir/../../../.."
function die {
  echo $1
  exit 1
}

which scalac || die "scalac must be installed to run this script. Exiting."

rm -f heron-storm.jar
(cd $root && bazel build --config=darwin scripts/packages:tarpkgs)

# Verify storm and heron bolts compile with heron-storm.jar
scalac -cp bazel-bin/./storm-compatibility/src/java/heron-storm.jar \
  $dir/org/apache/heron/examples/*.scala
