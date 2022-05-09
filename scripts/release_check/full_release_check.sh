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

set -o errexit

if [  $# -lt 1 ]
  then
    echo "Usage:"
    echo "$0 PAT_TO_APACHE_RAT_JAR"
    echo ""
    echo "Note: Apache Rat package can be downloaded from:"
    echo "  http://ftp.wayne.edu/apache/creadur/apache-rat-0.13/apache-rat-0.13-bin.tar.gz"
    exit 1
  fi

RAT_JAR_PATH=$1

echo "Check licenses..."
sh ./scripts/release_check/license_check.sh $RAT_JAR_PATH

echo "Build artifacts..."
sh ./scripts/release_check/build.sh

echo "Run a test topology locally..."
sh ./scripts/release_check/run_test_topology.sh

echo "Build debian11 docker image..."
sh ./scripts/release_check/build_docker.sh
