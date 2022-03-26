#!/usr/bin/env bash
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
:<<'DOC'
This script is for running tests in a local VM, similar to the environment used in the CI pipeline. If the targent script fails, a shell will be opened up within the VM.

To only run integration tests:
  ./local-ci.sh test

To run the full ci pipeline:
  ./local-ci.sh ci

DOC

set -o errexit -o nounset -o pipefail
HERE="$(cd "$(dirname "$0")" && pwd -P)"

cd "$HERE"

state="$(vagrant status primary --machine-readable | grep primary,state, | cut -d, -f4)"
if [ "$state" != "running" ]; then
    vagrant up primary
fi


# allows you to do `$0 test` to run only integration tests
script="${1-ci}"
env="JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/"
# run the CI, if it fails drop into a shell
vagrant ssh primary --command "cd /vagrant && $env ./scripts/travis/$script.sh" \
    || vagrant ssh primary --command "cd /vagrant && $env exec bash"
