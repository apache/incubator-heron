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

# Install Dependencies
#sudo apt-get update && sudo apt install -y automake cmake libtool-bin g++ \
# python-setuptools python-dev python-wheel python python-pip unzip tree openjdk-8-jdk virtualenv

# Install Bazel 0.26
#wget -O ./bazel-0.26.0-installer-linux-x86_64.sh https://github.com/bazelbuild/bazel/releases/download/0.26.0/bazel-0.26.0-installer-linux-x86_64.sh && \
# chmod +x ./bazel-0.26.0-installer-linux-x86_64.sh && \
#./bazel-0.26.0-installer-linux-x86_64.sh --user && \
# export PATH="$PATH:$HOME/bin"

set -e

HERONPY_VERSION=$1
HERON_ROOT_DIR=$(git rev-parse --show-toplevel)
cd ${HERON_ROOT_DIR}

# ./bazel_configure.py

# Generate python whl packages, packages will be generated in ${HERON_ROOT_DIR}/bazel-genfiles/scripts/packages/
bazel build --config=ubuntu scripts/packages:pypkgs

cd website2/website/
mkdir -p ./tmp/
TMP_DIR=./tmp
mkdir -p ./venv/
VENV=./venv/
echo $VENV
PIP_LOCATION=${HERON_ROOT_DIR}/bazel-genfiles/scripts/packages

virtualenv "$VENV"
source "$VENV/bin/activate"

pip install pdoc==0.3.2
pip install --ignore-installed six
# Install the heronpy
pip install $PIP_LOCATION/heronpy-${HERONPY_VERSION}-py2.py3-none-any.whl

mkdir -p static/api && rm -rf static/api/python

pdoc heronpy \
  --html \
  --html-dir $TMP_DIR

mv $TMP_DIR/heronpy static/api/python
