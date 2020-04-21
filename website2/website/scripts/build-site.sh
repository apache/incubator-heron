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

ROOT_DIR=$(git rev-parse --show-toplevel)
# @TODO
VERSION=0.20

set -x -e

cd ${ROOT_DIR}/website2/website
npm install yarn
npm run-script build

node ./scripts/replace.js

rm -rf ${ROOT_DIR}/generated-site/content
mkdir -p ${ROOT_DIR}/generated-site/content/api/java
mkdir -p ${ROOT_DIR}/generated-site/content/api/python
## copy generated site
cp -R build/incubator-heron/* ${ROOT_DIR}/generated-site/content
## copy java docs
cp -R ${ROOT_DIR}/website2/website/public/api/java/* ${ROOT_DIR}/generated-site/content/api/java/
## copy pydocs
cp -R ${ROOT_DIR}/website2/website/static/api/python/* ${ROOT_DIR}/generated-site/content/api/python/
## remove bazelrc for dockerfile
rm ${ROOT_DIR}/website2/website/scripts/bazelrc