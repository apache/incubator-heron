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
WORK_DIR=${ROOT_DIR}/generated-site/content
ME=`basename $0`

# push all of the results to asf-site branch
git checkout asf-site
git clean -f -d
git pull origin asf-site

rm -rf ${ROOT_DIR}/content
mkdir ${ROOT_DIR}/content

cp -a $WORK_DIR/* ${ROOT_DIR}/content
cp -a ${ROOT_DIR}/.htaccess ${ROOT_DIR}/content

git add ${ROOT_DIR}/content
git commit -m "git-site-role commit from $ME"
git push origin asf-site
