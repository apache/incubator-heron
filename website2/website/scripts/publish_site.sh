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

set -e

ROOT_DIR=$(git rev-parse --show-toplevel)
WORK_DIR=${ROOT_DIR}/generated-site/content
ME=`basename $0`

ORIGIN_REPO=$(git remote show origin | grep 'Push  URL' | awk -F// '{print $NF}')
echo "ORIGIN_REPO: $ORIGIN_REPO"

HERON_SITE_TMP=/tmp/heron-site
(

  cd $ROOT_DIR
  rm -rf $HERON_SITE_TMP
  mkdir $HERON_SITE_TMP
  cd $HERON_SITE_TMP

  git clone "https://$GH_TOKEN@$ORIGIN_REPO" .
  git config user.name "Heron Site Updater"
  git config user.email "dev@heron.incubator.apache.org"
  git checkout asf-site


   # clean content directory
  rm -rf $HERON_SITE_TMP/content/
  mkdir $HERON_SITE_TMP/content

  # copy the generated dir
  cp -r $WORK_DIR/* $HERON_SITE_TMP/content

  #  copy the asf.yaml
  cp $ROOT_DIR/.asf.yaml $HERON_SITE_TMP/

  # push all of the results to asf-site branch
  git add -A .
  git diff-index --quiet HEAD || (git commit -m "git-site-role commit from $ME" && git push -q origin HEAD:asf-site)
  rm -rf $HERON_SITE_TMP

)