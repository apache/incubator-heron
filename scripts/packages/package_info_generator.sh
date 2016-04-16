#!/bin/bash -eu

# Copyright 2015 The Bazel Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Generate a RELEASE file for the package from the information provided
# by the build status command.

# Store the build status information we care about
release_name=
git_hash=
release_status=
build_time=
build_timestamp=
build_host=
build_user=
build_commit_msg=
build_commit_url=

for i in "${@}"; do
  while read line; do
    key=$(echo "$line" | cut -d " " -f 1)
    value="$(echo "$line" | cut -d " " -f 2- | tr '\f' '\n')"
    case $key in
      HERON_BUILD_SCM_RELEASE)
        release_name="$value"
        ;;
      HERON_BUILD_SCM_REVISION)
        git_hash="$value"
        ;;
      HERON_BUILD_RELEASE_STATUS)
        release_status="$value"
        ;;
      HERON_BUILD_TIME)
        build_time="$value"
        ;;
      HERON_BUILD_TIMESTAMP)
        build_timestamp="$value"
        ;;
      HERON_BUILD_HOST)
        build_host="$value"
        ;;
      HERON_BUILD_USER)
        build_user="$value"
        ;;
      HERON_BUILD_COMMIT_MSG)
        build_commit_msg="$value"
        ;;
      HERON_BUILD_COMMIT_URL)
        build_commit_url="$value"
        ;;
   esac
  done <<<"$(cat $i)"
done

echo "Build label: ${release_name}"
echo "Build time: ${build_time}"
echo "Build timestamp: ${build_timestamp}"
echo "Build host: ${build_host}"
echo "Build user: ${build_user}"
echo "Build git revision: ${git_hash}"
echo "Build git status: ${release_status}"
echo "Build git commit msg: ${build_commit_msg}"
echo "Build git commit url: ${build_commit_url}"
