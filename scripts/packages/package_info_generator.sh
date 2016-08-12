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
build_version=
git_hash=
release_status=
build_time=
build_timestamp=
build_host=
build_user=

for i in "${@}"; do
  while read line; do
    key=$(echo "$line" | cut -d " " -f 1)
    value="$(echo "$line" | cut -d " " -f 2- | tr '\f' '\n')"
    case $key in
      HERON_BUILD_VERSION)
        build_version="$value"
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
   esac
  done <<<"$(cat $i)"
done

echo "heron.build.version : '${build_version}'"
echo "heron.build.time : ${build_time}"
echo "heron.build.timestamp : ${build_timestamp}"
echo "heron.build.host : ${build_host}"
echo "heron.build.user : ${build_user}"
echo "heron.build.git.revision : ${git_hash}"
echo "heron.build.git.status : ${release_status}"
