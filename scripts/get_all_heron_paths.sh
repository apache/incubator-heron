#!/bin/bash
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
#
# Gets all libraries needed for IDE support in Bazel

set -eu

cd $(dirname "$0")
cd ../heron

function query() {
    ./output/bazel query "$@"
}

# Compile bazel
#[ -f "output/bazel" ] || ./compile.sh compile >&2 || exit $?

# Build almost everything.
#bazel build scripts/packages:heron-client-install.sh 
bazel build heron/...

# Source roots.
JAVA_PATHS="$(find heron -name "*.java" | sed "s|/src/java/.*$|/src/java|" |  sed "s|/tests/java/.*$|/tests/java|" | sort -u)"
if [ "$(uname -s | tr 'A-Z' 'a-z')" != "darwin" ]; then
  JAVA_PATHS="$(echo "${JAVA_PATHS}" | fgrep -v "/objc_tools/")"
fi
pwd
THIRD_PARTY_JAR_PATHS="$(find 3rdparty -name "*.jar" | sort -u)"
PYTHON_PATHS="$(find heron -name "*.py" | sed "s|/src/python/.*$||" |sed "s|/tests/python/.*$||" | sort -u)"

# All other generated libraries.
readonly package_list=$(find heron -name "BUILD" | sed "s|/BUILD||" | sed "s|^|//|")
# Returns the package of file $1
HERON_GEN_FILES=$(find bazel-bin/heron -type f | grep java.jar$ | grep proto)
function get_package_of() {
  # look for the longest matching package
  for i in ${package_list}; do
    if [[ "$1" =~ ^$i ]]; then  # we got a match
      echo $(echo -n $i | wc -c | xargs echo) $i
    fi
  done | sort -r -n | head -1 | cut -d " " -f 2
}

# returns the target corresponding to file $1
function get_target_of() {
  local package=$(get_package_of $1)
  local file=$(echo $1 | sed "s|^${package}/||g")
  echo "${package}:${file}"
}

# Returns the target that consume file $1
function get_consuming_target() {
  # Here to the god of bazel, I should probably offer one or two memory chips for that
  local target=$(get_target_of $1)
  # Get the rule that generated this file.
  local generating_target=$(query "kind(rule, deps(${target}, 1)) - ${target}")
  [[ -n $generating_target ]] || echo "Couldn't get generating target for ${target}" 1>&2
  local java_library=$(query "rdeps(//heron/..., ${generating_target}, 1) - ${generating_target}")
  echo "${java_library}"
}

# Returns the library that contains the generated file $1
function get_containing_library() {
  get_consuming_target $1 | sed 's|:|/lib|' | sed 's|^//|bazel-bin/|' | sed 's|$|.jar|'
}

function collect_generated_paths() {
  # uniq to avoid doing blaze query on duplicates.
  for path in $(find bazel-genfiles/ -name "*.java" | sed 's|/\{0,1\}bazel-genfiles/\{1,2\}|//|' | uniq); do
    source_path=$(echo ${path} | sed 's|//|bazel-genfiles/|' | sed 's|/com/.*$||')
    echo "$(get_containing_library ${path}):${source_path}"
  done | sort -u
}

# GENERATED_PATHS stores pairs of jar:source_path as a list of strings, with
# each pair internally delimited by a colon. Use ${string//:/ } to split one.
GENERATED_PATHS="$(collect_generated_paths)"
