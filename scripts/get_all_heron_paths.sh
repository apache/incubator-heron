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

function query() {
  ./output/bazel query "$@"
}

set +e
# Build everything
DIR=`dirname $0`
source ${DIR}/detect_os_type.sh
bazel build --config=`platform` {heron,integration_test,tools/java,examples,heronpy,storm-compatibility,storm-compatibility-examples,eco,eco-storm-examples,eco-heron-examples}/...
result=$?
if [ "${result}" -eq "0" ] ; then
  echo "Bazel build successful!!"
else
  echo "WARNING!!! - bazel build failed - intellij setup may not be consistent"
fi 
set -e
echo "Path is `pwd`"

function get_heron_python_paths() {
  echo "$(find heron -name "*.py" | sed "s|/src/python/.*$|/src/python/|" |sed "s|/tests/python/.*$|/tests/python/|" | sort -u)";
}

function get_heron_thirdparty_dependencies() {
  # bazel-bin/heron/proto for heron proto jars from heron/proto
  # bazel-genfiles/external for third_party deps
  # bazel-heron/bazel-out/host/bin/third_party for extra_action proto jars in third_party
  # bazel-heron/bazel-out/host/genfiles/external more third_party deps
  echo "$(find {bazel-bin/heron/proto,bazel-genfiles/external,bazel-incubator-heron/bazel-out/host/bin/third_party,bazel-incubator-heron/bazel-out/host/genfiles/external}/. -name "*jar" -type f | sort -u)";
}

function get_heron_bazel_deps(){
  local bazel_third_party_base="$(bazel info output_base)/external/bazel_tools/third_party/";
  local bazel_ext_deps=`bazel query 'labels("deps", heron/...)'  | egrep -E "bazel_tools"`;
  local heron_resolved_deps=`for dep in $bazel_ext_deps; do bazel query "$dep" --output xml | grep "<label" | grep "\.jar" | sed 's/<label value="//' | sed 's/"\/\>//'| sort -u | sed 's/\/\/third_party/$MODULE_DIR\/third_party/' | sed "s|\@bazel_tools\/\/third_party\:|$bazel_third_party_base|" ; done`;
  echo "${heron_resolved_deps}";
}

# All other generated libraries.
readonly package_list=$(find heron -name "BUILD" | sed "s|/BUILD||" | sed "s|^|//|")
# Returns the package of file $1
function get_package_of() {
  # look for the longest matching package
  for i in ${package_list}; do
    if [[ "$1" =~ ^$i ]]; then  # we got a match
      echo $(echo -n $i | wc -c | xargs echo) $i
    fi
  done | sort -r -n | head -1 | cut -d " " -f 2
}

function get_heron_java_paths() {
  local java_paths=$(find {heron,heron/tools,tools,integration_test,examples,storm-compatibility,eco,eco-storm-examples,eco-heron-examples,contrib} -name "*.java" | sed "s|/src/java/.*$|/src/java|"| sed "s|/java/src/.*$|/java/src|" |  sed "s|/tests/java/.*$|/tests/java|" | sort -u | fgrep -v "heron/scheduler/" | fgrep -v "heron/scheduler/" )
  if [ "$(uname -s | tr 'A-Z' 'a-z')" != "darwin" ]; then
    java_paths=$(echo "${java_paths}" | fgrep -v "/objc_tools/")
  fi
  echo "${java_paths}"
}

function get_heron_source_paths() {
  local java_paths=$(get_heron_java_paths)
  local python_paths=$(get_heron_python_paths)
  echo "$java_paths $python_paths";
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

function collect_generated_binary_deps() {
  local proto_deps=$(find bazel-bin/heron/proto -type f | grep "jar$");
  echo "${proto_deps}" | sort | uniq
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
