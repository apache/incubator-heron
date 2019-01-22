#!/bin/bash -e
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

function set_untar_flags {
  # Some tar implementations emit verbose timestamp warnings, allowing the ability to disable them
  # via --warning=no-timestamp (which we want to do in that case). To find out if we have one of
  # those implementations, we see if help returns an error for that flag.
  SUPPRESS_TAR_TS_WARNINGS="--warning=no-timestamp"
  tar $SUPPRESS_TAR_TS_WARNINGS --help &> /dev/null && TAR_X_FLAGS=$SUPPRESS_TAR_TS_WARNINGS
  # echo this so function doesn't return 1
  echo $TAR_X_FLAGS
}

# Untars a gzipped archived to an output dir. Lazily creates dir if it doesn't exit
function untar {
  if (( $# < 2 )); then
    echo "Usage: untar <tar_file> <output_dir>" >&2
    echo "Args passed: $@" >&2
    exit 1
  fi
  [ -d "$2" ] || mkdir -p $2
  tar xfz $1 -C $2 $TAR_X_FLAGS
}

function test_write() {
  local file="$1"
  while [ "$file" != "/" ] && [ -n "${file}" ] && [ ! -e "$file" ]; do
    file="$(dirname "${file}")"
  done
  [ -w "${file}" ] || {
    echo >&2
    echo "The Heron installer must have write access to $1!" >&2
    echo >&2
    usage
  }
}

# Test for unzip dependencies
function check_unzip() {
  if ! which unzip >/dev/null; then
    echo >&2
    echo "unzip not found, please install the corresponding package." >&2
    echo "See $getting_started_url for more information on" >&2
    echo "dependencies of Heron." >&2
    exit 1
  fi
}

# Test for tar dependencies
function check_tar() {
  if ! which tar >/dev/null; then
    echo >&2
    echo "tar not found, please install the corresponding package." >&2
    echo "See $getting_started_url for more information on" >&2
    echo "dependencies of Heron." >&2
    exit 1
  fi
}

# Test for java dependencies
function check_java() {
  if [ -z "${JAVA_HOME-}" ]; then
    case "$(uname -s | tr 'A-Z' 'a-z')" in
      linux)
        JAVA_HOME="$(readlink -f $(which java) 2>/dev/null | sed 's_/bin/java__')" || true
        BASHRC="~/.bashrc"
        ;;
      freebsd)
        JAVA_HOME="/usr/local/openjdk8"
        BASHRC="~/.bashrc"
        ;;
      darwin)
        JAVA_HOME="$(/usr/libexec/java_home -v ${JAVA_VERSION}+ 2> /dev/null)" || true
        BASHRC="~/.bash_profile"
        ;;
    esac
  fi
  if [ ! -x "${JAVA_HOME}/bin/java" ]; then
    echo >&2
    echo "Java not found, please install the corresponding package" >&2
    echo "See $getting_started_url for more information on" >&2
    echo "dependencies of Heron." >&2
    exit 1
  fi
}

set_untar_flags
