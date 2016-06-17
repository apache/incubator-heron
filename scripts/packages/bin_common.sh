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
  echo tar xfz $1 -C $2 $TAR_X_FLAGS
  tar xfz $1 -C $2 $TAR_X_FLAGS
}

set_untar_flags
