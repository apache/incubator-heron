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

function set_untar_command {
  # Some tar implementations emit verbose timestamp warnings, allowing the ability to disable them
  # via --warning=no-timestamp (which we want to do in that case). To find out if we have one of
  # those implementations, we see if help returns an error for that flag.
  SUPPRESS_TAR_TS_WARNINGS="--warning=no-timestamp"
  tar $SUPPRESS_TAR_TS_WARNINGS --help &> /dev/null && export TAR_X_FLAGS=$SUPPRESS_TAR_TS_WARNINGS
  UNTAR_CMD="tar xfz $TAR_X_FLAGS"
}

set_untar_command
