#!/usr/bin/env bash
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
set -e

##### Pyheron library creator ######
# Automatically creates an egg
#
# directory structure
# ├── heron
# │   ├── common
# │   │   └── src
# │   │       └── python
# │   │           ├── basics
# │   │           ├── network
# │   │           └── utils
# │   │               ├── metrics
# │   │               ├── misc
# │   │               └── topology
# │   └── proto
# ├── pyheron
# │   ├── bolt
# │   ├── component
# │   └── spout
# ├── setup.py
# └── requirements.txt

TMP_DIR=`mktemp -d`
PYHERON_TOPLEVEL_DIR="${TMP_DIR}/pyheron"
UNZIP_DEST="${TMP_DIR}/unzipped"
HERON_DIR=`git rev-parse --show-toplevel`
BUILT_PKG_PATH=${HERON_DIR}/bazel-bin/heron/streamparse/src/python/pyheron_pkg.pex

function usage() {
  echo "Usage: $progname [options]" >&2
  echo "  --output=/some/path set the destination egg path" >&2
  exit 1
}

OUT_PATH=${PWD}
for opt in "${@}"; do
  case $opt in
    --output=*)
      OUT_PATH="$(echo "$opt" | cut -d '=' -f 2-)"
      ;;
    *)
      usage
      ;;
  esac
done

# build pyheron_pkg pex file
cd ${HERON_DIR}
bazel build heron/streamparse/src/python:pyheron_pkg
unzip -qd $UNZIP_DEST $BUILT_PKG_PATH

# creates a pyheron directory
mkdir -p $PYHERON_TOPLEVEL_DIR

# copis everything under heron to pyheron dir
cp -R ${UNZIP_DEST}/heron ${PYHERON_TOPLEVEL_DIR}

# remove all pyc files
find ${PYHERON_TOPLEVEL_DIR} -name "*.pyc" -exec rm {} \;

# move streamparse/src/python -> pyheron
mv ${PYHERON_TOPLEVEL_DIR}/heron/streamparse/src/python ${PYHERON_TOPLEVEL_DIR}/pyheron
rm -rf ${PYHERON_TOPLEVEL_DIR}/heron/streamparse

# move heron/examples/src/python -> heron/examples
mv ${PYHERON_TOPLEVEL_DIR}/heron/examples/src/python/* ${PYHERON_TOPLEVEL_DIR}/heron/examples
rm -rf ${PYHERON_TOPLEVEL_DIR}/heron/examples/src

# path change
find ${PYHERON_TOPLEVEL_DIR}/heron/examples -name "*.py" -exec sed -i '' -e 's/heron\.examples\.src\.python/heron\.examples/g' {} \;
find ${PYHERON_TOPLEVEL_DIR}/heron/examples -name "*.py" -exec sed -i '' -e 's/heron\.streamparse\.src\.python/pyheron/g' {} \;

# copies setup.py and requirements.txt
cp ${HERON_DIR}/scripts/packages/pyheron/setup.py ${PYHERON_TOPLEVEL_DIR}
cp ${HERON_DIR}/scripts/packages/pyheron/requirements.txt ${PYHERON_TOPLEVEL_DIR}

# clean up
rm -rf $UNZIP_DEST

echo "PyHeron toplevel directory: ${PYHERON_TOPLEVEL_DIR}"
tree $PYHERON_TOPLEVEL_DIR

echo "=========== Creating egg file ============"
cd ${PYHERON_TOPLEVEL_DIR}
python setup.py bdist_egg

echo "=========== Copying egg file ============"
cp ${PYHERON_TOPLEVEL_DIR}/dist/*.egg ${OUT_PATH}

