#!/usr/bin/env bash
set -o errexit -o nounset -o pipefail

DIR=`dirname $0`
UTILS=${DIR}/../shutils
source ${UTILS}/common.sh

# Autodiscover the platform
PLATFORM=$(discover_platform)
echo "Using $PLATFORM platform"

# install heron locally
bazel --bazelrc=tools/travis/bazel.rc run --config=$PLATFORM -- scripts/packages:heron-install.sh --user
