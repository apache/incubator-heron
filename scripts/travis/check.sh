#!/bin/sh
set -e

# Check if any unshaded third party library is inside heron-instance.jar

HERON_CORE="heron-core.tar.gz"
HERON_CORE_LOC="bazel-bin/scripts/packages/${HERON_CORE}"
HERON_INSTANCE="heron-core/lib/instance/heron-instance.jar"
TMP_DIR=$(mktemp -d)

tar -xf $HERON_CORE_LOC -C $TMP_DIR 2>/dev/null
cd $TMP_DIR
THIRD_PARTIES="$(jar -tf $HERON_INSTANCE | grep -v 'com/twitter' | grep -oE '(com|org)/[^/]+/' | sort | uniq)"
if [ -z "$THIRD_PARTIES" ]; then
  exit 0
else
  echo "heron-instance.jar should not contain any unshade third party libraries:"
  echo "Found: ${THIRD_PARTIES}"
  exit 1
fi
