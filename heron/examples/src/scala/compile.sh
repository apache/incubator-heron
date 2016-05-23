#!/bin/bash
#
# Compiles the scala classes below with Heron and Storm jars to verify compile compatibility. Once
# We have scala/bazel support we should remote this script and replace with bazel targets.
#

set -ex

dir=$(dirname $0)
root="$dir/../../../.."
function die {
  echo $1
  exit 1
}

which scalac || die "scalac must be installed to run this script. Exiting."

rm -f heron-storm.jar
(cd $root && bazel build --config=darwin scripts/packages:tarpkgs)

# Verify storm and heron bolts compile with heron-storm.jar
scalac -cp bazel-genfiles/./heron/storm/src/java/heron-storm.jar \
  $dir/com/twitter/heron/examples/*.scala
