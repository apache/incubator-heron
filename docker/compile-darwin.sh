#!/bin/bash
set -o nounset
set -o errexit

HERON_VERSION=$1
SCRATCH_DIR=$2
SOURCE_TARBALL=$3
OUTPUT_DIRECTORY=$4

cd $SCRATCH_DIR

echo "Extracting source"
tar -C . -xzf src.tar.gz

echo "Building heron with version $HERON_VERSION for platform Darwin"

./bazel_configure.py
bazel clean

echo "Creating packages"
bazel build --config=darwin scripts/packages:tarpkgs
bazel build --config=darwin scripts/packages:binpkgs

echo "Moving tar files to /dist"
for file in ./bazel-bin/scripts/packages/*.tar.gz; do
  filename=$(basename $file)
  cp $file $OUTPUT_DIRECTORY/${filename/.tar/-$HERON_VERSION-darwin.tar}
done

echo "Moving self extracting binaries to /dist"
for file in ./bazel-bin/scripts/packages/*.sh; do
  filename=$(basename $file)
  cp $file $OUTPUT_DIRECTORY/${filename/.sh/-$HERON_VERSION-darwin.sh}
done

