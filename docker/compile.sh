#!/bin/bash
set -o nounset
set -o errexit

echo "Building heron with version $HERON_VERSION for platform $TARGET_PLATFORM"

mkdir /scratch
cd /scratch

echo "Extracting source"
tar -C . -xzf /src.tar.gz

./bazel_configure.py
bazel clean

echo "Creating packages"
bazel build --config=$TARGET_PLATFORM scripts/packages:tarpkgs
bazel build --config=$TARGET_PLATFORM scripts/packages:binpkgs

echo "Moving tar files to /dist"
for file in ./bazel-bin/scripts/packages/*.tar.gz; do
  filename=$(basename $file)
  cp $file /dist/${filename/.tar/-$HERON_VERSION-$TARGET_PLATFORM.tar}
done

echo "Moving self extracting binaries to /dist"
for file in ./bazel-bin/scripts/packages/*.sh; do
  filename=$(basename $file)
  cp $file /dist/${filename/.sh/-$HERON_VERSION-$TARGET_PLATFORM.sh}
done
