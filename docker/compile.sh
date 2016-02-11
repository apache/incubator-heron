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

echo "Creating release packages"
bazel build --config=$TARGET_PLATFORM release:packages

echo "Moving release files to /dist"
for file in ./bazel-bin/release/*.tar.gz; do
  filename=$(basename $file)
  cp $file /dist/${filename/.tar/-$HERON_VERSION.tar}
done
