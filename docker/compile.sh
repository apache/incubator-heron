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
bazel build --config=$TARGET_PLATFORM --define RELEASE=$HERON_VERSION release:packages

echo "Building required topologies and including to /dist"
bazel build //contrib/kafka9/examples/src/java:kafka-mirror_deploy.jar

mkdir -p /dist/topologies

cp ./bazel-bin/contrib/kafka9/examples/src/java/kafka-mirror_deploy.jar /dist/topologies

echo "Moving release files to /dist"
for file in ./bazel-bin/release/*.tar.gz; do
  filename=$(basename $file)
  cp $file /dist/${filename/.tar/-$HERON_VERSION.tar}
done
