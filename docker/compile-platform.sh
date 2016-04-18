#!/bin/bash
set -o nounset
set -o errexit

echo "Building heron with version $HERON_VERSION for platform $TARGET_PLATFORM"

mkdir -p $SCRATCH_DIR
cd $SCRATCH_DIR

echo "Extracting source"
tar -C . -xzf $SOURCE_TARBALL

CONFIG_PLATFORM=
if [[ "$TARGET_PLATFORM" =~ "ubuntu" ]]; then
  CONFIG_PLATFORM=ubuntu
elif [[ "$TARGET_PLATFORM" =~ "centos" ]]; then
  CONFIG_PLATFORM=centos
elif [[ "$TARGET_PLATFORM" =~ "darwin" ]]; then
  CONFIG_PLATFORM=darwin
else 
  echo "Unknown platform: $TARGET_PLATFORM"
  exit 1
fi

./bazel_configure.py
bazel clean

echo "Creating packages"
bazel build --config=$CONFIG_PLATFORM scripts/packages:tarpkgs
bazel build --config=$CONFIG_PLATFORM scripts/packages:binpkgs

echo "Moving tar files to /dist"
for file in ./bazel-bin/scripts/packages/*.tar.gz; do
  filename=$(basename $file)
  cp $file $OUTPUT_DIRECTORY/${filename/.tar/-$HERON_VERSION-$TARGET_PLATFORM.tar}
done

echo "Moving self extracting binaries to /dist"
for file in ./bazel-bin/scripts/packages/*.sh; do
  filename=$(basename $file)
  cp $file $OUTPUT_DIRECTORY/${filename/.sh/-$HERON_VERSION-$TARGET_PLATFORM.sh}
done
