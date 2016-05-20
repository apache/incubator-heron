#!/usr/bin/env bash

# Note: if the directory where generated protobuf files are put changes,
# then the following generated_root_dir needs to change accordingly.
# TODO: turn this back on once all the javadoc errors are fixed
#set -e

script_dir=`dirname $0`

# Heron root directory
heron_root_dir=$script_dir/..
output_dir=$script_dir/public/api
gen_proto_dir=$heron_root_dir/bazel-bin/heron/proto/_javac
SRC_FILES=`find $heron_root_dir -path "*/com/twitter/*" -name "*.java"`
GEN_FILES=`find $gen_proto_dir -name "*.java"`

rm -r $output_dir
mkdir -p $output_dir
javadoc -quiet -d $output_dir $GEN_FILES $SRC_FILES

echo Javdocs generated at $output_dir
exit 0
