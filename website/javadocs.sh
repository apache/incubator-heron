#!/usr/bin/env bash

set -e

script_dir=`dirname $0`
root_dir=$script_dir/..
output_dir=$script_dir/public/api

SRC_FILES=`find $root_dir -path "*/com/twitter/*" -name "*.java"`

rm -r $output_dir
mkdir -p $output_dir
javadoc -quiet -d $output_dir $SRC_FILES

echo Javdocs generated at $output_dir
