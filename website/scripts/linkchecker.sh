#!/bin/bash

HERON_ROOT_DIR=$(git rev-parse --show-toplevel)

source $HERON_ROOT_DIR/scripts/detect_os_type.sh

PLATFORM=`platform`

if [ $PLATFORM = "darwin" ]; then
    HTMLTEST_PLATFORM=osx
elif [ $PLATFORM = "ubuntu" ] || [ $PLATFORM = "centos" ]; then
    HTMLTEST_PLATFORM=linux
fi

echo $HTMLTEST_PLATFORM

EXECUTABLE=htmltest-$HTMLTEST_PLATFORM

tmp/$EXECUTABLE
