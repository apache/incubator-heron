#!/bin/bash -e
# Copyright 2015 The Bazel Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if ! which thrift >/dev/null; then
   echo "thrift not found,please install the corresponding package." >&2
   echo "***Build and Install Guide***" >&2
   echo "sudo apt-get install automake libssl-dev byacc bison flex libevent-dev -y"
   echo "git clone https://github.com/apache/thrift.git"
   echo "cd thrift && git checkout 0.9.3"
   echo "./bootstrap.sh"
   echo "./configure --with-boost=/bin --libdir=/usr/lib --without-java --without-python"
   echo "make && sudo make insall"
   exit 1
fi

DIR=`dirname $0`
rm -rf ${DIR}/gen-javabean
rm -rf ${DIR}/../java/org/apache/scribe/*
thrift --gen java:beans,hashcode,nocamel,generated_annotations=undated ${DIR}/scribe.thrift
cp -r ${DIR}/gen-javabean/*  ${DIR}/../java/
rm -rf ${DIR}/gen-javabean
