#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

this_dir=`cd "\`dirname \"$0\"\`";pwd`
top_dir=${this_dir}/../

die() {
    echo "$@" 1>&2 ; popd 2>/dev/null; exit 1
}

install_depends() {
    apt-get install -qq software-properties-common || die "cannot install software-properties-common"
    add-apt-repository -y ppa:ubuntu-toolchain-r/test || die "cannot add repository"
    apt-get update || die "cannot update repository"
    apt-get install -qq \
        dpkg-dev debhelper g++ cmake libxml2-dev uuid-dev protobuf-compiler \
        libprotobuf-dev libgsasl7-dev libkrb5-dev libboost-all-dev || die "cannot install dependencies"
}

build_with_boost() {
    pushd ${top_dir}
    rm -rf build && mkdir -p build && cd build || die "cannot create build directory"
    ../bootstrap --enable-boost || die "bootstrap failed"
    make -j2 unittest || die "failed to run unit tests"
    popd
}

build_with_debug() {
    pushd ${top_dir}
    rm -rf build && mkdir -p build && cd build || die "cannot create build directory"
    ../bootstrap --enable-debug || die "bootstrap failed"
    make -j2 unittest || die "failed to run unit tests"
    popd
}

create_package() {
	pushd ${top_dir}
    rm -rf build && mkdir -p build && cd build || die "cannot create build directory"
    ../bootstrap || die "bootstrap failed"
	make debian-package || die "failed to create debian package"
	popd
}


deploy() {
    pushd ${top_dir}

    version=$(cat ${top_dir}/obj-x86_64-linux-gnu/version)

    if [ -z "${version}" ]; then
        die "cannot get version"
    fi
    
    if [ -z "${BINTRAY_KEY}" ]; then
        die "bintray api key not set"
    fi

    message=`curl -H "X-Bintray-Publish: 1" -H "X-Bintray-Override: 1" -H "X-Bintray-Debian-Distribution: trusty" -H "X-Bintray-Debian-Component: contrib" -H "X-Bintray-Debian-Architecture: amd64" \
      -T ${top_dir}/../libhdfs3_${version}-1_amd64.deb -uwangzw:${BINTRAY_KEY} \
      https://api.bintray.com/content/wangzw/deb/libhdfs3/${version}/dists/trusty/contrib/binary-amd64/libhdfs3_${version}-1_amd64.deb`
    
    if [ -z `echo ${message} | grep "success"` ]; then
        echo ${message}
        die "failed to upload libhdfs3_${version}-1_amd64.deb"
    fi
    
    message=`curl -H "X-Bintray-Publish: 1" -H "X-Bintray-Override: 1" -H "X-Bintray-Debian-Distribution: trusty" -H "X-Bintray-Debian-Component: contrib" -H "X-Bintray-Debian-Architecture: amd64" \
      -T ${top_dir}/../libhdfs3-dev_${version}-1_amd64.deb -uwangzw:${BINTRAY_KEY} \
      https://api.bintray.com/content/wangzw/deb/libhdfs3/${version}/dists/trusty/contrib/binary-amd64/libhdfs3-dev_${version}-1_amd64.deb`
    
    if [ -z `echo ${message} | grep "success"` ]; then
        echo ${message}
        die "failed to upload libhdfs3-dev_${version}-1_amd64.deb"
    fi

    popd
}

run() {
    install_depends || die "failed to install dependencies"
    build_with_boost || die "build failed with boost"
    build_with_debug || die "build failed with debug mode"
    create_package || die "failed to create debian package"
    
    version=$(cat ${top_dir}/obj-x86_64-linux-gnu/version)
    echo "version ${version}"

    if [ -z "${BRANCH}" ]; then
        echo "skip deploy since environment variable BRANCH is not set"
        return
    fi
    
    if [ "${BRANCH}" = "v${version}" ]; then
        echo "deploy libhdfs3 version ${version}"
        deploy || die "failed to deploy libhdfs3 rpms"
    else
        echo "skip deploy for branch ${BRANCH}"
    fi
}

"$@"
