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
    yum install -y epel-release || die "cannot install epel"
    yum install -y \
        which make rpmdevtools gcc-c++ cmake boost-devel libxml2-devel libuuid-devel krb5-devel libgsasl-devel \
        protobuf-devel || die "cannot install dependencies"
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
	make rpm-package || die "failed to create debian package"
	popd
}

deploy() {
    pushd ${top_dir}

    version=$(cat ${top_dir}/rpms/BUILD/version)

    if [ -z "${version}" ]; then
        die "cannot get version"
    fi
    
    if [ -z "${BINTRAY_KEY}" ]; then
        die "bintray api key not set"
    fi
    
    message=`curl -H "X-Bintray-Publish: 1" -H "X-Bintray-Override: 1" -T ${top_dir}/rpms/RPMS/x86_64/libhdfs3-${version}-1.el7.centos.x86_64.rpm -uwangzw:${BINTRAY_KEY} \
      https://api.bintray.com/content/wangzw/rpm/libhdfs3/${version}/centos7/x86_64/libhdfs3-${version}-1.el7.centos.x86_64.rpm`
    
    if [ -z `echo ${message} | grep "success"` ]; then
        echo ${message}
        die "failed to upload libhdfs3-${version}-1.el7.centos.x86_64.rpm"
    fi
    
    message=`curl -H "X-Bintray-Publish: 1" -H "X-Bintray-Override: 1" -T ${top_dir}/rpms/RPMS/x86_64/libhdfs3-devel-${version}-1.el7.centos.x86_64.rpm -uwangzw:${BINTRAY_KEY} \
      https://api.bintray.com/content/wangzw/rpm/libhdfs3/${version}/centos7/x86_64/libhdfs3-devel-${version}-1.el7.centos.x86_64.rpm`
    
    if [ -z `echo ${message} | grep "success"` ]; then
        echo ${message}
        die "failed to upload libhdfs3-devel-${version}-1.el7.centos.x86_64.rpm"
    fi

    popd
}

run() {
    install_depends || die "failed to install dependencies"
    build_with_boost || die "build failed with boost"
    build_with_debug || die "build failed with debug mode"
    create_package || die "failed to create debian package"
    
    version=$(cat ${top_dir}/rpms/BUILD/version)
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
