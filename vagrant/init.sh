#!/bin/bash
set -o errexit -o nounset -o pipefail

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# NB: Apache Mesos requires the use of "master"/"slave"
install_mesos() {
    mode=$1 # master | slave
    apt-get -qy install mesos=0.25.0*

    echo "zk://primary:2181/mesos" > /etc/mesos/zk
    echo '5mins' > /etc/mesos-slave/executor_registration_timeout

    ip=$(cat /etc/hosts | grep `hostname` | grep -E -o "([0-9]{1,3}[\.]){3}[0-9]{1,3}")
    echo $ip > "/etc/mesos-$mode/ip"

    if [ "$mode" == "master" ]; then
        ln -s /lib/init/upstart-job /etc/init.d/mesos-master
        service mesos-master start
    else
        apt-get -qy remove zookeeper
    fi

    ln -s /lib/init/upstart-job /etc/init.d/mesos-slave
    echo 'docker,mesos' > /etc/mesos-slave/containerizers
    service mesos-slave start
}

install_marathon() {
    apt-get install -qy marathon=0.10.0*
    service marathon start
}

bazelVersion=3.7.0
bazel_install() {
    apt-get install -y automake cmake gcc g++ zlib1g-dev zip pkg-config wget libssl-dev libunwind-dev
    mkdir -p /opt/bazel
    pushd /opt/bazel
        wget -O /tmp/bazel.sh https://github.com/bazelbuild/bazel/releases/download/${bazelVersion}/bazel-${bazelVersion}-installer-linux-x86_64.sh
        chmod +x /tmp/bazel.sh
        /tmp/bazel.sh
    popd
}

build_heron() {
    pushd /vagrant
        bazel clean
        ./bazel_configure.py
        bazel --bazelrc=tools/travis/bazel.rc build --config=ubuntu heron/...
    popd
}

if [[ "$1" != "master" && $1 != "slave" ]]; then
    echo "Usage: $0 master|slave"
    exit 1
fi
mode="$1"

cd /vagrant/vagrant

# name resolution
cp .vagrant/hosts /etc/hosts

# XXX: not needed?
# ssh key
key=".vagrant/ssh_key.pub"
if [ -f "$key" ]; then
    cat $key >> /home/vagrant/.ssh/authorized_keys
fi

# disable ipv6
echo -e "\nnet.ipv6.conf.all.disable_ipv6 = 1\n" >> /etc/sysctl.conf
sysctl -p

# use apt-proxy if present
if [ -f ".vagrant/apt-proxy" ]; then
    apt_proxy=$(cat ".vagrant/apt-proxy")
    echo "Using apt-proxy: $apt_proxy";
    echo "Acquire::http::Proxy \"$apt_proxy\";" > /etc/apt/apt.conf.d/90-apt-proxy.conf
fi

:<<'REMOVED'
# add mesosphere repo
apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv E56151BF
DISTRO=$(lsb_release -is | tr '[:upper:]' '[:lower:]')
CODENAME=$(lsb_release -cs)
echo "deb http://repos.mesosphere.io/${DISTRO} cosmic main" | tee /etc/apt/sources.list.d/mesosphere.list
REMOVED

apt-get -qy update

# install deps
apt-get install -qy ant vim zip mc curl wget openjdk-11-jdk scala git python3-setuptools python3-venv python3-dev libtool-bin libcppunit-dev python-is-python3

# install_mesos $mode
if [ $mode == "master" ]; then 
    # install_marathon
    bazel_install
    # switch to non-root so bazel cache can be reused when SSHing in
    # su --login vagrant /vagrant/scripts/travis/ci.sh
fi
