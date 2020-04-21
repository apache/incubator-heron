#!/bin/bash -ex

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

install_mesos() {
    mode=$1 # master | slave
    apt-get -qy install mesos=0.25.0*

    echo "zk://master:2181/mesos" > /etc/mesos/zk
    echo '5mins' > /etc/mesos-slave/executor_registration_timeout

    ip=$(cat /etc/hosts | grep `hostname` | grep -E -o "([0-9]{1,3}[\.]){3}[0-9]{1,3}")
    echo $ip > "/etc/mesos-$mode/ip"

    if [ $mode == "master" ]; then
        ln -s /lib/init/upstart-job /etc/init.d/mesos-master
        service mesos-master start
    else
        apt-get -qy remove zookeeper
    fi

    ln -s /lib/init/upstart-job /etc/init.d/mesos-slave
    service mesos-slave start
}

install_marathon() {
    apt-get install -qy marathon=0.10.0*
    service marathon start
}

install_docker() {
    apt-get install -qy lxc-docker
    echo 'docker,mesos' > /etc/mesos-slave/containerizers
    service mesos-slave restart
}

install_jdk8() {
    apt-get install -y software-properties-common python-software-properties
    add-apt-repository -y ppa:webupd8team/java
    apt-get -y update
    /bin/echo debconf shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections
    apt-get -y install oracle-java8-installer oracle-java8-set-default vim wget screen git    
}

bazelVersion=3.0.0
bazel_install() {
    install_jdk8
    apt-get install -y g++ automake cmake gcc-4.8 g++-4.8 zlib1g-dev zip pkg-config wget libssl-dev
    mkdir -p /opt/bazel
    pushd /opt/bazel
        pushd /tmp
            wget http://download.savannah.gnu.org/releases/libunwind/libunwind-1.1.tar.gz
            tar xvfz libunwind-1.1.tar.gz
            cd libunwind-1.1 && ./configure --prefix=/usr && make install 
        popd
        wget -O /tmp/bazel.sh https://github.com/bazelbuild/bazel/releases/download/${bazelVersion}/bazel-${bazelVersion}-installer-linux-x86_64.sh
        chmod +x /tmp/bazel.sh
        /tmp/bazel.sh --user 
    popd
}

build_heron() {
    pushd /tmp
        wget http://ftpmirror.gnu.org/libtool/libtool-2.4.6.tar.gz
        tar xf libtool*
        cd libtool-2.4.6
        sh configure --prefix /usr/local
        make install 
    popd
    pushd /vagrant
        export CC=gcc-4.8
        export CXX=g++-4.8
        export PATH=/sbin:$PATH
        ~/bin/bazel clean
        ./bazel_configure.py
        ~/bin/bazel --bazelrc=tools/travis/bazel.rc build --config=ubuntu heron/...
    popd
}

if [[ $1 != "master" && $1 != "slave" ]]; then
    echo "Usage: $0 master|slave"
    exit 1
fi
mode=$1

cd /vagrant/vagrant

# name resolution
cp .vagrant/hosts /etc/hosts

# ssh key
key=".vagrant/ssh_key.pub"
if [ -f $key ]; then
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

# add mesosphere repo
apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv E56151BF
DISTRO=$(lsb_release -is | tr '[:upper:]' '[:lower:]')
CODENAME=$(lsb_release -cs)
echo "deb http://repos.mesosphere.io/${DISTRO} ${CODENAME} main" | tee /etc/apt/sources.list.d/mesosphere.list

# add docker repo
apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 36A1D7869245C8950F966E92D8576A8BA88D21E9
echo "deb http://get.docker.com/ubuntu docker main" > /etc/apt/sources.list.d/docker.list

apt-get -qy update

# install deps
apt-get install -qy vim zip mc curl wget openjdk-7-jre scala git python-setuptools python-dev

install_mesos $mode
if [ $mode == "master" ]; then 
    install_marathon
    bazel_install
    build_heron
fi

install_docker

