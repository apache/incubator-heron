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

bazelVersion=3.0.0

install_docker() {
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
    add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
    apt-get update
    apt-get install -qy docker-ce docker-ce-cli containerd.io
    usermod -aG docker vagrant
}

install_k8s_stack() {
    install_docker
    # install kubectl
    curl -Lo /usr/local/bin/kubectl https://storage.googleapis.com/kubernetes-release/release/v1.18.3/bin/linux/amd64/kubectl
    chmod +x /usr/local/bin/kubectl
    # install kind
    curl -Lo /usr/local/bin/kind "https://kind.sigs.k8s.io/dl/v0.8.1/kind-$(uname)-amd64"
    chmod +x /usr/local/bin/kind
    # helm
    curl --location https://get.helm.sh/helm-v3.2.1-linux-amd64.tar.gz \
        | tar --extract --gzip linux-amd64/helm --to-stdout \
        > /usr/local/bin/helm
    chmod +x /usr/local/bin/helm
}

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

cd /vagrant/vagrant

# name resolution
cp .vagrant/hosts /etc/hosts

# disable ipv6
echo -e "\nnet.ipv6.conf.all.disable_ipv6 = 1\n" >> /etc/sysctl.conf
sysctl -p

# use apt-proxy if present
if [ -f ".vagrant/apt-proxy" ]; then
    apt_proxy=$(cat ".vagrant/apt-proxy")
    echo "Using apt-proxy: $apt_proxy";
    echo "Acquire::http::Proxy \"$apt_proxy\";" > /etc/apt/apt.conf.d/90-apt-proxy.conf
fi

apt-get -qy update

# install deps
apt-get install -qy vim zip mc curl wget openjdk-11-jdk scala git python3-setuptools python3-dev libtool-bin libcppunit-dev python-is-python3 tree

install_k8s_stack
bazel_install

# configure environment variables required
{
    ## the install script with the --user flag will install binaries here
    #echo 'export PATH=$HOME/bin:$PATH' >> ~vagrant/.bashrc
    # the discover_platform helper uses this as `python -mplatform` does not put out expected info in this image
    echo 'export PLATFORM=Ubuntu' >> ~vagrant/.bashrc
    # set JAVA_HOME as plenty of places in heron use it to find java
    echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/' >> ~vagrant/.bashrc
}
