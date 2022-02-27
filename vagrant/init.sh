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

bazelVersion=4.2.2
bazel_install() {
    apt-get install -y automake cmake gcc g++ zlib1g-dev zip pkg-config wget libssl-dev libunwind-dev
    mkdir -p /opt/bazel
    pushd /opt/bazel
        wget -O /tmp/bazel.sh https://github.com/bazelbuild/bazel/releases/download/${bazelVersion}/bazel-${bazelVersion}-installer-linux-x86_64.sh
        chmod +x /tmp/bazel.sh
        /tmp/bazel.sh
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

# disable ipv6
echo -e "\nnet.ipv6.conf.all.disable_ipv6 = 1\n" >> /etc/sysctl.conf
sysctl -p

# install docker repo
apt-get install -qy ca-certificates curl gnupg lsb-release
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null

# update installed packages
apt-get -qy update

# install deps
apt-get install -qy ant vim zip mc curl wget openjdk-11-jdk scala git python3-setuptools python3-venv python3-dev libtool-bin python-is-python3

# install docker 
apt-get install -qy docker-ce docker-ce-cli containerd.io
usermod -aG docker vagrant

if [ $mode == "master" ]; then 
    bazel_install
fi
