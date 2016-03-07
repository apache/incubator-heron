#!/bin/bash

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

install_ssh_keys() {
  # own key
  cp .vagrant/`hostname`_key.pub /home/vagrant/.ssh/id_rsa.pub
  cp .vagrant/`hostname`_key /home/vagrant/.ssh/id_rsa
  chown vagrant:vagrant /home/vagrant/.ssh/id_rsa*

  # other hosts keys
  cat .vagrant/*_key.pub >> /home/vagrant/.ssh/authorized_keys
}

install_mesos() {
    mode=$1 # master | slave
    apt-get -qy install mesos=0.25.0*

    echo "zk://master:2181/mesos" > /etc/mesos/zk
    echo '10mins' > /etc/mesos-slave/executor_registration_timeout
    if [ $mode == "master" ]; then
        echo 'cpus:1;mem:2048;ports:[5000-32000]' > /etc/mesos-slave/resources
    else
        echo 'cpus:2;mem:2048;ports:[5000-32000]' > /etc/mesos-slave/resources
    fi

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

install_kafka-mesos() {
    # download CLI & kafka 08/09
    pushd /home/vagrant
    wget -q "https://github.com/mesos/kafka/releases/download/0.9.4.0/kafka-mesos_0.9.4.0.tar.gz"
    tar -xf kafka-mesos*gz
    rm kafka-mesos*gz

    mv -T kafka-mesos* kafka-08
    cp -r kafka-08 kafka-09
    sed -i s/7000/7001/g kafka-09/kafka-mesos.properties

    # No Kafka 8 topology for now, not launching 08 Kafka-Mesos
    # wget -q "http://www.eu.apache.org/dist/kafka/0.8.2.2/kafka_2.10-0.8.2.2.tgz" -P kafka-08
    wget -q "http://www.eu.apache.org/dist/kafka/0.9.0.0/kafka_2.10-0.9.0.0.tgz" -P kafka-09
    popd

    # run APP
    # curl -X POST -H "Content-Type: application/json" --data @kafka-08.json http://master:8080/v2/apps
    curl -X POST -H "Content-Type: application/json" --data @kafka-09.json http://master:8080/v2/apps
}

install_aurora_coordinator() {
    mkdir -p /home/vagrant/aurora
    pushd /home/vagrant/aurora
    # Installing scheduler
    wget -c https://apache.bintray.com/aurora/aurora-scheduler_0.10.0-1_amd64.deb
    dpkg -i aurora-scheduler_0.10.0-1_amd64.deb
    stop aurora-scheduler
    sudo -u aurora mkdir -p /var/lib/aurora/scheduler/db
    sudo -u aurora mesos-log initialize --path=/var/lib/aurora/scheduler/db
    start aurora-scheduler
    # Installing client
    wget https://apache.bintray.com/aurora/aurora-tools_0.10.0-1_amd64.deb
    dpkg -i aurora-tools_0.10.0-1_amd64.deb
    popd
}

install_aurora_worker() {
    mkdir -p /home/vagrant/aurora
    pushd /home/vagrant/aurora
    wget -c https://apache.bintray.com/aurora/aurora-executor_0.10.0-1_amd64.deb
    dpkg -i aurora-executor_0.10.0-1_amd64.deb
    popd
}

setup_heron_zk_nodes() {
    mkdir -p /home/vagrant/solr
    pushd /home/vagrant/solr
        # Not the fastest way to do this, need to figure out how to put zk-setup.cpp to use for this
        wget 'http://www.eu.apache.org/dist/lucene/solr/5.4.1/solr-5.4.1.tgz'
        tar -zxf solr-5.4.1.tgz
        ./solr-5.4.1/server/scripts/cloud-scripts/zkcli.sh -zkhost master:2181 -cmd makepath /storm/heron/cluster/pplans
        ./solr-5.4.1/server/scripts/cloud-scripts/zkcli.sh -zkhost master:2181 -cmd makepath /storm/heron/cluster/executionstate
        ./solr-5.4.1/server/scripts/cloud-scripts/zkcli.sh -zkhost master:2181 -cmd makepath /storm/heron/cluster/tmasters
        ./solr-5.4.1/server/scripts/cloud-scripts/zkcli.sh -zkhost master:2181 -cmd makepath /storm/heron/cluster/topologies
    popd
}

copy_scripts() {
    # Copying all the scripts to the home directory for simpler launching through 'vagrant ssh master -c'.
    cp *.sh /home/vagrant
    cp *.json /home/vagrant
}

print_usage() {
    echo "Usage: $0 master|slave mesos|aurora"
}

if [[ $1 != "master" && $1 != "slave" ]]; then
    print_usage
    exit 1
fi
mode=$1

if [[ $2 != "mesos" && $2 != "aurora" ]]; then
    print_usage
    exit 1
fi
scheduler=$2

cd /vagrant/contrib/kafka9/vagrant

chmod +x *.sh

# name resolution
cp .vagrant/hosts /etc/hosts

install_ssh_keys

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

add-apt-repository ppa:openjdk-r/ppa -y

apt-get -qy update

# install deps
apt-get install -qy vim zip mc curl wget openjdk-8-jdk scala git libcurl4-nss-dev libunwind8

update-alternatives --set java /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java

install_mesos $mode
if [ $mode == "master" ]; then
    install_marathon
    install_kafka-mesos
    if [ $scheduler == "aurora" ]; then
        install_aurora_coordinator
    fi
    if [ $scheduler == "mesos" ]; then
        ./submit-mesos-scheduler.sh
    fi
    ./setup-dist-dir.sh
    setup_heron_zk_nodes
    copy_scripts
fi
if [ $scheduler == "aurora" ]; then
    install_aurora_worker
fi
