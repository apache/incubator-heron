#!/usr/bin/env bash

DIST_DIR=/vagrant/dist/ubuntu
HERON_CONF_PATH=/vagrant/contrib/kafka9/vagrant/conf

if [[ $# -ne 1 ]] ; then
    echo 'USAGE: ./kill-topology-mesos.sh <topology_name>'
    exit 1
fi

pushd ${DIST_DIR}
    JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/ ./heron-cli/bin/heron kill mesos/vagrant $1 --config-path ${HERON_CONF_PATH} --heron_home /vagrant/dist/heron-cli
popd