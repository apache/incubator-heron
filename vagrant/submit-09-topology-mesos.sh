#!/usr/bin/env bash

DIST_DIR=/vagrant/dist
HERON_CONF_PATH=/vagrant/contrib/kafka9/vagrant/conf

if [[ $# -ne 4 ]] ; then
    echo 'USAGE: ./submit-09-topology-mesos.sh <topology_name> <bootstrap_broker> <source_topic> <target_topic>'
    exit 1
fi

pushd ${DIST_DIR}/ubuntu
    JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/ ./heron-cli/bin/heron submit mesos/vagrant ${DIST_DIR}/topologies/kafka-09-mirror_deploy.jar com.twitter.heron.KafkaMirrorTopology $1 $2 $3 $4 --config-path ${HERON_CONF_PATH} --heron_home /vagrant/dist/heron-cli
popd