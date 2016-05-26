#!/usr/bin/env bash

DIST_DIR=/vagrant/dist
HERON_CONF_DIR=$/vagrant/contrib/kafka9/vagrant/conf

if [[ $# -ne 4 ]] ; then
    echo 'USAGE: ./submit-09-topology.sh <topology_name> <bootstrap_broker> <source_topic> <target_topic>'
    exit 1
fi

pushd ${DIST_DIR}/ubuntu
    ./heron-0.1.0-SNAPSHOT/bin/heron-cli2 submit "example/vagrant/devel" ${DIST_DIR}/topologies/kafka-08-mirror_deploy.jar com.twitter.heron.KafkaOldMirrorTopology $1 $2 $3 $4 master:2181 /kafka-08 --config-loader com.twitter.heron.scheduler.aurora.AuroraConfigLoader --config-path ${HERON_CONF_DIR}
popd