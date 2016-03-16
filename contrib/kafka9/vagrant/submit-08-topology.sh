#!/usr/bin/env bash

VAGRANT_DIR=/vagrant/contrib/kafka9/vagrant
DIST_DIR=${VAGRANT_DIR}/heron-ubuntu
HERON_CONF_DIR=${VAGRANT_DIR}/conf
DEFN_TMP_DIR=${DIST_DIR}/defn-tmp

if [[ $# -ne 4 ]] ; then
    echo 'USAGE: ./submit-09-topology.sh <topology_name> <bootstrap_broker> <source_topic> <target_topic>'
    exit 1
fi

pushd ${DIST_DIR}
    ./heron-0.1.0-SNAPSHOT/bin/heron-cli2 submit "example/vagrant/devel" ${DIST_DIR}/topologies/kafka-08-mirror_deploy.jar com.twitter.heron.KafkaOldMirrorTopology $1 $2 $3 $4 --config-loader com.twitter.heron.scheduler.aurora.AuroraConfigLoader --config-path ${HERON_CONF_DIR} --tmp-dir ${DEFN_TMP_DIR}
popd