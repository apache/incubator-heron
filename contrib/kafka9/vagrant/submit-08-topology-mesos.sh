#!/usr/bin/env bash

DIST_DIR=/vagrant/dist
HERON_CONF_PATH=/vagrant/contrib/kafka9/vagrant/conf/mesos_scheduler.conf
DEFN_TMP_DIR=${DIST_DIR}/ubuntu/defn-tmp

if [[ $# -ne 4 ]] ; then
    echo 'USAGE: ./submit-09-topology-mesos.sh <topology_name> <bootstrap_broker> <source_topic> <target_topic>'
    exit 1
fi

pushd ${DIST_DIR}/ubuntu
    ./heron-0.1.0-SNAPSHOT/bin/heron-cli2 submit "" ${DIST_DIR}/topologies/kafka-08-mirror_deploy.jar com.twitter.heron.KafkaOldMirrorTopology $1 $2 $3 $4 --config-loader com.twitter.heron.scheduler.util.DefaultConfigLoader --config-path ${HERON_CONF_PATH} --tmp-dir ${DEFN_TMP_DIR}
popd