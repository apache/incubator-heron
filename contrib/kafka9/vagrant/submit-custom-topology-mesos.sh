#!/usr/bin/env bash

DIST_DIR=/vagrant/dist
HERON_CONF_PATH=/vagrant/contrib/kafka9/vagrant/conf/mesos_scheduler.conf
DEFN_TMP_DIR=${DIST_DIR}/ubuntu/defn-tmp

if [[ $# -lt 3 ]] ; then
    echo 'USAGE: ./submit-custom-topology-mesos.sh <jar_file_name> <main_class_name> <topology_name> <args>'
    exit 1
fi

pushd ${DIST_DIR}/ubuntu
    ./heron-0.1.0-SNAPSHOT/bin/heron-cli2 submit "" ${DIST_DIR}/topologies/$1 $2 $3 ${@:4} --config-loader com.twitter.heron.scheduler.util.DefaultConfigLoader --config-path ${HERON_CONF_PATH} --tmp-dir ${DEFN_TMP_DIR}
popd

