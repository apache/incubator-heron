#!/usr/bin/env bash

DIST_DIR=/vagrant/dist
HERON_CONF_PATH=/vagrant/contrib/kafka9/vagrant/conf/mesos_scheduler.conf

if [[ $# -lt 3 ]] ; then
    echo 'USAGE: ./submit-custom-topology-mesos.sh <jar_file_name> <main_class_name> <topology_name> <args>'
    exit 1
fi

pushd ${DIST_DIR}/ubuntu
    ./heron-0.1.0-SNAPSHOT/bin/heron-cli2 submit "heron.topology.pkg.uri: file:///vagrant/dist/packages/example/devel/vagrant/$1/topology.tar.gz" ${DIST_DIR}/topologies/$1 $2 $3 ${@:4} --config-loader com.twitter.heron.scheduler.util.DefaultConfigLoader --config-path ${HERON_CONF_PATH}
popd

