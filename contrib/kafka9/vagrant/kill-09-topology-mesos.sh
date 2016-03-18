#!/usr/bin/env bash

DIST_DIR=/vagrant/dist/ubuntu
HERON_CONF_PATH=/vagrant/contrib/kafka9/vagrant/conf/mesos_scheduler.conf

if [[ $# -ne 1 ]] ; then
    echo 'USAGE: ./kill-09-topology-mesos.sh <topology_name>'
    exit 1
fi

pushd ${DIST_DIR}
    ./heron-0.1.0-SNAPSHOT/bin/heron-cli2 kill "" $1 --config-loader com.twitter.heron.scheduler.util.DefaultConfigLoader --config-path ${HERON_CONF_PATH}
popd