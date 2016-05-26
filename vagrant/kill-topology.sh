#!/usr/bin/env bash

DIST_DIR=/vagrant/dist/ubuntu
HERON_CONF_PATH=/vagrant/contrib/kafka9/vagrant/conf

if [[ $# -ne 1 ]] ; then
    echo 'USAGE: ./kill-topology.sh <topology_name>'
    exit 1
fi

pushd ${DIST_DIR}
    ./heron-0.1.0-SNAPSHOT/bin/heron-cli2 kill "example/vagrant/devel" $1 --config-loader com.twitter.heron.scheduler.aurora.AuroraConfigLoader --config-path ${HERON_CONF_PATH}
popd