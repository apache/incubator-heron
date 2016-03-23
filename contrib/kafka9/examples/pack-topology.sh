#!/usr/bin/env bash

DIST_DIR=../../../dist
HERON_CONF_PATH=../../contrib/kafka9/examples/conf/pack_topology.conf

if [[ $# -lt 3 ]] ; then
    echo 'USAGE: ./pack-topology.sh <jar_file_name> <main_class_name> <topology_name> <args>'
    exit 1
fi

pushd ${DIST_DIR}/centos
    ./heron-0.1.0-SNAPSHOT/bin/heron-cli2 submit "" topologies/$1 $2 $3 ${@:4} --config-loader com.twitter.heron.scheduler.util.DefaultConfigLoader --config-path ${HERON_CONF_PATH}
popd