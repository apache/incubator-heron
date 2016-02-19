#!/usr/bin/env bash

if [[ $# -eq 0 ]] ; then
    echo 'USAGE: ./submit-topology.sh <topology_name>'
    exit 1
fi

./contrib/kafka9/vagrant/heron-ubuntu/heron-0.1.0-SNAPSHOT/bin/heron-cli2 submit "example/vagrant/devel" /vagrant/contrib/kafka9/vagrant/kafka-mirror_deploy.jar com.twitter.heron.KafkaMirrorTopology $1 master:5000 foo foo_mirrored --config-loader com.twitter.heron.scheduler.aurora.AuroraConfigLoader --config-path /vagrant/contrib/kafka9/vagrant/conf --tmp-dir /vagrant/contrib/kafka9/vagrant/heron-ubuntu