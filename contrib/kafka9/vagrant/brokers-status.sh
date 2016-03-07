#!/usr/bin/env bash

pushd /home/vagrant/kafka-08
    echo "Kafka 0.8 cluster:"
    ./kafka-mesos.sh broker list
popd

pushd /home/vagrant/kafka-09
    echo "Kafka 0.9 cluster:"
    ./kafka-mesos.sh broker list
popd