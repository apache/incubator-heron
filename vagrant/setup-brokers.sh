#!/usr/bin/env bash

if [[ $# -ne 2 ]]; then
    echo "USAGE: $0 <number_of_kafka_8_brokers> <number_of_kafka_9_brokers>"
    exit 1
fi

pushd /home/vagrant/kafka-08
for (( i=0; i<$1; i++ ))
do
   ./kafka-mesos.sh broker add $i --cpus=0.2 --mem=512 --heap=256
   ./kafka-mesos.sh broker start $i
done
popd

pushd /home/vagrant/kafka-09
for (( i=0; i<$2; i++ ))
do
   ./kafka-mesos.sh broker add $i --cpus=0.2 --mem=512 --heap=256
   ./kafka-mesos.sh broker start $i
done
popd
