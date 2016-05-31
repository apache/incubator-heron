# Vagrant image for running Heron examples

## Overview

With the help of this image one should be able to test their topologies on Heron.

## Prerequisites

There are two requirements for a host machine. Please refer to official docs for installation instructions:

- [Vagrant](http://vagrantup.com) - required for running cluster VMs

## Contents

- 1x Mesos Master node
- Configurable number of Mesos Slaves/Agents (via `SLAVES` parameter in `Vagrantfile`)
- Zookeeper running at `master:2181`
- Marathon running at `master:8080`
- Aurora scheduler (plain Mesos cluster supported by changing `SCHEDULER` parameter in `Vagrantfile` to `mesos`) running on master
- Aurora executor running on master and all slaves
- Kafka-Mesos scheduler running at `master:7000`
- 1x Kafka Broker running on top of Kafka-Mesos framework, either 0.8 or 0.9 (Kafka version is configurable via `KAFKA` parameter in `Vagrantfile`) running at `master:9092`

## Running the cluster

In order to spin the cluster up, please run

```
vagrant up
```

### Adding more Kafka brokers

By default, a single Kafka broker will be running. In case you need more than one broker, just SSH on any node, cd to `/home/vagrant/kafka-mesos` and add additional brokers via [Kafka-Mesos CLI](https://github.com/mesos/kafka#navigating-the-cli).

For more details regarding managing Kafka brokers, please refer to [Kafka-Mesos readme](https://github.com/mesos/kafka).