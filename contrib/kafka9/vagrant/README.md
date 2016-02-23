# Vagrant image for running Heron-Kafka example

## Overview

With the help of this image one should be able to run an example `KafkaMirror` topology on Heron. Heron will launch 
itself on top of Aurora cluster. 
Other topologies would also run on this image, read further for instructions. 

## Prerequisites

There are two requirements for a host machine. Please refer to official docs for installation instructions:

- [Vagrant](http://vagrantup.com) - required for running cluster VMs
- [Docker](http://docker.com) - required for building Heron dist and example topology

## Build

A dedicated Docker image is used for building the dist. In order to build, please run
  
```
./build-heron.sh
```

The release packages and a sample topology will be packed to `heron-ubuntu` directory. If you have already built Heron 
dist, you may simply copy all the dist files to `heron-ubuntu` sub-dir in this directory.
If you want to build only `KafkaMirror` topology, please set the following before running `build-heron` script:

```
export TOPOLOGY_ONLY="true"
```

## Running the cluster

In order to bring the cluster up, please run

```
vagrant up
```

After the cluster is up an running, the following components are being provisioned:

- Mesos master + N slave(s) (see `Vagrantfile` to adjust N as well as resources provided to VMs)
- Marathon
- Aurora scheduler on master
- Aurora executor on master and all the slaves
- Kafka-Mesos scheduler for Kafka-0.9.x 

## Running topology

In order to run the topology, one should set up one or more Kafka broker and submit the topology using Heron CLI

### Setting up Kafka brokers

By default no kafka brokers are created. Just schedulers are running. You may add the required number of brokers by 
running `setup-brokers.sh` script in the vagrant home dir on a cluster.

```
vagrant ssh master -c "./setup-brokers.sh <number_of_kafka_8_brokers> <number_of_kafka_9_brokers>"
# Please await for the following message to appear for all the brokers you want to launch:
broker started:
  id: 0
  active: true
  state: running
  resources: cpus:0.20, mem:512, heap:256, port:auto
  failover: delay:1m, max-delay:10m
  stickiness: period:10m, hostname:master
  task:
    id: broker-0-d278ca52-1945-4e70-9e31-30bb85563761
    state: running
    endpoint: master:5000
```

In order to launch the topology, it is necessary to specify at least one broker endpoint, so please note the 
endpoint field in the yaml representation of a broker. In case if brokers weren't launched within timeout time, you can 
check the endpoint later like this: 

```
vagrant ssh master -c "./brokers-status.sh"
```

In case if there are any issues, you may investigate further using Kafka-Mesos CLIs which are placed in the dedicated 
directories in vagrant home dir:

```
vagrant ssh master
# For the Kafka 0.8 
cd kafka-08
# For the Kafka 0.9 
cd kafka-09
```

For the details on managing brokers, please refer to https://github.com/mesos/kafka

### Submitting the topology

In order to run the example topology please run:

```
vagrant ssh master -c "./submit-09-topology.sh <topology_name> <bootstrap_broker> <source_topic> <target_topic>" 
```

## Verification

In order to verify correct work of the topology, one may use standard Kafka CLI clients which can be found in vagrant 
home dir:

```
vagrant ssh master

cd kafka-09/bin

# Type some messages into the opened shell here:
./kafka-console-producer.sh --topic foo --broker-list master:5000
 
# You should see your messages when consuming from the target topic here:
./kafka-console-consumer.sh --topic foo_mirrored --bootstrap-server master:5000 --new-consumer --from-beginning
```

## Shutting the topology down

It is still quite unclear on how to gracefully shutdown a Heron topology, which is hopefully to change in the nearest 
future. Although it is always possible to kill an Aurora job, responsible for running the topology like this:

```
vagrant ssh master -c "aurora job killall example/vagrant/devel/<topology_name>"
```

Note, that in this case, if you will want to restart a topology on the same cluster, you will need to use a different 
topology name, as it will be still in a running state, as it is visible to Heron.

## Customization

In case if one wants to update the Heron or topology code, it is possible to use the updated dist without restarting
Vagrant cluster. Just run `build-heron` script as it was referred in `Build` section. If Heron release dist was updated 
during the build, please run:

```
vagrant ssh master -c "./setup-dist-dir"
```

After that, the next topology submits will use the latest build.

## Submitting custom topology

In case if one wants to submit a different topology, simply place the topology fat jar into the 
`heron-ubuntu/topologies` dir. Then it is possible to launch one's custom topology as follows:

```
vagrant ssh master -c "./submit-custom-topology.sh <jar_file_name> <main_class_name> <topology_name> <args>"
```

For the required dependencies and the packing that would work, please check `KafkaMirror` topology `BUILD` file.