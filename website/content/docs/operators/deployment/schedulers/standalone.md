---
title: Heron standalone
---

Heron enables you to easily run a multi-node cluster in **standalone mode**. The difference between standalone mode and [local mode](../local) for Heron is that standalone mode involves running multiple compute nodes---using [Hashicorp](https://www.hashicorp.com/)'s [Nomad](https://www.nomadproject.io/) as a scheduler---rather than just one.

## Installation

You can use Heron in standalone mode using the normal [Heron CLI](../../../heron-cli) tool, which can be installed using the instructions [here](../../../../getting-started).

## Configuration

Once you have the Heron CLI tool installed, you need to provide a list of hosts for:

* Heron master nodes
* Heron slave nodes
* ZooKeeper nodes

You can easily do this by running the following commands:

```bash
$ heron standalone set masters
$ heron standalone set slaves
$ heron standalone set zookeepers
```

Each of these commands will open up a configuration file that you can edit in Vim. To edit the file, type **a** and add each host to the existing list, separated by line. Here's an example:

```bash
10.0.0.1
10.0.0.2
10.0.0.3
```

Once you've added the list of hosts for masters, slaves, and ZooKeeper nodes, you can move on to [starting the cluster](#starting-and-stopping-the-cluster).

> To run Heron in standalone mode locally on your laptop, set the host to `127.0.0.1` for both the masters and slaves and set the ZooKeeper host to `127.0.0.1:2181`. These are all defaults that you won't need to modify.

## Starting and stopping the cluster

To start Heron in standalone mode once the configuration for hosts has been applied:

```bash
$ heron standalone cluster start
```

You should see output like this:

```bash
[2018-01-21 13:35:12 -0800] [INFO]: Roles:
[2018-01-21 13:35:12 -0800] [INFO]:  - Master Servers: ['127.0.0.1']
[2018-01-21 13:35:12 -0800] [INFO]:  - Slave Servers: ['127.0.0.1']
[2018-01-21 13:35:12 -0800] [INFO]:  - Zookeeper Servers: ['127.0.0.1:2181']
[2018-01-21 13:35:12 -0800] [INFO]: Starting master on 127.0.0.1
[2018-01-21 13:35:12 -0800] [INFO]: Done starting masters
[2018-01-21 13:35:12 -0800] [INFO]: Starting slave on 127.0.0.1
[2018-01-21 13:35:12 -0800] [INFO]: Starting slave on 127.0.0.1
[2018-01-21 13:35:12 -0800] [INFO]: Done starting slaves
[2018-01-21 13:35:12 -0800] [INFO]: Starting Heron API Server on 127.0.0.1
[2018-01-21 13:35:12 -0800] [INFO]: Done starting Heron API Server
[2018-01-21 13:35:12 -0800] [INFO]: Heron standalone cluster complete!
```

If you see the `Heron standalone cluster complete!` message, that means that the cluster is ready for you to [submit](#submitting-a-topology) and manage topologies.

<!--
## Deploying from a template

```bash
$ heron standalone template configs \
  ~/.heron/conf/standalone
```
-->

## Submitting a topology

```bash
$ heron 
```

## Managing topologies

```bash
$ heron-tracker
$ heron-ui
```