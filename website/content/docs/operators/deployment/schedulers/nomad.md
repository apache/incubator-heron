---
title: Nomad
---

Heron supports [Hashicorp](https://hashicorp.com)'s [Nomad](https://nomadproject.io) as a scheduler. Nomad may be a good option for smaller deployments that don't require a bulkier scheduler like [Mesos](../mesos) or [Kubernetes](../kubernetes).

## Nomad setup

Setting up a nomad cluster will not be covered here. See the [official Nomad docs](https://www.nomadproject.io/intro/getting-started/install.html) for instructions.

> Heron currently only supports the [raw exec driver](https://www.nomadproject.io/docs/drivers/raw_exec.html) for Nomad.

When setting up your Nomad cluster, the following are required:

* The [Heron CLI tool](../../../heron-cli) must be installed on each machine used to deploy Heron topologies
* Python 2.7, Java 7 or 8, and [curl](https://curl.haxx.se/) must be installed on every machine in the cluster
* A [ZooKeeper cluster](https://zookeeper.apache.org)

## Configuring Heron settings

Before installing Heron via Nomad, you'll need to configure some settings. Once you've [installed Heron](../../../../getting-started), all of the configurations you'll need to modify will be in the `$HOME/.heron/conf/nomad` diredctory.

First, you'll need to use a topology uploader to deploy topology packages to nodes in your cluster. You can use one of the following uploaders:

* The HTTP uploader in conjunction with Heron's [API server](../../../heron-api-server)