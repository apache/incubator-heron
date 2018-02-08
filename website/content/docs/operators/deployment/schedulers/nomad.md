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

Before installing Heron via Nomad, you'll need to configure some settings. Once you've [installed Heron](../../../../getting-started), all of the configurations you'll need to modify will be in the `~/.heron/conf/nomad` diredctory.

First, you'll need to use a topology uploader to deploy topology packages to nodes in your cluster. You can use one of the following uploaders:

* The HTTP uploader in conjunction with Heron's [API server](../../../heron-api-server). The Heron API server acts like a file server to which users can upload topology packages. The API server distributes the packages, along with the Heron core package, to the relevant machines. You can also use the API server to submit your Heron topology to Nomad (described [below](#deploying-with-the-api-server)) <!-- TODO: link to upcoming HTTP uploader documentation -->
* [Amazon S3](../../uploaders/s3). Please note that the S3 uploader requires an AWS account.
* [SCP](../../uploaders/scp). Please note that the SCP uploader requires SSH access to nodes in the cluster.

You can modify the `heron.class.uploader` parameter in `~/.heron/conf/nomad/uploader.yaml` to choose an uploader. The following uploaders are available:

{{< list "uploaders" >}}

