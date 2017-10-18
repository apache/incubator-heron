---
title: Intro to Heron cluster configuration
---

{{< alert "heron-config" >}}

Heron clusters can be configured on two different levels:

Level | Description
:-----|:----------
**System** | System-level configurations apply to the whole Heron cluster rather than to any specific component (for example logging configurations)
**Component** | Component-level configurations enable you to establish default configurations for specific components of a Heron cluster, such as the [scheduler](../../concepts/architecture), the [Heron API server](../heron-api-server), etc.

> Neither system- nor component-level configurations can be overridden by topology developers in topology code. Heron clusters instead set component-level configurations for things like [Heron Instances](../../concepts/architecture#heron-instance) that applies to the system as a whole, whereas topology developers have the option to override those defaults if desired.

## Configuration system

All system- and component-level configs are declared in a series of [YAML](http://www.yaml.io/) files, each of which is responsible for configuring different elements of a Heron cluster.

Config file | What it configures
:-----------|:------------------
[`client.yaml`](client) | Interactions between the [Heron CLI client](../heron-cli) and the cluster
[`heron_internals.yaml`](heron-internals) | [Heron Instances](../../concepts/architecture#heron-instance), the [Stream Manager](../../concepts/architecture#stream-manager), logging, the [Metrics Manager](../../concepts/architecture#metrics-manager), and the [Topology Master](../../concepts/architecture#topology-master)
[`metrics_sinks.yaml`](metrics-sinks) | The metrics sink (or sinks) used by Heron
[`packing.yaml`](packing) | The packing algorithm used for topologies in the cluster
[`scheduler.yaml`](scheduler) | The [scheduler](../../concepts/architecture#schedulers) used by Heron to run topologies on physical machines
[`statemgr.yaml`](state-manager) | The [state manager](../../concepts/architecture#state-manager) used by Heron when running [stateful topologies](../../concepts/delivery-semantics#stateful-topologies)
[`uploader.yaml`](uploader) | The [uploader](../../concepts/architecture#uploaders) used by Heron to store artifacts (such as topology code stored as tarballs)

Each of these YAML files, complete with defaults, can be found in a scheduler-specific [base template](#base-templates). When deploying a Heron cluster, you can leave the defaults in place

> The language of "defaults" here is a little tricky.

## Base templates

Each Heron cluster relies on a [scheduler](../../concepts/architecture#schedulers).

Each scheduler supported by Heron, such as [Kubernetes](../deployment/schedulers/kubernetes), [Mesos](../deployment/schedulers/mesos), or the [local filesystem](../deployment/schedulers/local), has a **base template** configuration that provides default configurations for that scheduler.

## The System Level

There are a small handful of system-level configs for Heron. These are detailed
in [System-level Configuration](../system).

## The Component Level

There is a wide variety of component-level configurations that you can establish
as defaults in your Heron cluster. These configurations tend to apply to
specific components in a topology and are detailed in the docs below:

* [Heron Instance](../instance)
* [Heron Metrics Manager](../metrics-manager)
* [Heron Stream Manager](../stmgr)
* [Heron Topology Master](../tmaster)

### Overriding Heron Cluster Configuration

The Heron configuration applies globally to a cluster. 
It is discouraged to modify the configuration to suit one topology.
It is not possible to override the Heron configuration
for a topology via Heron client or other Heron tools.

More on Heron's CLI tool can be found in [Managing Heron
Topologies](../../heron-cli).
