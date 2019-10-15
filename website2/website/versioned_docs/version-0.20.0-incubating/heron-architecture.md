---
id: version-0.20.0-incubating-heron-architecture
title: Heron Architecture
sidebar_label: Heron Architecture
original_id: heron-architecture
---
<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

Heron is a general-purpose stream processing engine designed for speedy performance,
low latency, isolation, reliability, and ease of use for developers and administrators
alike. Heron was [open
sourced](https://blog.twitter.com/engineering/en_us/topics/open-source/2016/open-sourcing-twitter-heron.html)
by [Twitter](https://twitter.github.io/).

> We recommend reading [Heron's Design Goals](heron-design-goals) and [Heron Topologies](../topologies) in conjunction with this guide.

The sections below:

* clarify the distinction between Heron and [Apache Storm](#relationship-with-apache-storm)
* describe Heron's basic [system architecture](#basic-system-architecture)
* explain the role of major [components](#topology-components) of Heron's architecture
* provide an overview of what happens when [submit a topology](#topology-submission)

## Topologies

You can think of a Heron cluster as a mechanism for managing the lifecycle of
stream-processing entities called **topologies**. Topologies can be written in
Java or Python.


More information can be found
in the [Heron Topologies](heron-topology-concepts) document.

## Relationship with Apache Storm

[Apache Storm](https://storm.apache.org) is a stream processing system originally
open sourced by Twitter in 2011. Heron, also developed at Twitter, was created
to overcome many of the shortcomings that Storm exhibited when run in production
at Twitter scale.

Shortcoming | Solution
:-----------|:--------
Resource isolation | Heron uses process-based isolation both between topologies and between containers within topologies, which is more reliable and easier to monitor and debug than Storm's model, which involves shared communication threads in the same [JVM](https://en.wikipedia.org/wiki/Java_virtual_machine)
Resource efficiency | Storm requires [scheduler](#schedulers) resources to be provisioned up front, which can lead to over-provisioning. Heron avoids this problem by using cluster resources on demand.
Throughput | For a variety of architectural reasons, Heron has consistently been shown to provide much higher throughput and much lower latency than Storm

### Storm compatibility

Heron was built to be fully backwards compatible with Storm and thus to enable
[topology](heron-topology-concepts) developers to use Heron to run topologies created using
Storm's [topology API](http://storm.apache.org/about/simple-api.html).

Currently, Heron is compatible with topologies written using:

1. The new [Heron Streamlet API](topology-development-streamlet-api) 
1. The [Heron Topology API](topology-development-topology-api-java)

If you have existing topologies created using the [Storm API](http://storm.apache.org/about/simple-api.html),
you can make them Heron compatible by following [these simple instructions](../../migrate-storm-to-heron)

Heron was initially developed at Twitter with a few main goals in mind:

1. Providing blazing-fast performance, reliability, and easy troubleshooting by leveraging a process-based computing model and full topology isolation.
2. Retaining full compatibility with Storm's data model and [topology API](http://storm.apache.org/about/simple-api.html).

For a more in-depth discussion of Heron and Storm, see the [Twitter Heron:
Stream Processing at Scale](http://dl.acm.org/citation.cfm?id=2742788) paper.

Heron thus enables you to achieve major gains along a variety of axes---throughput,
latency, reliability---without needing to sacrifice engineering resources.

## Heron Design Goals

For a description of the core goals of Heron as well as the principles that have
guided its development, see [Heron Design Goals](heron-design-goals).

## Basic system architecture


# Topology Components

From an architectural standpoint, Heron was built as an interconnected set of modular
components. 


The following core components of Heron topologies are discussed in depth in
the sections below:

* [Topology Master](#topology-master)
* [Containers](#containers)
* [Stream Manager](#stream-manager)
* [Heron Instance](#heron-instance)
* [Metrics Manager](#metrics-manager)
* [Heron Tracker](#heron-tracker)

### Topology Master

The **Topology Master** \(TM) manages a topology throughout its entire lifecycle,
from the time it's submitted until it's ultimately killed. When `heron` deploys
a topology it starts a single TM and multiple [containers](heron-architecture#container).
The **TM** creates an ephemeral [ZooKeeper](http://zookeeper.apache.org) node to
ensure that there's only one **TM** for the topology and that the **TM** is easily
discoverable by any process in the topology. The **TM** also constructs the [physical
plan](heron-topology-concepts#physical-plan) for a topology which it relays to different
components.

![Topology Master](assets/tmaster.png)

#### Topology Master Configuration

TMs have a variety of [configurable
parameters](cluster-config-tmaster) that you can adjust at each
phase of a topology's [lifecycle](heron-topology-concepts#topology-lifecycle).

### Containers

Each Heron topology consists of multiple **containers**, each of which houses
multiple [Heron Instances](#heron-instance), a [Stream
Manager](#stream-manager), and a [Metrics Manager](#metrics-manager). Containers
communicate with the topology's **TM** to ensure that the topology forms a fully
connected graph.

For an illustration, see the figure in the [Topology Master](#topology-master)
section above.

> In Heron, all topology containerization is handled by the scheduler, be it [Mesos](schedulers-meso-local-mac), [Kubernetes](schedulers-k8s-with-helm), [YARN](schedulers-k8s-by-hand), or something else. Heron schedulers typically use [cgroups](https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/6/html/resource_management_guide/ch01) to manage Heron topology processes.

### Stream Manager

The **Stream Manager** (SM) manages the routing of tuples between topology
components. Each [Heron Instance]({{< ref "#heron-instance" >}}) in a topology connects to its
local **SM**, while all of the **SMs** in a given topology connect to one another to
form a network. Below is a visual illustration of a network of **SMs**:

![Heron Data Flow](assets/data-flow.png)

In addition to being a routing engine for data streams, **SMs** are responsible for
propagating [back pressure](https://en.wikipedia.org/wiki/Back_pressure)
within the topology when necessary. Below is an illustration of back pressure:

![Back Pressure 1](assets/backpressure1.png)

In the diagram above, assume that bolt **B3** (in container **A**) receives all
of its inputs from spout **S1**. **B3** is running more slowly than other
components. In response, the SM for container **A** will refuse input from the
SMs in containers **C** and **D**, which will lead to the socket buffers in
those containers filling up, which could lead to throughput collapse.

In a situation like this, Heron's back pressure mechanism will kick in. The **SM**
in container **A** will send a message to all the other **SMs**, then all
**SMs** will cut off inputs from local spouts and no new data will be accepted
into the topology.

![Back Pressure 2](assets/backpressure2.png)

Once the lagging bolt (**B3**) begins functioning normally, the **SM** in container
**A** will notify the other **SMs** and stream routing within the topology will
return to normal.

#### Stream Manager Configuration

**SMs** have a variety of [configurable
parameters](state-managers-local-fs) that you can adjust at each
phase of a topology's [lifecycle](heron-topology-concepts#topology-lifecycle).

### Heron Instance

A **Heron Instance** (HI) is a process that handles a single task of a
[spout](../topologies#spouts) or [bolt](heron-topology-concepts##bolts), which allows
for easy debugging and profiling.

Currently, Heron only supports Java, so all
**HIs** are [JVM](https://en.wikipedia.org/wiki/Java_virtual_machine) processes, but
this will change in the future.

#### Heron Instance Configuration

**HIs** have a variety of [configurable
parameters](cluster-config-instance) that you can adjust at
each phase of a topology's [lifecycle](heron-topology-concepts##topology-lifecycle).

### Metrics Manager

Each topology runs a **Metrics Manager** (MM) that collects and exports metrics from
all components in a [container]({{< ref "#container" >}}). It then routes those metrics to
both the [Topology Master]({{< ref "#topology-master" >}}) and to external collectors, such as
[Scribe](https://github.com/facebookarchive/scribe),
[Graphite](http://graphite.wikidot.com/), or analogous systems.

You can adapt Heron to support additional systems by implementing your own
[custom metrics sink](extending-heron-metric-sink).

# Cluster-level Components

All of the components listed in the sections above can be found in each
topology. The components listed below are cluster-level components that function
outside of particular topologies.

### Heron CLI

Heron has a **CLI** tool called `heron` that is used to manage topologies.
Documentation can be found in [Managing
Topologies](user-manuals-heron-cli).

### Heron API server

The [Heron API server](deployment-api-server) handles all requests from
the [Heron CLI tool](#heron-cli), uploads topology artifacts to the designated storage
system, and interacts with the scheduler.

> When running Heron [locally](getting-started-local-single-node), you won't need to deploy
> or configure the Heron API server.

### Heron Tracker

The **Heron Tracker** (or just Tracker) is a centralized gateway for
cluster-wide information about topologies, including which topologies are
running, being launched, being killed, etc. It relies on the same
[ZooKeeper](http://zookeeper.apache.org) nodes as the topologies in the cluster
and exposes that information through a JSON REST API. The Tracker can be
run within your Heron cluster (on the same set of machines managed by your
Heron [scheduler](schedulers-local)) or outside of it.

Instructions on running the tracker including JSON API docs can be found in [Heron
Tracker](user-manuals-heron-tracker-runbook).

### Heron UI

**Heron UI** is a rich visual interface that you can use to interact with
topologies. Through **Heron UI** you can see color-coded visual representations of
the [logical](heron-topology-concepts#logical-plan) and
[physical](heron-topology-concepts#physical-plan) plan of each topology in your cluster.

For more information, see the [Heron UI](user-manuals-heron-ui) document.

<!--
## Topology Submit Sequence

[Topology Lifecycle](../topologies#topology-lifecycle) describes the lifecycle states of a Heron
topology. The diagram below illustrates the sequence of interactions amongst the Heron architectural
components during the `submit` and `deactivate` client actions. Additionally, the system interaction
while viewing a topology on the Heron UI is shown.

The source for this diagram lives here:
https://docs.google.com/drawings/d/10d1Q_VO0HFtOHftDV7kK6VbZMVI5EpEYHrD-LR7SczE

<img src="assets/topology-submit-sequence-diagram.png" alt="Topology Sequence Diagram"/>
-->

## Topology submission

The diagram below illustrates what happens when you submit a Heron topology:

{{< diagram
    width="80"
    url="https://www.lucidchart.com/publicSegments/view/766a2ee5-7a07-4eff-9fde-dd79d6cc355e/image.png" >}}

Component | Description
:---------|:-----------
Client | When a topology is submitted using the [`heron submit`](user-manuals-heron-cli#submitting-a-topology) command of the [Heron CLI tool](user-manuals-heron-cli), it first executes the `main` function of the topology and creates a `.defn` file containing the topology's [logical plan](heron-topology-concepts#logical-plan). Then, it runs [`org.apache.heron.scheduler.SubmitterMain`](/api/java/org/apache/heron/scheduler/SubmitterMain.html), which is responsible for uploading the topology artifact to the [Heron API server](deployment-api-server).
Heron API server | When the [Heron API server](deployment-api-server) has been notified that a topology is being submitted, it does two things. First, it uploads the topology artifacts (a JAR for Java or a PEX for Python, plus a few other files) to a storage service; Heron supports multiple uploaders for a variety of storage systems, such as [Amazon S3](uploaders-amazon-s3), [HDFS](uploaders-hdfs), and the [local filesystem](uploaders-local-fs).
Heron scheduler | When the Heron CLI (client) submits a topology to the Heron API server, the API server notifies the Heron scheduler and also provides the scheduler with the topology's [logical plan](heron-topology-concepts#logical-plan), [physical plan](heron-topology-concepts#physical-plan), and some other artifacts. The scheduler, be it [Mesos](schedulers-mesos-local-mac), [Aurora](schedulers-aurora-cluster), the [local filesystem](schedulers-local), or something else, then deploys the topology using containers.
Storage | When the topology is deployed to containers by the scheduler, the code running in those containers then downloads the remaining necessary topology artifacts (essentially the code that will run in those containers) from the storage system.

* Shared Services

    When the main scheduler (`org.apache.heron.scheduler.SchedulerMain`) is invoked
    by the launcher, it fetches the submitted topology artifact from the
    topology storage, initializes the **State Manager**, and prepares a physical plan that
    specifies how multiple instances should be packed into containers. Then, it starts
    the specified scheduler, such as `org.apache.heron.scheduler.local.LocalScheduler`,
    which invokes the `heron-executor` for each container.

* Topologies

    `heron-executor` process is started for each container and is responsible for
    executing the **Topology Master** or **Heron Instances** (Bolt/Spout) that are
    assigned to the container. Note that the **Topology Master** is always executed
    on container 0. When `heron-executor` executes normal **Heron Instances**
    (i.e. except for container 0), it first prepares
    the **Stream Manager** and the **Metrics Manager** before starting
    `org.apache.heron.instance.HeronInstance` for each instance that is
    assigned to the container.
    
    **Heron Instance** has two threads: the gateway thread and the slave thread.
    The gateway thread is mainly responsible for communicating with the **Stream Manager**
    and the **Metrics Manager** using `StreamManagerClient` and `MetricsManagerClient`
    respectively, as well as sending/receiving tuples to/from the slave
    thread. On the other hand, the slave thread runs either Spout or Bolt
    of the topology based on the physical plan.
    
    When a new **Heron Instance** is started, its `StreamManagerClient` establishes
    a connection and registers itself with the **Stream Manager**.
    After the successful registration, the gateway thread sends its physical plan to
    the slave thread, which then executes the assigned instance accordingly.
    

## Codebase

Heron is primarily written in **Java**, **C++**, and **Python**.

A detailed guide to the Heron codebase can be found
[here](compiling-code-organization).
