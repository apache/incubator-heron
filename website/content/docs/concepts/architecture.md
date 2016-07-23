---
title: Heron Architecture
---

Heron is the direct successor of [Apache Storm](http://storm.apache.org). From
an architectural perspective it is markedly different from Storm but fully
backwards compatible with it from an API perspective.

The sections below clarify the distinction between [Heron and
Storm]({{< ref "#relationship-with-apache-storm" >}}), describe the [design
goals]({{< ref "#heron-design-goals" >}}) behind Heron, and explain major
[components]({{< ref "#topology-components" >}}) of its architecture.

## Codebase

A detailed guide to the Heron codebase can be found
[here]({{< relref "codebase.md" >}}).

## Topologies

You can think of a Heron cluster as a mechanism for managing the lifecycle of
stream-processing entities called **topologies**. More information can be found
in the [Heron Topologies](../topologies) document.

## Relationship with Apache Storm

Heron is the direct successor of [Apache Storm](http://storm.apache.org) but
built with two goals in mind:

1. Overcoming Storm's performance, reliability, and other shortcomings by
replacing Storm's thread-based computing model with a process-based model.
2. Retaining full compatibility with Storm's data model and [topology
API](http://storm.apache.org/about/simple-api.html).

For a more in-depth discussion of Heron and Storm, see the [Twitter Heron:
Stream Processing at Scale](http://dl.acm.org/citation.cfm?id=2742788) paper.

## Heron Design Goals

For a description of the principles that Heron was designed to fulfill, see
[Heron Design Goals](/docs/concepts/design-goals).

## Topology Components

The following core components of Heron topologies are discussed in depth in
the sections below:

* [Topology Master]({{< ref "#topology-master" >}})
* [Container]({{< ref "#container" >}})
* [Stream Manager]({{< ref "#stream-manager" >}})
* [Heron Instance]({{< ref "#heron-instance" >}})
* [Metrics Manager]({{< ref "#metrics-manager" >}})
* [Heron Tracker]({{< ref "#heron-tracker" >}})

### Topology Master

The Topology Master \(TM) manages a topology throughout its entire lifecycle,
from the time it's submitted until it's ultimately killed. When `heron` deploys
a topology it starts a single TM and multiple [containers]({{< ref "#container" >}}).
The TM creates an ephemeral [ZooKeeper](http://zookeeper.apache.org) node to
ensure that there's only one TM for the topology and that the TM is easily
discoverable by any process in the topology. The TM also constructs the [physical
plan](../topologies#physical-plan) for a topology which it relays to different
components.

![Topology Master](/img/tmaster.png)

#### Topology Master Configuration

TMs have a variety of [configurable
parameters](../../operators/configuration/tmaster) that you can adjust at each
phase of a topology's [lifecycle](../topologies#topology-lifecycle).

### Container

Each Heron topology consists of multiple **containers**, each of which houses
multiple [Heron Instances]({{< ref "#heron-instance" >}}), a [Stream
Manager]({{< ref "#stream-manager" >}}), and a [Metrics Manager]({{< ref "#metrics-manager" >}}). Containers
communicate with the topology's TM to ensure that the topology forms a fully
connected graph.

For an illustration, see the figure in the [Topology Master]({{< ref "#topology-master" >}})
section above.

### Stream Manager

The **Stream Manager** (SM) manages the routing of tuples between topology
components. Each [Heron Instance]({{< ref "#heron-instance" >}}) in a topology connects to its
local SM, while all of the SMs in a given topology connect to one another to
form a network. Below is a visual illustration of a network of SMs:

![Heron Data Flow](/img/data-flow.png)

In addition to being a routing engine for data streams, SMs are responsible for
propagating [back pressure](https://en.wikipedia.org/wiki/Back_pressure)
within the topology when necessary. Below is an illustration of back pressure:

![Back Pressure 1](/img/backpressure1.png)

In the diagram above, assume that bolt **B3** (in container **A**) receives all
of its inputs from spout **S1**. **B3** is running more slowly than other
components. In response, the SM for container **A** will refuse input from the
SMs in containers **C** and **D**, which will lead to the socket buffers in
those containers filling up, which could lead to throughput collapse.

In a situation like this, Heron's back pressure mechanism will kick in. The SM
in container **A** will send a message to all the other SMs. In response, the
other SMs will examine the container's [physical
plan](../topologies#physical-plan) and cut off inputs from spouts that feed
bolt **B3** (in this case spout **S1**).

![Back Pressure 2](/img/backpressure2.png)

Once the lagging bolt (**B3**) begins functioning normally, the SM in container
**A** will notify the other SMs and stream routing within the topology will
return to normal.

#### Stream Manger Configuration

SMs have a variety of [configurable
parameters](../../operators/configuration/stmgr) that you can adjust at each
phase of a topology's [lifecycle](../topologies#topology-lifecycle).

### Heron Instance

A **Heron Instance** (HI) is a process that handles a single task of a
[spout](../topologies#spouts) or [bolt](../topologies#bolts), which allows
for easy debugging and profiling.

Currently, Heron only supports Java, so all
HIs are [JVM](https://en.wikipedia.org/wiki/Java_virtual_machine) processes, but
this will change in the future.

#### Heron Instance Configuration

HIs have a variety of [configurable
parameters](../../operators/configuration/instance) that you can adjust at
each phase of a topology's [lifecycle](../topologies#topology-lifecycle).

### Metrics Manager

Each topology runs a Metrics Manager (MM) that collects and exports metrics from
all components in a [container]({{< ref "#container" >}}). It then routes those metrics to
both the [Topology Master]({{< ref "#topology-master" >}}) and to external collectors, such as
[Scribe](https://github.com/facebookarchive/scribe),
[Graphite](http://graphite.wikidot.com/), or analogous systems.

You can adapt Heron to support additional systems by implementing your own
[custom metrics sink](../../contributors/custom-metrics-sink).

## Cluster-level Components

All of the components listed in the sections above can be found in each
topology. The components listed below are cluster-level components that function
outside of particular topologies.

### Heron CLI

Heron has a CLI tool called `heron` that is used to manage topologies.
Documentation can be found in [Managing
Topologies](../../operators/heron-cli).

### Heron Tracker

The **Heron Tracker** (or just Tracker) is a centralized gateway for
cluster-wide information about topologies, including which topologies are
running, being launched, being killed, etc. It relies on the same
[ZooKeeper](http://zookeeper.apache.org) nodes as the topologies in the cluster
and exposes that information through a JSON REST API. The Tracker can be
run within your Heron cluster (on the same set of machines managed by your
Heron [scheduler](../../operators/deployment)) or outside of it.

Instructions on running the tracker including JSON API docs can be found in [Heron
Tracker](../../operators/heron-tracker).

### Heron UI

**Heron UI** is a rich visual interface that you can use to interact with
topologies. Through Heron UI you can see color-coded visual representations of
the [logical](../topologies#logical-plan) and
[physical](../topologies#physical-plan) plan of each topology in your cluster.

For more information, see the [Heron UI](../../operators/heron-ui) document.

## Topology Submit Sequence

[Topology Lifecycle](../topologies#topology-lifecycle) describes the lifecycle states of a Heron
topology. The diagram below illustrates the sequence of interactions amongst the Heron architectural
components during the `submit` and `deactivate` client actions. Additionally, the system interaction
while viewing a topology on the Heron UI is shown.

<!--
The source for this diagram lives here:
https://docs.google.com/drawings/d/10d1Q_VO0HFtOHftDV7kK6VbZMVI5EpEYHrD-LR7SczE
-->
<img src="/img/topology-submit-sequence-diagram.png" style="max-width:140%;!important;" alt="Topology Sequence Diagram"/>

### Topology Submit Description

The following describes in more detail how a topology is submitted and
launched using local scheduler.

* Client

    When a topology is submitted using the `heron submit` command, it first executes
    the `main` of the topology and creates a `.defn` file containing the topology's
    logical plan. Then, it runs `com.twitter.heron.scheduler.SubmitterMain`, which
    is responsible for invoking an uploader and a launcher for the topology.
    The uploader uploads the topology package to the given location, while the
    launcher registers the topology's logical plan and executor state with
    the State Manager and invokes the main scheduler.

* Shared Services

    When the main scheduler (`com.twitter.heron.scheduler.SchedulerMain`) is invoked
    by the launcher, it fetches the submitted topology artifact from the
    topology storage, initializes the State Manager, and prepares a physical plan that
    specifies how multiple instances should be packed into containers. Then, it starts
    the specified scheduler, such as `com.twitter.heron.scheduler.local.LocalScheduler`,
    which invokes the `heron-executor` for each container.

* Topologies

    `heron-executor` process is started for each container and is responsible for
    executing the Topology Master or Heron Instances (Bolt/Spout) that are
    assigned to the container. Note that the Topology Master is always executed
    on container 0. When `heron-executor` executes normal Heron Instances
    (i.e. except for container 0), it first prepares
    the Stream Manager and the Metrics Manager before starting
    `com.twitter.heron.instance.HeronInstance` for each instance that is
    assigned to the container.
    
    Heron Instance has two threads: the gateway thread and the slave thread.
    The gateway thread is mainly responsible for communicating with the Stream Manager
    and the Metrics Manager using `StreamManagerClient` and `MetricsManagerClient`
    respectively, as well as sending/receiving tuples to/from the slave
    thread. On the other hand, the slave thread runs either spout or bolt
    of the topology based on the physical plan.
    
    When a new Heron Instance is started, its `StreamManagerClient` establishes
    a connection and registers itself with the stream manager.
    After the successful registration, the gateway thread sends its physical plan to
    the slave thread, which then executes the assigned instance accordingly.
    


