---
title: Heron Topologies
---

A Heron **topology** is a [directed acyclic
graph](https://en.wikipedia.org/wiki/Directed_acyclic_graph) used to process
streams of data. Heron topologies consist of three basic components:
[spouts](#spouts) and [bolts](#bolts), which are connected via
**streams** of [tuples](../../developers/data-model). Below is a visual
illustration of a simple topology:

![Heron topology](/img/topology.png)

Spouts are responsible for emitting [tuples](../../developers/data-model)
into the topology, while bolts are responsible for processing those tuples. In
the diagram above, spout **S1** feeds tuples to bolts **B1** and **B2** for
processing; in turn, bolt **B1** feeds processed tuples to bolts **B3** and
**B4**, while bolt **B2** feeds processed tuples to bolt **B4**.

This is just a simple example; you can use bolts and spouts to form arbitrarily
complex topologies.

## Topology Lifecycle

Once you've set up a [Heron cluster](../../operators/deployment), you
can use Heron's [CLI tool](../../operators/heron-cli) to manage the entire
lifecycle of a topology, which typically goes through the following stages:

1. [submit](../../operators/heron-cli#submitting-a-topology) the topology
   to the cluster. The topology is not yet processing streams but is ready to be
   activated.
2. [activate](../../operators/heron-cli#activating-a-topology) the
   topology. The topology will begin processing streams in accordance with
   the topology architecture that you've created.
3. [restart](../../operators/heron-cli#restarting-a-topology) an
   active topology if, for example, you need update the topology configuration.
4. [deactivate](../../operators/heron-cli#deactivating-a-topology) the
   topology. Once deactivated, the topology will stop processing but
   remain running in the cluster.
5. [kill](../../operators/heron-cli#killing-a-topology) a topology to completely
   remove it from the cluster.  It is no longer known to the Heron cluster and
   can no longer be activated. Once killed, the only way to run that topology is
   to re-submit it.

## Spouts

A Heron **spout** is a source of streams, responsible for emitting
[tuples](../../developers/data-model) into the topology. A spout may, for
example, read data from a Kestrel queue or read tweets from the Twitter API and
emit tuples to one or more bolts.

Information on building spouts can be found in [Building
Spouts](../../developers/java/spouts).

## Bolts

A Heron **bolt** consumes streams of
[tuples](../../developers/data-model) emitted by spouts and performs some
set of user-defined processing operations on those tuples, which may include
performing complex stream transformations, performing storage operations,
aggregating multiple streams into one, emitting tuples to other bolts within the
topology, and much more.

Information on building bolts can be found in [Building
Bolts](../../developers/java/bolts).

## Data Model

Heron has a fundamentally tuple-driven data model. You can find more information
in [Heron's Data Model](../../developers/data-model).

## Logical Plan

A topology's **logical plan** is analagous to a database [query
plan](https://en.wikipedia.org/wiki/Query_plan). The image at the top of this
page is an example logical plan for a topology.

## Physical Plan

A topology's **physical plan** is related to its logical plan but with the
crucial difference that a physical plan maps the actual execution logic of a
topology, including the machines running each spout or bolt and more. Here's a
rough visual representation of a physical plan:

![Topology Physical Plan](/img/physicalplan.png)
