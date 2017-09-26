---
title: Heron Topologies
---

> ## New DSL for Heron
> As of Heron version 0.15.2, there is a new **Heron DSL** that can be used
> to write topologies in a more functional manner, without needing to
> specify spout and bolt logic directly. There is currently a [DSL for
> Python](../../developers/python/python-dsl) and a DSL for Java will be
> added soon.
> 
> More information on the DSL can be found [below](#the-heron-dsl).

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

## The Heron DSL

When Heron was first created, the model for creating topologies was deeply
indebted to the Apache Storm model. Under that model, developers creating topologies
needed to explicitly define the behavior of every spout and bolt in the topology.
Although this provided a powerful low-level API for creating topologies, the chief
drawbacks was that topologies

The Heron DSL provides a higher-level API that is

From the standpoint of both operators and developers [managing topologies'
lifecycles](#topology-lifecycle), the resulting topologies are equivalent. From a
development workflow standpoint, however, the difference is profound. The Heron
DSL allows for topology code that is:

* Less verbose and far less dependent on boilerplate
* 

### Streamlets

The core construct underlying the Heron DSL is that of the **streamlet**. A streamlet is
a potentially unbounded, ordered collection of tuples. Streamlets can originate from a
wide variety of sources, such as pub-sub messaging systems like [Apache
Kafka](http://kafka.apache.org/) and [Apache Pulsar](https://pulsar.incubator.apache.org)
(incubating), random generators, or static files like CVS or Parquet files.

In the Heron DSL, processing tuples means *transforming streamlets into other
streamlets*. This can be done using a wide variety of available operations, including
many you may be familiar with from functional programming:

Operation | Description
:---------|:-----------
map | Returns a new streamlet by applying the supplied mapping function to each element in the original streamlet
flatMap | Like a map operation but with the important difference that each element of the streamlet is flattened
join | Joins two separate streamlets into a single streamlet
filter | Returns a new streamlet containing only the elements that satisfy the supplied filtering function
sample | Returns a new streamlet containing only a fraction of elements. That fraction is defined by the supplied function.

You can see an example streamlet-based processing graph in the diagram below:

![Streamlet-based processing graph for Heron](https://www.lucidchart.com/publicSegments/view/dc74f0b2-0d3d-46da-b80d-0bc70ad4f64c/image.png)

Here's the corresponding Java code for the processing logic shown in the diagram:

```java
package heron.dsl.example;

import com.twitter.heron.dsl.*;
import com.twitter.heron.dsl.impl.BaseStreamlet;

import java.util.concurrent.ThreadLocalRandom;

public final class ExampleDSLTopology {
    public ExampleDSLTopology() {}

    public static void main(String[] args) {
        Streamlet<Integer> randoms = BaseStreamlet.createSupplierStreamlet(() -> ThreadLocalRandom.current().nextInt(1, 11));
        Streamlet<Integer> zeroes = BaseStreamlet.createSupplierStreamlet(() -> 0);

        Builder builder = Builder.CreateBuilder();

        builder.newSource(zeroes)
                .setName("zeroes");

        builder.newSource(randoms)
                .setName("random-ints")
                .map(i -> i + 1)
                .setName("add-one")
                .union(zeroes)
                .setName("unify-streams")
                .filter(i -> i != 2)
                .setName("remove-all-twos")
                .log();

        Config conf = new Config();
        conf.setNumContainers(2);

        new Runner().run("ExampleDSLTopology", conf, builder);
    }
}
```

This Java code will produce this [physical plan]():

![Heron DSL physical plan](/img/dsl-physical-plan.png)

### Key-value streamlets

In order to perform some operations, such as streamlet joins and streamlet reduce operations,

### Heron DSL topologies

With the Heron DSL *you still create topologies*, but only implicitly. Heron automatically
performs the heavy lifting of converting the streamlet-based processing logic that you
create into spouts and bolts and, from there, into containers that are then deployed using
whichever scheduler your Heron cluster is using.

### Why a DSL?

The older, spouts-and-bolts based model of creating topologies provides 

### Available DSLs

The Heron DSL is currently available in the following languages:

* [Python](../../developers/python/python-dsl)

## Partitioning

## Windowing

Window configurations have two core components:

1. The length of the window

> In the Heron DSL, all window lengths and sliding intervals are specified **in seconds**.

### Sliding windows

In a sliding time window, tuples

![Sliding time window](https://www.lucidchart.com/publicSegments/view/57d2fcbb-591b-4403-9258-e5b8e1e25de2/image.png)

### Tumbling windows

> One of the crucial differences between sliding and tumbling windows is that sliding
> windows can overlap.

![Tumbling time window](https://www.lucidchart.com/publicSegments/view/881f99ee-8f93-448f-a178-b9f72dce6491/image.png)

### Count windows

There are thus four total window types:

## Resource allocation with the Heron DSL

Three configs:

1. Number of containers
1. CPU
1. RAM

The defaults:

Resource | Default
:--------|:-------
Number of containers | 1
CPU | 1.0
RAM | 512 MB

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
