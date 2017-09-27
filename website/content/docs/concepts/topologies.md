---
title: Heron Topologies
---

> ## New DSL for Heron
> As of Heron version 0.15.2, there is a new **Heron DSL** that you can use
> to write topologies in a more declarative, functional manner, without
> needing to specify spout and bolt logic directly. There is currently a [DSL for
> Java](../../developers/java/dsl). A DSL for Python will be added soon.
> 
> More information on the DSL can be found [below](#the-heron-dsl).

A Heron **topology** is a [directed acyclic
graph](https://en.wikipedia.org/wiki/Directed_acyclic_graph) (DAG) used to process
streams of data. Topologies can be stateless or 
[stateful](../delivery-semantics#stateful-topologies) depending on your use case.

Heron topologies consist of two basic components:

* [Spouts](#spouts) inject data into Heron topologies, potentially from external sources like pub-sub messaging systems (Apache Kafka, Apache Pulsar, etc.)
* [Bolts](#bolts) apply user-defined processing logic to data supplied by spouts

Spouts and bolts are connected to one another via **streams** of data. Below is a
visual illustration of a simple Heron topology:

![Heron topology](/img/topology.png)

In the diagram above, spout **S1** feeds data to bolts **B1** and **B2** for
processing; in turn, bolt **B1** feeds processed data to bolts **B3** and
**B4**, while bolt **B2** feeds processed data to bolt **B4**. This is just a
simple example; you can create arbitrarily complex topologies in Heron.

## Creating topologies

There are currently two APIs available that you can use to build Heron topologies:

1. The higher-level [Heron DSL](#the-heron-dsl) (recommended for new topologies), which enables you to create topologies in a declarative, developer-friendly style inspired by functional programming concepts (such as map, flatMap, and filter operations)
1. The lower-level [topology API](#the-topology-api) (*not* recommended for new topologies), based on the original [Apache Storm](http://storm.apache.org/about/simple-api.html) API, which requires you to specify spout and bolt logic directly

## Topology Lifecycle

Once you've set up a [Heron cluster](../../operators/deployment), you
can use Heron's [CLI tool](../../operators/heron-cli) to manage the entire
lifecycle of a topology, which typically goes through the following stages:

1. [Submit](../../operators/heron-cli#submitting-a-topology) the topology
   to the cluster. The topology is not yet processing streams but is ready to be
   activated.
2. [Activate](../../operators/heron-cli#activating-a-topology) the
   topology. The topology will begin processing streams in accordance with
   the topology architecture that you've created.
3. [Restart](../../operators/heron-cli#restarting-a-topology) an
   active topology if, for example, you need to update the topology configuration.
4. [Deactivate](../../operators/heron-cli#deactivating-a-topology) the
   topology. Once deactivated, the topology will stop processing but
   remain running in the cluster.
5. [Kill](../../operators/heron-cli#killing-a-topology) a topology to completely
   remove it from the cluster.  It is no longer known to the Heron cluster and
   can no longer be activated. Once killed, the only way to run that topology is
   to re-submit it.

## Logical Plan

A topology's **logical plan** is analagous to a database [query
plan](https://en.wikipedia.org/wiki/Query_plan) in that it maps out the basic
operations associated with a topology.

![Topology logical Plan](https://www.lucidchart.com/publicSegments/view/4e6e1ede-45f1-471f-b131-b3ecb7b7c3b5/image.png)

Whether you use the [Heron DSL](#the-heron-dsl) or the [topology
API](#the-topology-api), Heron automatically  transforms the processing logic that
you create into a physical network of [spouts](#spouts) and [bolts](#bolts) that
run inside of containers.

## Physical Plan

A topology's **physical plan** is related to its logical plan but with the
crucial difference that a physical plan maps the "physical" execution logic of a
topology. Here's a
rough visual representation of a physical plan:

![Topology Physical Plan](https://www.lucidchart.com/publicSegments/view/5c2fe0cb-e4cf-4192-9416-b1b64b5ce958/image.png)

Whether you use the [Heron DSL](#the-heron-dsl) or the [topology
API](#the-topology-api), Heron automatically  transforms the processing logic that
you create into a physical network of [spouts](#spouts) and [bolts](#bolts) that
run inside of containers.

## The Heron DSL

When Heron was first created, the model for creating topologies was deeply
indebted to the Apache Storm model. Under that model, developers creating topologies
needed to explicitly define the behavior of every spout and bolt in the topology.
Although this provided a powerful low-level API for creating topologies, the chief
drawbacks was that topologies

### Why a DSL?

The older, spouts-and-bolts based model of creating topologies provided a powerful API
for specifying processing logic, but it presented a number of challenges for developers as well:

* **Verbosity** --- In both the Java and Python topology APIs, creating spouts and bolts involved substantial boilerplate, requiring developers to both provide implementations for spout and bolt classes and also specify the connections between those spouts and bolts. This often led to the problem of...
* **Difficult debugging** --- When spouts, bolts, and the connections between them need to be created "by hand," a great deal of cognitive load
* **Tuple-based data model** --- In the older topology API, spouts and bolts passed tuples and nothing but tuples within topologies. Although tuples are a powerful and flexible data type, the topology API forced *all* spouts and bolts to serialize or deserialize tuples.

In contrast with the topology API, the Heron DSL offers:

* **Boilerplate-free code** --- Instead of re to implement spout and bolt classes, the Heron DSL enables you to write functions, such as map, flatMap, join, and filter functions, instead.
* **Easy debugging** --- With the Heron DSL, you don't have to worry about spouts and bolts, which means that you can more easily surface problems with your processing logic.
* **Completely flexible data model** --- Instead of requiring that all processing components pass tuples to one another (which implicitly requires serialization to and deserializaton from your application-specific types), the Heron DSL enables you to write your processing logic in accordance with whatever types you'd like---including tuples, if you wish.

From the standpoint of both operators and developers [managing topologies'
lifecycles](#topology-lifecycle), the resulting topologies are equivalent. From a
development workflow standpoint, however, the difference is profound.

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

### DSL example

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

    private int randomInt(int lower, int upper) {
        return ThreadLocalRandom.current().nextInt(lower, upper + 1);
    }

    public static void main(String[] args) {
        Builder builder = Builder.CreateBuilder();

        builder.newSource(() -> 0)
                .setName("zeroes");

        builder.newSource(() -> randomInt(1, 10))
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

That Java code will produce this [physical plan](#physical-plan):

![Heron DSL physical plan](https://www.lucidchart.com/publicSegments/view/4e6e1ede-45f1-471f-b131-b3ecb7b7c3b5/image.png)

### Key-value streamlets

In order to perform some operations, such as streamlet joins and streamlet reduce operations,

### Heron DSL topologies

With the Heron DSL *you still create topologies*, but only implicitly. Heron automatically
performs the heavy lifting of converting the streamlet-based processing logic that you
create into spouts and bolts and, from there, into containers that are then deployed using
whichever scheduler your Heron cluster is using.

### Available DSLs

The Heron DSL is currently available in the following languages:

* [Java](../../developers/java/dsl)

## Partitioning

In the topology API, processing parallelism can be managed via adjusting the number of spouts and bolts performing different operations, enabling you to, for example, increase the relative parallelism of a bolt by using three of that bolt instead of two.

The Heron DSL provides a different mechanism for controlling parallelism: **partitioning**. To understand partitioning, keep in mind that rather than physical spouts and bolts, the core processing construct in the Heron DSL is the processing step. With the Heron DSL, you can assign a number of partitions to each processing step in your graph (the default is one partition).

The example topology [above](#streamlets), for example, has five steps: the random integer source, the "add one" map operation, the union operation, the filtering operation, and finally the logging operation. You could apply varying numbers of partitions each step in that topology like this:

```java
Builder builder = Builder.CreateBuilder();

builder.newSource(() -> 0)
        .setName("zeroes");

builder.newSource(() -> ThreadLocalRandom.current().nextInt(1, 11))
        .setName("random-ints")
        .setNumPartitions(3)
        .map(i -> i + 1)
        .setName("add-one")
        .setNumPartitions(3)
        .union(zeroes)
        .setName("unify-streams")
        .setNumPartitions(2)
        .filter(i -> i != 2)
        .setName("remove-all-twos")
        .setNumPartitions(2)
        .log();
```

With the older topology API, you had two "levers" for managing topology performance: 

1. Adjusting the number of spouts and bolts performing operations
1. Adjusting the resources (CPU and RAM) used by the topology

The Heron DSL still enables you to adjust the CPU and RAM used by the topology but replaces #1 with per-processing-step partitioning.

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

## The topology API

### Data Model

Heron has a fundamentally tuple-driven data model. You can find more information
in [Heron's Data Model](../../developers/data-model).
