---
title: Heron Topologies
---

{{< alert "new-streamlet-api" >}}

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

1. The higher-level [Heron Streamlet API](#the-heron-streamlet-api) (recommended for new topologies), which enables you to create topologies in a declarative, developer-friendly style inspired by functional programming concepts (such as map, flatMap, and filter operations)
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
   to re-submit and re-activate it.

## Logical Plan

A topology's **logical plan** is analagous to a database [query
plan](https://en.wikipedia.org/wiki/Query_plan) in that it maps out the basic
operations associated with a topology. Here's an example logical plan for the
example Streamlet API topology [below](#streamlet-api-example):

![Topology logical Plan](https://www.lucidchart.com/publicSegments/view/4e6e1ede-45f1-471f-b131-b3ecb7b7c3b5/image.png)

Whether you use the [Heron Streamlet API](#the-heron-streamlet-api) or the [topology
API](#the-topology-api), Heron automatically transforms the processing logic that
you create into both a logical plan and a [physical plan](#physical-plan).

## Physical Plan

A topology's **physical plan** is related to its logical plan but with the
crucial difference that a physical plan determines the "physical" execution
logic of a topology, i.e. how topology processes are divided between containers. Here's a
basic visual representation of a
physical plan:

![Topology Physical Plan](https://www.lucidchart.com/publicSegments/view/5c2fe0cb-e4cf-4192-9416-b1b64b5ce958/image.png)

In this example, a Heron topology consists of one [spout](#spouts) and five
different [bolts](#bolts) (each of which has multiple instances) that have automatically 
been distributed between five different containers.


## Window operations

**Windowed computations** gather results from a topology or topology component within a specified finite time frame rather than, say, on a per-tuple basis.

Here are some examples of window operations:

* Counting how many customers have purchased a product during each one-hour period in the last 24 hours.
* Determining which player in an online game has the highest score within the last 1000 computations.

### Sliding windows

**Sliding windows** are windows that overlap, as in this figure:

![Sliding time window](https://www.lucidchart.com/publicSegments/view/57d2fcbb-591b-4403-9258-e5b8e1e25de2/image.png)

For sliding windows, you need to specify two things:

1. The length or duration of the window (length if the window is a [count window](#count-windows), duration if the window is a [time window](#time-windows)).
1. The sliding interval, which determines when the window slides, i.e. at what point during the current window the new window begins.

In the figure above, the duration of the window is 10 seconds, while the sliding interval is 5 seconds. Each new window begins five seconds into the current window.

> With sliding time windows, data can be processed in more than one window. Tuples 3, 4, and 5 above are processed in both window 1 and window 2 while tuples 6, 7, and 8 are processed in both window 2 and window 3.

Setting the duration of a window to 16 seconds and the sliding interval to 12 seconds would produce this window arrangement:

![Sliding time window with altered time interval](https://www.lucidchart.com/publicSegments/view/44bd4835-a692-44e6-a5d8-8e47151e3167/image.png)

Here, the sliding interval determines that a new window is always created 12 seconds into the current window.

### Tumbling windows

**Tumbling windows** are windows that don't overlap, as in this figure:

![Tumbling time window](https://www.lucidchart.com/publicSegments/view/881f99ee-8f93-448f-a178-b9f72dce6491/image.png)

Tumbling windows don't overlap because a new window doesn't begin until the current window has elapsed. For tumbling windows, you only need to specify the length or duration of the window but *no sliding interval*.

> With tumbling windows, data are *never* processed in more than one window because the windows never overlap.

### Count windows

**Count windows** are specified on the basis of the number of operations rather than a time interval. A count window of 100 would mean that a window would elapse after 100 tuples have been processed, *with no relation to clock time*.

With count windows, this scenario (for a count window of 50) would be completely normal:

Window | Tuples processed | Clock time
:------|:-----------------|:----------
1 | 50 | 10 seconds
2 | 50 | 12 seconds
3 | 50 | 1 hour, 12 minutes
4 | 50 | 5 seconds

### Time windows

**Time windows** differ from [count windows](#count-windows) because you need to specify a time duration (in seconds) rather than a number of tuples processed.

With time windows, this scenario (for a time window of 30 seconds) would be completely normal:

Window | Tuples processed | Clock time
:------|:-----------------|:----------
1 | 150 | 30 seconds
2 | 50 | 30 seconds
3 | 0 | 30 seconds
4 | 375 | 30 seconds

### All window types

As explained above, windows differ along two axes: sliding (overlapping) vs. tumbling (non overlapping) and count vs. time. This produces four total types:

1. [Sliding](#sliding-windows) [time](#time-windows) windows
1. [Sliding](#sliding-windows) [count](#count-windows) windows
1. [Tumbling](#tumbling-windows) [time](#time-windows) windows
1. [Tumbling](#tumbling-windows) [count](#count-windows) windows

## Resource allocation with the Heron Streamlet API

When creating topologies using the Streamlet API, there are three types of resources that you can specify:

1. The number of containers into which the topology's [physical plan](#physical-plan) will be split
1. The total number of CPUs allocated to be used by the topology
1. The total amount of RAM allocated to be used by the topology

For each topology, there are defaults for each resource type:

Resource | Default | Minimum
:--------|:--------|:-------
Number of containers | 1 | 1
CPU | 1.0 | 1.0
RAM | 512 MB | 192MB

### Allocating resources to topologies

For instructions on allocating resources to topologies, see the language-specific documentation for:

* [Java](../../developers/java/streamlet-api#containers-and-resources)

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

Heron's original topology API required using a fundamentally tuple-driven data model.
You can find more information in [Heron's Data Model](../../developers/data-model).
