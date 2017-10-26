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

## The Heron Streamlet API

{{< alert "streamlet-api-beta" >}}

When Heron was first created, the model for creating topologies was deeply
indebted to the Apache Storm model. Under that model, developers creating topologies
needed to explicitly define the behavior of every spout and bolt in the topology.
Although this provided a powerful low-level API for creating topologies, that approach
presented a variety of drawbacks for developers:

* **Verbosity** --- In both the Java and Python topology APIs, creating spouts and bolts involved substantial boilerplate, requiring developers to both provide implementations for spout and bolt classes and also specify the connections between those spouts and bolts. This often led to the problem of...
* **Difficult debugging** --- When spouts, bolts, and the connections between them need to be created "by hand," a great deal of cognitive load
* **Tuple-based data model** --- In the older topology API, spouts and bolts passed tuples and nothing but tuples within topologies. Although tuples are a powerful and flexible data type, the topology API forced *all* spouts and bolts to serialize or deserialize tuples.

In contrast with the topology API, the Heron Streamlet API offers:

* **Boilerplate-free code** --- Instead of re to implement spout and bolt classes, the Heron Streamlet API enables you to write functions, such as map, flatMap, join, and filter functions, instead.
* **Easy debugging** --- With the Heron Streamlet API, you don't have to worry about spouts and bolts, which means that you can more easily surface problems with your processing logic.
* **Completely flexible, type-safe data model** --- Instead of requiring that all processing components pass tuples to one another (which implicitly requires serialization to and deserializaton from your application-specific types), the Heron Streamlet API enables you to write your processing logic in accordance with whatever types you'd like---including tuples, if you wish. In the Streamlet API for [Java](../../developers/java/streamlet-api), all streamlets are typed (e.g. `Streamlet<MyApplicationType>`), which means that type errors can be caught at compile time rather than runtime.

### Heron Streamlet API topologies

With the Heron Streamlet API *you still create topologies*, but only implicitly. Heron
automatically performs the heavy lifting of converting the streamlet-based processing logic
that you create into spouts and bolts and, from there, into containers that are then deployed using
whichever [scheduler](../../operators/deployment) your Heron cluster relies upon.

From the standpoint of both operators and developers [managing topologies'
lifecycles](#topology-lifecycle), the resulting topologies are equivalent. From a
development workflow standpoint, however, the difference is profound.

## Streamlets

The core construct underlying the Heron Streamlet API is that of the **streamlet**. A streamlet is
a potentially unbounded, ordered collection of some data type. Streamlets can originate from a
wide variety of sources, such as pub-sub messaging systems like [Apache
Kafka](http://kafka.apache.org/) and [Apache Pulsar](https://pulsar.incubator.apache.org)
(incubating), random generators, or static files like CSV or [Apache Parquet](https://parquet.apache.org/) files.

These **source streamlets** can then be manipulated in a wide variety of ways. You can apply
[map](#map-operations), [filter](#filter-operations), [flatMap](#flatmap-operations), and many
other operations to them. With [key-value streamlets](#key-value-streamlets) you can these
same operations as well as [join](#join-operations) and [reduce by key and window](#reduce-by-key-and-window-operations)
operations.

### Streamlet example

A visual representation of a streamlet processing graph is shown in the diagram below:

![Example streamlet transformation](https://www.lucidchart.com/publicSegments/view/5c451e53-46f8-4e36-86f4-9a11ca015c21/image.png)

In this diagram, a **source** is used to construct a **source streamlet**. In this case, an integer streamlet
is produced by a random generator that continuously emits random integers between 1 and 100. From there:

* A filter operation is applied to the source streamlet that filters out all values less than or equal to 30
* A *new streamlet* is produced by the filter operation (with the Heron Streamlet API, you're always transforming streamlets into other streamlets)
* A map operation adds 15 to each item in the streamlet, which produces the final streamlet in our graph. We *could* hypothetically go much further and add as many transformation steps to the graph as we'd like.
* Once the final desired streamlet is created, each item in the streamlet is sent to a sink. Sinks are where items leave the processing graph. 


#### An example streamlet processing graph in Java

```java
import com.twitter.heron.streamlet.Builder;
import com.twitter.heron.streamlet.Config;
import com.twitter.heron.streamlet.Runner;

Builder builder = Builder.createBuilder();

builder.newSource(() -> randomInt(1, 100))
        .filter(i -> i > 30)
        .map(i -> i + 15)
        .log();

Config config = new Config();
config.setNumContainers(2);

String streamletGraphTopologyName = "IntegerProcessingGraph";

new Runner(streamletGraphTopology, config, builder).run();
```

As you can see, the Java code for the example streamlet processing graph requires very little boilerplate.

### Key-value streamlets

In the example [above](#streamlet-example), the source streamlet consisted only of integers (31, 47, 82, etc.). In addition
to single-value streamlets, the Heron Streamlet API also enables you to work with **key-value streamlets**. With key-value
streamlets, every element of the streamlet is a [`KeyValue`](/api/java/com/twitter/heron/streamlet/KeyValue.html) object in
which the key and value can be any type you'd like.

There's a variety of [operations](#key-value-streamlet-operations) that are only available for key-value streamlets.

## Streamlet operations

In the Heron Streamlet API, processing data means *transforming streamlets into other
streamlets*. This can be done using a wide variety of available operations, including
many that you may be familiar with from functional programming:

Operation | Description
:---------|:-----------
[map](#map-operations) | Returns a new streamlet by applying the supplied mapping function to each element in the original streamlet
[flatMap](#flatMap-operations) | Like a map operation but with the important difference that each element of the streamlet is flattened into a collection type
[filter](#filter-operations) | Returns a new streamlet containing only the elements that satisfy the supplied filtering function
[union](#filter-operations) | Unifies two streamlets into one, without [windowing](#windowing) or modifying the elements of the two streamlets
[clone](#clone-operations) | Creates any number of identical copies of a streamlet
[transform](#transform-operations) | TODO
[toSink](#sink-operations) | TODO

### Key-value streamlet operations

There are also some operations that are available only for [key-value streamlets](#key-value-streamlets).

Operation | Description
:---------|:-----------
[reduceByKeyAndWindow](#reduce-by-key-and-window-operations) | Produces a streamlet out of two separate key-value streamlets on a key, within a [time window](#windowing), and in accordance with a reduce function that you apply to all the accumulated values

[join](#join-operations) | Joins two separate key-value streamlets into a single streamlet on a key, within a [time window](#windowing), and in accordance with a join function

### Map operations

Map operations create a new streamlet by applying the supplied mapping function to each element in the original streamlet. Here's a Java example:

```java
import com.twitter.heron.streamlet.Builder;

Builder builder = Builder.createBuilder();

builder.newSource(() -> 1)
    .map(i -> i + 12);
```

In this example, a supplier streamlet emits an indefinite series of 1s. The `map` operation then adds 12 to each incoming element, producing a streamlet of 13s. The effect of this operation is to transform the `Streamlet<Integer>` into a transformed `Streamlet<Integer>`, although map operations can also convert streamlets into streamlets of a different type.

### FlatMap operations

FlatMap operations are like [map operations](#map-operations) but with the important difference that each element of the streamlet is "flattened" into another type. In the Java example below, a supplier streamlet emits the same sentence over and over again; the `flatMap` operation transforms each sentence into a Java `List` of individual words:

```java
builder.newSource(() -> "I have nothing to declare but my genius")
        .flatMap((sentence) -> Arrays.asList(sentence.split("\\s+")));
```

The effect of this operation is to transform the `Streamlet<String>` into a `Streamlet<List<String>>`.

> One of the core differences between map and flatMap operations is that flatMap operations typically transform non-collection types into collection types.

### Filter operations

Filter operations retain elements in a streamlet, while potentially excluding some elements, on the basis of a provided filtering function. Here's a Java example:

```java
builder.newSource(() -> ThreadLocalRandom.current().nextInt(1, 11))
        .filter((i) -> i < 7);
```

In this example, a source streamlet consisting of random integers between 1 and 10 is modified by a filter operation that removes all streamlet elements that are greater than 7.

### Union operations

Union operations combine two streamlets of the same type into a single streamlet without modifying the elements. Here's a Java example:

```java
Streamlet<String> oohs = builder.newSource(() -> "ooh");
Streamlet<String> aahs = builder.newSource(() -> "aah");

Streamlet<String> combined = oohs
        .union(aahs);
```

Here, one streamlet is an endless series of "ooh"s while the other is an endless series of "aah"s. The `union` operation combines them into a single streamlet of alternating "ooh"s and "aah"s.

### Clone operations

Clone operations enable you to create any number of "copies" of a streamlet. Each of the "copy" streamlets contains all the elements of and can be manipulated just like the "original" streamlet.

```java
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

Streamlet<Integer> integers = builder.newSource(() -> ThreadLocalRandom.current().nextInt(100));

List<Streamlet<Integer>> copies = integers.clone(5);
Streamlet<Integer> ints1 = copies.get(0);
Streamlet<Integer> ints2 = copies.get(1);
// and so on
```

In this example, a streamlet of random integers between 1 and 100 is split into 5 identical streams.

### Transform operations

Transform operations are highly flexible operations that are most useful for:

* operations involving state in [stateful topologies](../../concepts/delivery-semantics#stateful-topologies)
* operations that don't neatly fit into the other categories or into a lambda-based logic

Transform operations require you to implement three different methods:

* A `setup` method that enables you to pass a context object to the operation and to specify what happens prior to the `transform` step
* A `transform` operation that performs the desired transformation
* A `cleanup` method that allows you to specify what happens after the `transform` step

The context object available to a transform operation provides access to:

* the current state of the topology
* the topology's configuration
* the name of the stream
* the stream partition
* the current task ID

Here's a Java example of a transform operation in a topology where a stateful record is kept of the number of items processed:

```java
import com.twitter.heron.streamlet.Context;
import com.twitter.heron.streamlet.SerializableTransformer;

import java.util.function.Consumer;

public class CountNumberOfItems implements SerializableTransformer<String, String> {
    private int numberOfItems;

    public void setup(Context context) {
        numberOfItems = (int) context.getState("number-of-items");
        context.getState().put("number-of-items", numberOfItems + 1);
    }

    public void transform(String in, Consumer<String> consumer) {
        String transformedString = // Apply some transformation
        consumer.accept(transformedString);
    }

    public void cleanup() {
        System.out.println(
                String.format("Successfully processed new state: %d", numberOfItems));
    }
}
```

This operation does a few things:

* In the `setup` method, the [`Context`](/api/java/com/twitter/heron/streamlet/Context.html) object is used to access the current state (which has the semantics of a Java `Map`). The current number of items processed is incremented by one and then saved as the new state.
* In the `transform` method, the incoming string is transformed in some way and then "accepted" as the new value.
* In the `cleanup` step, the current count of items processed is logged.

Here's that operation within the context of a streamlet processing graph:

```java
Builder builder = Builder.createBuilder();

builder.newSource(() -> "Some string over and over")
        .transform(new CountNumberOfItems())
        .log();
```

### Sink operations

In processing graphs like the ones you build using the Heron Streamlet API, **sinks** are essentially the terminal points in your graph, where your processing logic comes to an end. A processing graph can end with writing to a database, publishing to a topic in a pub-sub messaging system, and so on. With the Streamlet API, you can implement your own custom sinks. Here's an example:

```java
import com.twitter.heron.streamlet.Context;
import com.twitter.heron.streamlet.Sink;

public class FormattedLogSink implements Sink<T> {
    private String streamletName;

    public void setup(Context context) {
        streamletName = context.getStreamletName();
    }

    public void put(T element) {
        String message = String.format("Streamlet %s has produced an element with a value of: '%s'",
                streamletName,
                element.toString());
        System.out.println(message);
    }

    public void cleanup() {}
}
```

In this example, the sink fetches the name of the enclosing streamlet from the context passed in the `setup` method. The `put` method specifies how the sink handles each element that is received (in this case, a formatted message is logged to stdout). The `cleanup` method enables you to specify what happens after the element has been processed by the sink.

Here is the `FormattedLogSink` at work in an example processing graph:

```java
Builder builder = Builder.createBuilder();

builder.newSource(() -> "Here is a string to be passed to the sink")
        .toSink(new FormattedLogSink());
```

> [Log operations](#log-operations) rely on a log sink that is provided out of the box. You'll need to implement other sinks yourself.

### Reduce by key and window operations

When working with [key-value streamlets](#key-value-streamlets), you can combine two such streamlets together by:

* key
* [time window](#window-operations)
* via a reduce function

```java
Builder builder = Builder.createBuilder();

KVStreamlet<String, String> s1 = builder.newKVSource(() -> new KeyValue<>("foo", "bar"));

s1
        .reduceByKeyAndWindow()
```

### Join operations

Join operations in the Streamlet API take two [key-value streamlets](#key-value-streamlets) and join them together:

* based on the key in the key-value streamlet
* over elements accumulated during a specified [time window](#windowing)
* using a join function that specifies *how* values will be processed

You may already be familiar with `JOIN` operations in SQL databases, like this:

```sql
SELECT username, email
FROM all_users
INNER JOIN banned_users ON all_users.username NOT IN banned_users.username;
```

> If you'd like to unite two streamlets into one *without* applying a window or a join function, you can use a [union](#union-operations) operation, which are available for key-value streamlets as well as normal streamlets.

All join operations are done:

1. Over elements accumulated during a specified [time window](#windowing).
1. In accordance with a function that performs the join.

> Join operations can be performed only on [key-value streamlets](#key-value-streamlets), since joins can be performed only on a key.

#### Inner joins

Inner joins operate over the [Cartesian product](https://en.wikipedia.org/wiki/Cartesian_product) of the left stream and the right stream, i.e. over all the whole set of all ordered pairs between the two streams. Imagine this set of key-value pairs accumulated within a time window:

Left Streamlet | Right Streamlet
:--------------|:---------------
("player1", 4) | ("player1", 10)
("player1", 5) | ("player1", 12)
("player1", 17) | 

An inner join operation would thus apply the join function to every possible pairing of key-values, thus **3 &times; 2 = 6** in total.

If the join function, say, added the values together, then the resulting stream would look like this:

Operation | Joined Streamlet
:---------|:----------------
4 + 10 | ("player1", 14)
4 + 12 | ("player1", 16)
5 + 10 | ("player1", 15)
5 + 12 | ("player1", 17)
17 + 10 | ("player1", 27)
17 + 12 | ("player1", 29)

> Inner joins are in a certain sense the "default" join type in the Heron Streamlet API. If you call the `join` method without specifying a join type, inner join will be used.

##### Java example

```java
playerScoresLeft
        .join(playerScoresRight, WindowConfig.)


scores1
        .join(scores2, WindowConfig.TumblingCountWindow(10), (x, y) -> x + y);
```

#### Outer left joins

Left stream | Right stream
:-----------|:------------
("player1", 4) | ("player1", 10)
("player1", 5) | ("player1", 12)
("player1", 17) | 

> With an outer left join, *all* elements in each Streamlet are guaranteed to be included in the resulting joined Streamlet.

#### Outer right joins

#### Outer joins

### Streamlet API example

You can see an example streamlet-based processing graph in the diagram below:

![Streamlet-based processing graph for Heron](https://www.lucidchart.com/publicSegments/view/dc74f0b2-0d3d-46da-b80d-0bc70ad4f64c/image.png)

Here's the corresponding Java code for the processing logic shown in the diagram:

```java
package heron.streamlet.example;

import com.twitter.heron.streamlet.*;
import com.twitter.heron.streamlet.impl.StreamletImpl;

import java.util.concurrent.ThreadLocalRandom;

public final class ExampleStreamletAPITopology {
    public ExampleStreamletAPITopology() {}

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

        new Runner().run("ExampleStreamletAPITopology", conf, builder);
    }
}
```

That Java code will produce this [logical plan](#logical-plan):

![Heron Streamlet API logical plan](https://www.lucidchart.com/publicSegments/view/4e6e1ede-45f1-471f-b131-b3ecb7b7c3b5/image.png)

### Key-value streamlets

In order to perform some operations, such as streamlet joins and streamlet reduce operations, you'll need to create **key-value** streamlets.

## Partitioning

In the topology API, processing parallelism can be managed via adjusting the number of spouts and bolts performing different operations, enabling you to, for example, increase the relative parallelism of a bolt by using three of that bolt instead of two.

The Heron Streamlet API provides a different mechanism for controlling parallelism: **partitioning**. To understand partitioning, keep in mind that rather than physical spouts and bolts, the core processing construct in the Heron Streamlet API is the processing step. With the Heron Streamlet API, you can explicitly assign a number of partitions to each processing step in your graph (the default is one partition).

The example topology [above](#streamlets), for example, has five steps:

* the random integer source
* the "add one" map operation
* the union operation
* the filtering operation
* the logging operation.

You could apply varying numbers of partitions to each step in that topology like this:

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

The number of partitions to assign to each processing step when using the Streamlet API depends
on a variety of factors.

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
