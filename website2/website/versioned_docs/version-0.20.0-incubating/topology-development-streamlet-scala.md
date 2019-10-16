---
id: version-0.20.0-incubating-topology-development-streamlet-scala
title: The Heron Streamlet API for Scala
sidebar_label: The Heron Streamlet API for Scala
original_id: topology-development-streamlet-scala
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

## Getting started

In order to use the Heron Streamlet API for Scala, you'll need to install the `heron-api` library.

### Maven setup

In order to use the `heron-api` library, add this to the `dependencies` block of your `pom.xml` configuration file:

```xml
<dependency>
    <groupId>org.apache.heron</groupId>
    <artifactId>heron-api</artifactId>
    <version>{{< heronVersion >}}</version>
</dependency>
```

#### Compiling a JAR with dependencies

In order to run a Scala topology created using the Heron Streamlet API in a Heron cluster, you'll need to package your topology as a "fat" JAR with dependencies included. You can use the [Maven Assembly Plugin](https://maven.apache.org/plugins/maven-assembly-plugin/usage.html) to generate JARs with dependencies. To install the plugin and add a Maven goal for a single JAR, add this to the `plugins` block in your `pom.xml`:

```xml
<plugin>
    <artifactId>maven-assembly-plugin</artifactId>
    <configuration>
        <descriptorRefs>
            <descriptorRef>jar-with-dependencies</descriptorRef>
        </descriptorRefs>
        <archive>
            <manifest>
                <mainClass></mainClass>
            </manifest>
        </archive>
    </configuration>
    <executions>
        <execution>
            <id>make-assembly</id>
            <phase>package</phase>
            <goals>
                <goal>single</goal>
            </goals>
        </execution>
    </executions>
</plugin>
```

Once your `pom.xml` is properly set up, you can compile the JAR with dependencies using this command:

```bash
$ mvn assembly:assembly
```

By default, this will add a JAR in your project's `target` folder with the name `PROJECT-NAME-VERSION-jar-with-dependencies.jar`. Here's an example topology submission command using a compiled JAR:

```bash
$ mvn assembly:assembly
$ heron submit local \
  target/my-project-1.2.3-jar-with-dependencies.jar \
  com.example.Main \
  MyTopology arg1 arg2
```

## Streamlet API topology configuration

Every Streamlet API topology needs to be configured using a `Config` object. Here's an example default configuration:

```scala
import org.apache.heron.streamlet.Config
import org.apache.heron.streamlet.scala.Runner

val topologyConfig = Config.defaultConfig()

// Apply topology configuration using the topologyConfig object
val topologyRunner = new Runner()
topologyRunner.run("name-for-topology", topologyConfig, topologyBuilder)
```

The table below shows the configurable parameters for Heron topologies:

Parameter | Default
:---------|:-------
[Delivery semantics](#delivery-semantics) | At most once
Serializer | [Kryo](https://github.com/EsotericSoftware/kryo)
Number of total container topologies | 2
Per-container CPU | 1.0
Per-container RAM | 100 MB

Here's an example non-default configuration:

```scala
val topologyConfig = Config.newBuilder()
        .setNumContainers(5)
        .setPerContainerRamInGigabytes(10)
        .setPerContainerCpu(3.5f)
        .setDeliverySemantics(Config.DeliverySemantics.EFFECTIVELY_ONCE)
        .setSerializer(Config.Serializer.JAVA)
        .setUserConfig("some-key", "some-value")
        .build()
```

### Delivery semantics

You can apply [delivery semantics](../../../concepts/delivery-semantics) to a Streamlet API topology like this:

```scala
topologyConfig
        .setDeliverySemantics(Config.DeliverySemantics.EFFECTIVELY_ONCE)
```

The other available options in the `DeliverySemantics` enum are `ATMOST_ONCE` and `ATLEAST_ONCE`.

## Streamlets

In the Heron Streamlet API for Scala, processing graphs consist of [streamlets](../../../concepts/topologies#streamlets). One or more supplier streamlets inject data into your graph to be processed by downstream operators.

## Operations

Operation | Description | Example
:---------|:------------|:-------
[`map`](#map-operations) | Create a new streamlet by applying the supplied mapping function to each element in the original streamlet | Add 1 to each element in a streamlet of integers
[`flatMap`](#flatmap-operations) | Like a map operation but with the important difference that each element of the streamlet is flattened | Flatten a sentence into individual words
[`filter`](#filter-operations) | Create a new streamlet containing only the elements that satisfy the supplied filtering function | Remove all inappropriate words from a streamlet of strings
[`union`](#union-operations) | Unifies two streamlets into one, without modifying the elements of the two streamlets | Unite two different `Streamlet<String>`s into a single streamlet
[`clone`](#clone-operations) | Creates any number of identical copies of a streamlet | Create three separate streamlets from the same source
[`transform`](#transform-operations) | Transform a streamlet using whichever logic you'd like (useful for transformations that don't neatly map onto the available operations) |
[`join`](#join-operations) | Create a new streamlet by combining two separate key-value streamlets into one on the basis of each element's key. Supported Join Types: Inner (as default), Outer-Left, Outer-Right and Outer | Combine key-value pairs listing current scores (e.g. `("h4x0r", 127)`) for each user into a single per-user stream
[`keyBy`](#key-by-operations) | Returns a new key-value streamlet by applying the supplied extractors to each element in the original streamlet |
[`reduceByKey`](#reduce-by-key-operations) |  Produces a streamlet of key-value on each key, and in accordance with a reduce function that you apply to all the accumulated values | Count the number of times a value has been encountered
[`reduceByKeyAndWindow`](#reduce-by-key-and-window-operations) |  Produces a streamlet of key-value on each key, within a time window, and in accordance with a reduce function that you apply to all the accumulated values | Count the number of times a value has been encountered within a specified time window
[`countByKey`](#count-by-key-operations) | A special reduce operation of counting number of tuples on each key | Count the number of times a value has been encountered
[`countByKeyAndWindow`](#count-by-key-and-window-operations) | A special reduce operation of counting number of tuples on each key, within a time window | Count the number of times a value has been encountered within a specified time window
[`split`](#split-operations) | Split a streamlet into multiple streamlets with different id |
[`withStream`](#with-stream-operations) | Select a stream with id from a streamlet that contains multiple streams |
[`applyOperator`](#apply-operator-operations) | Returns a new streamlet by applying an user defined operator to the original streamlet | Apply an existing bolt as an operator
[`repartition`](#repartition-operations) | Create a new streamlet by applying a new parallelism level to the original streamlet | Increase the parallelism of a streamlet from 5 to 10
[`toSink`](#sink-operations) | Sink operations terminate the processing graph by storing elements in a database, logging elements to stdout, etc. | Store processing graph results in an AWS Redshift table
[`log`](#log-operations) | Logs the final results of a processing graph to stdout. This *must* be the last step in the graph. |
[`consume`](#consume-operations) | Consume operations are like sink operations except they don't require implementing a full sink interface (consume operations are thus suited for simple operations like logging) | Log processing graph results using a custom formatting function

### Map operations

Map operations create a new streamlet by applying the supplied mapping function to each element in the original streamlet. Here's an example:

```scala
builder.newSource(() => 1)
    .map[Int]((i: Int) => i + 12) // or .map[Int](_.+(12)) as synthetic function
```

In this example, a supplier streamlet emits an indefinite series of 1s. The `map` operation then adds 12 to each incoming element, producing a streamlet of 13s.

### FlatMap operations

FlatMap operations are like `map` operations but with the important difference that each element of the streamlet is "flattened" into a collection type. In this example, a supplier streamlet emits the same sentence over and over again; the `flatMap` operation transforms each sentence into a Scala `List` of individual words:

```scala
builder.newSource(() => "I have nothing to declare but my genius")
    .flatMap[String](_.split(" "))
```

The effect of this operation is to transform the `Streamlet[String]` into a `Streamlet[List[String]]`.

> One of the core differences between `map` and `flatMap` operations is that `flatMap` operations typically transform non-collection types into collection types.

### Filter operations

Filter operations retain elements in a streamlet, while potentially excluding some or all elements, on the basis of a provided filtering function. Here's an example:

```scala
import java.util.concurrent.ThreadLocalRandom

builder.newSource(() => ThreadLocalRandom.current().nextInt(1, 11))
        .filter(_.<(7))
```

In this example, a source streamlet consisting of random integers between 1 and 10 is modified by a filter operation that removes all streamlet elements that are lower than 7.

### Union operations

Union operations combine two streamlets of the same type into a single streamlet without modifying the elements. Here's an example:

```scala
val flowers = builder.newSource(() => "flower")
val butterflies = builder.newSource(() => "butterfly")

val combinedSpringStreamlet = flowers.union(butterflies)
```

Here, one streamlet is an endless series of "flowers" while the other is an endless series of "butterflies". The `union` operation combines them into a single streamlet of alternating "flowers" and "butterflies".

### Clone operations

Clone operations enable you to create any number of "copies" of a streamlet. Each of the "copy" streamlets contains all the elements of the original and can be manipulated just like the original streamlet. Here's an example:

```scala
import scala.util.Random

val integers = builder.newSource(() => Random.nextInt(100))

val copies = integers.clone(5)
val ints1 = copies.get(0)
val ints2 = copies.get(1)
val ints3 = copies.get(2)
// and so on...
```

In this example, a streamlet of random integers between 0 and 99 is split into 5 identical streamlets.

### Transform operations

Transform operations are highly flexible operations that are most useful for:

* operations involving state in [stateful topologies](../../concepts/delivery-semantics#stateful-topologies)
* operations that don't neatly fit into the other categories or into a lambda-based logic

Transform operations require you to implement three different methods:

* A `setup` function that enables you to pass a context object to the operation and to specify what happens prior to the `transform` step
* A `transform` operation that performs the desired transformation
* A `cleanup` function that allows you to specify what happens after the `transform` step

The context object available to a transform operation provides access to:

* the current state of the topology
* the topology's configuration
* the name of the stream
* the stream partition
* the current task ID

Here's a Scala example of a transform operation in a topology where a stateful record is kept of the number of items processed:

```scala
import org.apache.heron.streamlet.Context
import org.apache.heron.streamlet.scala.SerializableTransformer

class CountNumberOfItems extends SerializableTransformer[String, String] {
    private val numberOfItems = new AtomicLong()

    override def setup(context: Context): Unit = {
      numberOfItems.incrementAndGet()
      context.getState().put("number-of-items", numberOfItems)
    }

    override def transform(i: String, f: String => Unit): Unit = {
      val transformedString = i.toUpperCase
      f(transformedString)
    }

    override def cleanup(): Unit =
      println(s"Successfully processed new state: $numberOfItems")
  }
```

This operation does a few things:

* In the `setup` method, the [`Context`](/api/java/org/apache/heron/streamlet/Context.html) object is used to access the current state (which has the semantics of a Java `Map`). The current number of items processed is incremented by one and then saved as the new state.
* In the `transform` method, the incoming string is transformed as UpperCase in some way and then "accepted" as the new value.
* In the `cleanup` step, the current count of items processed is logged.

Here's that operation within the context of a streamlet processing graph:

```scala
builder.newSource(() => "Some string over and over");
        .transform(new CountNumberOfItems())
        .log()
```

### Join operations

> For a more in-depth conceptual discussion of joins, see the [Heron Streamlet API](../../../concepts/streamlet-api#join-operations) doc.

Join operations unify two streamlets *on a key* (join operations thus require KV streamlets). Each `KeyValue` object in a streamlet has, by definition, a key. When a `join` operation is added to a processing graph,

```scala
import org.apache.heron.streamlet.{Config, KeyValue, WindowConfig}
import org.apache.heron.streamlet.scala.Builder

val builder = Builder.newBuilder()

val streamlet1 = builder
  .newSource(() =>
    new KeyValue[String, String]("heron-api", "topology-api"))
  .setName("streamlet1")

val streamlet2 = builder
  .newSource(() =>
    new KeyValue[String, String]("heron-api", "streamlet-api"))
  .setName("streamlet2")

streamlet1.join[KeyValue[String, String], KeyValue[String, String], String](
  streamlet2,
  (kv: KeyValue[String, String]) => kv,
  (kv: KeyValue[String, String]) => kv,
  WindowConfig.TumblingCountWindow(10),
  (kv1: KeyValue[String, String], kv2: KeyValue[String, String]) =>
    kv1.getValue + " - " + kv2.getValue
)
```

In this case, the resulting streamlet would consist of an indefinite stream with two `KeyValue` objects with the key `heron-api` but different values (`topology-api` and `streamlet-api`).

> The effect of a `join` operation is to create a new streamlet *for each key*.

### Key by operations

Key by operations convert each item in the original streamlet into a key-value pair and return a new streamlet. Here is an example:

```scala
val builder = Builder.newBuilder()

builder
  .newSource(() => "Paco de Lucia is one of the most popular virtuoso")
  // Convert each sentence into individual words
  .flatMap[String](_.split(" "))
  .keyBy[String, Int](
      // Key extractor (in this case, each word acts as the key)
      (word: String) => word,
      // Value extractor (get the length of each word)
      (word: String) => word.length
  )
  // The result is logged
  .log();
```

### Reduce by key operations

You can apply [reduce](https://docs.oracle.com/javase/tutorial/collections/streams/reduction.html) operations to streamlets by specifying:

* a key extractor that determines what counts as the key for the streamlet
* a value extractor that determines which final value is chosen for each element of the streamlet
* a reduce function that produces a single value for each key in the streamlet

Reduce by key operations produce a new streamlet of key-value window objects (which include a key-value pair including the extracted key and calculated value). Here's an example:

```scala
val builder = Builder.newBuilder()

builder
  .newSource(() => "Paco de Lucia is one of the most popular virtuoso")
  // Convert each sentence into individual words
  .flatMap[String](_.split(" "))
  .reduceByKey[String, Int](
      // Key extractor (in this case, each word acts as the key)
      (word: String) => word,
      // Value extractor (each word appears only once, hence the value is always 1)
      (word: String) => 1,
      // Reduce operation (a running sum)
      (x: Int, y: Int) => x + y)
  // The result is logged
  .log();
```

### Reduce by key and window operations

You can apply [reduce](https://docs.oracle.com/javase/tutorial/collections/streams/reduction.html) operations to streamlets by specifying:

* a key extractor that determines what counts as the key for the streamlet
* a value extractor that determines which final value is chosen for each element of the streamlet
* a [time window](../../../concepts/topologies#window-operations) across which the operation will take place
* a reduce function that produces a single value for each key in the streamlet

Reduce by key and window operations produce a new streamlet of key-value window objects (which include a key-value pair including the extracted key and calculated value, as well as information about the window in which the operation took place). Here's an example:

```scala
import org.apache.heron.streamlet.WindowConfig;

val builder = Builder.newBuilder()

builder
  .newSource(() => "Paco de Lucia is one of the most popular virtuoso")
  // Convert each sentence into individual words
  .flatMap[String](_.split(" "))
  .reduceByKeyAndWindow[String, Int](
      // Key extractor (in this case, each word acts as the key)
      (word: String) => word,
      // Value extractor (each word appears only once, hence the value is always 1)
      (word: String) => 1,
      // Window configuration
      WindowConfig.TumblingCountWindow(50),
      // Reduce operation (a running sum)
      (x: Int, y: Int) => x + y)
  // The result is logged
  .log();
```

### Count by key operations

Count by key operations extract keys from data in the original streamlet and count the number of times a key has been encountered. Here's an example:

```scala
val builder = Builder.newBuilder()

builder
  .newSource(() => "Paco de Lucia is one of the most popular virtuoso")
  // Convert each sentence into individual words
  .flatMap[String](_.split(" "))
  // Count the number of occurrences of each word
  .countByKey[String]((word: String) => word)
  // The result is logged
  .log();
```

### Count by key and window operations

Count by key and window operations extract keys from data in the original streamlet and count the number of times a key has been encountered within each [time window](../../../concepts/topologies#window-operations). Here's an example:

```scala
val builder = Builder.newBuilder()

builder
  .newSource(() => "Paco de Lucia is one of the most popular virtuoso")
  // Convert each sentence into individual words
  .flatMap[String](_.split(" "))
  // Count the number of occurrences of each word within each time window
  .countByKeyAndWindow[String](
      (word: String) => word,
      WindowConfig.TumblingCountWindow(50))
  // The result is logged
  .log();
```

### Split operations

Split operations split a streamlet into multiple streamlets with different id by getting the corresponding stream ids from each item in the origina streamlet. Here is an example:

```scala
val builder = Builder.newBuilder()

builder
  .newSource(() => "Paco de Lucia is one of the most popular virtuoso")
  // Convert each sentence into individual words
  .flatMap[String](_.split(" "))
  // Count the number of occurrences of each word within each time window
  .split(Map(
      "long_word" -> { word: String => word.length >= 4 },
      "short_word" -> { word: String => word.length < 4 }
  ))
  .withStream("short_word)
  // The result is logged
  .log();
```

### With stream operations

With stream operations select a stream with id from a streamlet that contains multiple streams. They are often used with [split](#split-operations).

### Apply operator operations

Apply operator operations apply a user defined operator (like a bolt) to each element of the original streamlet and return a new streamlet. Here is an example:

```scala
val builder = Builder.newBuilder()

private class MyBoltOperator extends MyBolt
    with IStreamletOperator[String, String] {
}

builder
  .newSource(() => "Paco de Lucia is one of the most popular virtuoso")
  // Convert each sentence into individual words
  .flatMap[String](_.split(" "))
  // Apply user defined operation
  .applyOperator(new MyBoltOperator())
  // The result is logged
  .log();
```

### Repartition operations

When you assign a number of [partitions](#partitioning-and-parallelism) to a processing step, each step that comes after it inherits that number of partitions. Thus, if you assign 5 partitions to a `map` operation, then any `mapToKV`, `flatMap`, `filter`, etc. operations that come after it will also be assigned 5 partitions. But you can also change the number of partitions for a processing step (as well as the number of partitions for downstream operations) using `repartition`. Here's an example:

```scala
import java.util.concurrent.ThreadLocalRandom;

val builder = Builder.newBuilder

val numbers = builder
  .newSource(() => ThreadLocalRandom.current().nextInt(1, 11))

numbers
  .setNumPartitions(5)
  .map(i => i + 1)
  .repartition(2)
  .filter(i => i > 7 && i < 2)
  .log()
```

In this example, the supplier streamlet emits random integers between 1 and 10. That operation is assigned 5 partitions. After the `map` operation, the `repartition` function is used to assign 2 partitions to all downstream operations.

### Sink operations

In processing graphs like the ones you build using the Heron Streamlet API, **sinks** are essentially the terminal points in your graph, where your processing logic comes to an end. A processing graph can end with writing to a database, publishing to a topic in a pub-sub messaging system, and so on. With the Streamlet API, you can implement your own custom sinks. Here's an example:

```scala
import org.apache.heron.streamlet.Context
import org.apache.heron.streamlet.scala.Sink

class FormattedLogSink extends Sink[String] {
    private var streamName: Option[String] = None

    override def setup(context: Context): Unit =
      streamName = Some(context.getStreamName)

    override def put(tuple: String): Unit =
      println(s"The current value of tuple is $tuple in stream: $streamName")

    override def cleanup(): Unit = {}
  }
```

In this example, the sink fetches the name of the enclosing streamlet from the context passed in the `setup` method. The `put` method specifies how the sink handles each element that is received (in this case, a formatted message is logged to stdout). The `cleanup` method enables you to specify what happens after the element has been processed by the sink.

Here is the `FormattedLogSink` at work in an example processing graph:

```scala
val builder = Builder.newBuilder

builder.newSource(() => "Here is a string to be passed to the sink")
        .toSink(new FormattedLogSink)
```

> [Log operations](#log-operations) rely on a log sink that is provided out of the box. You'll need to implement other sinks yourself.

### Log operations

Log operations are special cases of consume operations that log streamlet elements to stdout.

> Streamlet elements will be using their `toString` representations and at the `INFO` level.

### Consume operations

Consume operations are like [sink operations](#sink-operations) except they don't require implementing a full sink interface. Consume operations are thus suited for simple operations like formatted logging. Here's an example:

```scala
val builder = Builder.newBuilder
      .newSource(() => Random.nextInt(10))
      .filter(i => i % 2 == 0)
      .consume(i => println(s"Even number found: $i"))
```
