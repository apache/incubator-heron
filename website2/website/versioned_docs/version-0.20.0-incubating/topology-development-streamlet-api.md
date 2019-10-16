---
id: version-0.20.0-incubating-topology-development-streamlet-api
title: The Heron Streamlet API for Java
sidebar_label: The Heron Streamlet API for Java
original_id: topology-development-streamlet-api
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

 > **The Heron Streamlet API is in beta.** 
 > The Heron Streamlet API is well tested and can be used to build and test topologies locally. The API is not yet fully stable, however, and breaking changes are likely in the coming weeks.


Heron processing topologies can be written using an API called the **Heron Streamlet API**. The Heron Streamlet API is currently available for the following languages:

* [Java](topology-development-streamlet-api)
* [Scala](topology-development-streamlet-scala)

> Although this document covers the new Heron Streamlet API, topologies created using the original [topology API](topology-development-topology-api-java) can still be used with Heron (which means that all of your older topologies will still run).

For a more in-depth conceptual guide to the new API, see [The Heron Streamlet API](topology-development-streamlet-api). A high-level overview can also be found in the section immediately [below](#the-heron-streamlet-api-vs-the-topology-api).

## The Heron Streamlet API vs. The Topology API

When Heron was first released, all Heron topologies needed to be written using an API based on the [Storm Topology API](topology-development-topology-api-java). Although this API is quite powerful (and can still be used), the **Heron Streamlet API** enables you to create topologies without needing to implement spouts and bolts directly or to connect spouts and bolts together.

Here are some crucial differences between the two APIs:

Domain | Original Topology API | Heron Streamlet API
:------|:----------------------|:--------------------
Programming style | Procedural, processing component based | Functional
Abstraction level | **Low level**. Developers must think in terms of "physical" spout and bolt implementation logic. | **High level**. Developers can write processing logic in an idiomatic fashion in the language of their choice, without needing to write and connect spouts and bolts.
Processing model | [Spout](heron-topology-concepts#spouts) and [bolt](heron-topology-concepts#bolts) logic must be created explicitly, and connecting spouts and bolts is the responsibility of the developer | Spouts and bolts are created for you automatically on the basis of the processing graph that you build

The two APIs also have a few things in common:

* Topologies' [logical](heron-topology-concepts#logical-plan) and [physical](heron-topology-concepts#physical-plan) plans are automatically created by Heron
* Topologies are [managed](user-manuals-heron-cli) in the same way using the `heron` CLI tool

## Getting started

In order to use the Heron Streamlet API for Java, you'll need to install the `heron-api` library.

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

In order to run a Java topology created using the Heron Streamlet API in a Heron cluster, you'll need to package your topology as a "fat" JAR with dependencies included. You can use the [Maven Assembly Plugin](https://maven.apache.org/plugins/maven-assembly-plugin/usage.html) to generate JARs with dependencies. To install the plugin and add a Maven goal for a single JAR, add this to the `plugins` block in your `pom.xml`:

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

### Java Streamlet API starter project

If you'd like to up and running quickly with the Heron Streamlet API for Java, you can clone [this repository](https://github.com/streamlio/heron-java-streamlet-api-example), which includes an example topology built using the Streamlet API as well as the necessary Maven configuration. To build a JAR with dependencies of this example topology:

```bash
$ git clone https://github.com/streamlio/heron-java-streamlet-api-example
$ cd heron-java-streamlet-api-example
$ mvn assembly:assembly
$ ls target/*.jar
target/heron-java-streamlet-api-example-latest-jar-with-dependencies.jar
target/heron-java-streamlet-api-example-latest.jar
```

If you're running a [local Heron cluster](getting-started-local-single-node), you can submit the built example topology like this:

```bash
$ heron submit local target/heron-java-streamlet-api-example-latest-jar-with-dependencies.jar \
  io.streaml.heron.streamlet.WordCountStreamletTopology \
  WordCountStreamletTopology
```

#### Selecting delivery semantics

Heron enables you to apply one of three [delivery semantics](heron-delivery-semantics) to any Heron topology. For the [example topology](#java-streamlet-api-starter-project) above, you can select the delivery semantics when you submit the topology with the topology's second argument. This command, for example, would apply [effectively-once](heron-delivery-semantics) to the example topology:

```bash
$ heron submit local target/heron-java-streamlet-api-example-latest-jar-with-dependencies.jar \
  io.streaml.heron.streamlet.WordCountStreamletTopology \
  WordCountStreamletTopology \
  effectively-once
```

The other options are `at-most-once` and `at-least-once`. If you don't explicitly select the delivery semantics, at-least-once semantics will be applied.

## Streamlet API topology configuration

Every Streamlet API topology needs to be configured using a `Config` object. Here's an example default configuration:

```java
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Runner;

Config topologyConfig = Config.defaultConfig();

// Apply topology configuration using the topologyConfig object
Runner topologyRunner = new Runner();
topologyRunner.run("name-for-topology", topologyConfig, topologyBuilder);
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

```java
Config topologyConfig = Config.newBuilder()
        .setNumContainers(5)
        .setPerContainerRamInGigabytes(10)
        .setPerContainerCpu(3.5f)
        .setDeliverySemantics(Config.DeliverySemantics.EFFECTIVELY_ONCE)
        .setSerializer(Config.Serializer.JAVA)
        .setUserConfig("some-key", "some-value")
        .build();
```

### Delivery semantics

You can apply [delivery semantics](heron-delivery-semantics) to a Streamlet API topology like this:

```java
topologyConfig
        .setDeliverySemantics(Config.DeliverySemantics.EFFECTIVELY_ONCE);
```

The other available options in the `DeliverySemantics` enum are `ATMOST_ONCE` and `ATLEAST_ONCE`.

## Streamlets

In the Heron Streamlet API for Java, processing graphs consist of streamlets. One or more supplier streamlets inject data into your graph to be processed by downstream operators.

## Operations

Operation | Description | Example
:---------|:------------|:-------
[`map`](#map-operations) | Create a new streamlet by applying the supplied mapping function to each element in the original streamlet | Add 1 to each element in a streamlet of integers
[`flatMap`](#flatmap-operations) | Like a map operation but with the important difference that each element of the streamlet is flattened | Flatten a sentence into individual words
[`filter`](#filter-operations) | Create a new streamlet containing only the elements that satisfy the supplied filtering function | Remove all inappropriate words from a streamlet of strings
[`union`](#union-operations) | Unifies two streamlets into one, without modifying the elements of the two streamlets | Unite two different `Streamlet<String>`s into a single streamlet
[`clone`](#clone-operations) | Creates any number of identical copies of a streamlet | Create three separate streamlets from the same source
[`transform`](#transform-operations) | Transform a streamlet using whichever logic you'd like (useful for transformations that don't neatly map onto the available operations) |
[`join`](#join-operations) | Create a new streamlet by combining two separate key-value streamlets into one on the basis of each element's key. Supported Join Types: Inner (as default), Outer-Left, Outer-Right and Outer. | Combine key-value pairs listing current scores (e.g. `("h4x0r", 127)`) for each user into a single per-user stream
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

```java
builder.newSource(() -> 1)
    .map(i -> i + 12);
```

In this example, a supplier streamlet emits an indefinite series of 1s. The `map` operation then adds 12 to each incoming element, producing a streamlet of 13s.

### FlatMap operations

FlatMap operations are like `map` operations but with the important difference that each element of the streamlet is "flattened" into a collection type. In this example, a supplier streamlet emits the same sentence over and over again; the `flatMap` operation transforms each sentence into a Java `List` of individual words:

```java
builder.newSource(() -> "I have nothing to declare but my genius")
    .flatMap((sentence) -> Arrays.asList(sentence.split("\\s+")));
```

The effect of this operation is to transform the `Streamlet<String>` into a `Streamlet<List<String>>`.

> One of the core differences between `map` and `flatMap` operations is that `flatMap` operations typically transform non-collection types into collection types.

### Filter operations

Filter operations retain elements in a streamlet, while potentially excluding some or all elements, on the basis of a provided filtering function. Here's an example:

```java
builder.newSource(() -> ThreadLocalRandom.current().nextInt(1, 11))
        .filter((i) -> i < 7);
```

In this example, a source streamlet consisting of random integers between 1 and 10 is modified by a `filter` operation that removes all streamlet elements that are greater than 6.

### Union operations

Union operations combine two streamlets of the same type into a single streamlet without modifying the elements. Here's an example:

```java
Streamlet<String> flowers = builder.newSource(() -> "flower");
Streamlet<String> butterflies = builder.newSource(() -> "butterfly");

Streamlet<String> combinedSpringStreamlet = flowers
        .union(butterflies);
```

Here, one streamlet is an endless series of "flowers" while the other is an endless series of "butterflies". The `union` operation combines them into a single `Spring` streamlet of alternating "flowers" and "butterflies".

### Clone operations

Clone operations enable you to create any number of "copies" of a streamlet. Each of the "copy" streamlets contains all the elements of the original and can be manipulated just like the original streamlet. Here's an example:

```java
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

Streamlet<Integer> integers = builder.newSource(() -> ThreadLocalRandom.current().nextInt(100));

List<Streamlet<Integer>> copies = integers.clone(5);
Streamlet<Integer> ints1 = copies.get(0);
Streamlet<Integer> ints2 = copies.get(1);
Streamlet<Integer> ints3 = copies.get(2);
// and so on...
```

In this example, a streamlet of random integers between 1 and 100 is split into 5 identical streamlets.

### Transform operations

Transform operations are highly flexible operations that are most useful for:

* operations involving state in [stateful topologies](heron-delivery-semantics#stateful-topologies)
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
import org.apache.heron.streamlet.Context;
import org.apache.heron.streamlet.SerializableTransformer;

import java.util.function.Consumer;

public class CountNumberOfItems implements SerializableTransformer<String, String> {
    private int numberOfItems;

    public void setup(Context context) {
        numberOfItems = (int) context.getState().get("number-of-items");
        context.getState().put("number-of-items", numberOfItems + 1);
    }

    public void transform(String in, Consumer<String> consumer) {
        String transformedString = // Apply some operation to the incoming value
        consumer.accept(transformedString);
    }

    public void cleanup() {
        System.out.println(
                String.format("Successfully processed new state: %d", numberOfItems));
    }
}
```

This operation does a few things:

* In the `setup` method, the [`Context`](/api/java/org/apache/heron/streamlet/Context.html) object is used to access the current state (which has the semantics of a Java `Map`). The current number of items processed is incremented by one and then saved as the new state.
* In the `transform` method, the incoming string is transformed in some way and then "accepted" as the new value.
* In the `cleanup` step, the current count of items processed is logged.

Here's that operation within the context of a streamlet processing graph:

```java
builder.newSource(() -> "Some string over and over");
        .transform(new CountNumberOfItems())
        .log();
```

### Join operations

Join operations unify two streamlets *on a key* (join operations thus require KV streamlets). Each `KeyValue` object in a streamlet has, by definition, a key. When a join operation is added to a processing graph, 

```java
import org.apache.heron.streamlet.WindowConfig;

Builder builder = Builder.newBuilder();

KVStreamlet<String, String> streamlet1 =
        builder.newKVSource(() -> new KeyValue<>("heron-api", "topology-api"));

builder.newSource(() -> new KeyValue<>("heron-api", "streamlet-api"))
    .join(streamlet1, WindowConfig.TumblingCountWindow(10), KeyValue::create);
```

In this case, the resulting streamlet would consist of an indefinite stream with two `KeyValue` objects with the key `heron-api` but different values (`topology-api` and `streamlet-api`).

> The effect of a join operation is to create a new streamlet *for each key*.

### Key by operations

Key by operations convert each item in the original streamlet into a key-value pair and return a new streamlet. Here is an example:

```java
import java.util.Arrays;

Builder builder = Builder.newBuilder()
    .newSource(() -> "Mary had a little lamb")
    // Convert each sentence into individual words
    .flatMap(sentence -> Arrays.asList(sentence.toLowerCase().split("\\s+")))
    .keyBy(
        // Key extractor (in this case, each word acts as the key)
        word -> word,
        // Value extractor (get the length of each word)
        word -> workd.length()
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

```java
import java.util.Arrays;

Builder builder = Builder.newBuilder()
    .newSource(() -> "Mary had a little lamb")
    // Convert each sentence into individual words
    .flatMap(sentence -> Arrays.asList(sentence.toLowerCase().split("\\s+")))
    .reduceByKeyAndWindow(
        // Key extractor (in this case, each word acts as the key)
        word -> word,
        // Value extractor (each word appears only once, hence the value is always 1)
        word -> 1,
        // Reduce operation (a running sum)
        (x, y) -> x + y
    )
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

```java
import java.util.Arrays;

import org.apache.heron.streamlet.WindowConfig;

Builder builder = Builder.newBuilder()
    .newSource(() -> "Mary had a little lamb")
    // Convert each sentence into individual words
    .flatMap(sentence -> Arrays.asList(sentence.toLowerCase().split("\\s+")))
    .reduceByKeyAndWindow(
        // Key extractor (in this case, each word acts as the key)
        word -> word,
        // Value extractor (each word appears only once, hence the value is always 1)
        word -> 1,
        // Window configuration
        WindowConfig.TumblingCountWindow(50),
        // Reduce operation (a running sum)
        (x, y) -> x + y
    )
    // The result is logged
    .log();
```

### Count by key operations

Count by key operations extract keys from data in the original streamlet and count the number of times a key has been encountered. Here's an example:

```java
import java.util.Arrays;

Builder builder = Builder.newBuilder()
    .newSource(() -> "Mary had a little lamb")
    // Convert each sentence into individual words
    .flatMap(sentence -> Arrays.asList(sentence.toLowerCase().split("\\s+")))
    .countByKeyAndWindow(word -> word)
    // The result is logged
    .log();
```

### Count by key and window operations

Count by key and window operations extract keys from data in the original streamlet and count the number of times a key has been encountered within each [time window](../../../concepts/topologies#window-operations). Here's an example:

```java
import java.util.Arrays;

import org.apache.heron.streamlet.WindowConfig;

Builder builder = Builder.newBuilder()
    .newSource(() -> "Mary had a little lamb")
    // Convert each sentence into individual words
    .flatMap(sentence -> Arrays.asList(sentence.toLowerCase().split("\\s+")))
    .countByKeyAndWindow(
        // Key extractor (in this case, each word acts as the key)
        word -> word,
        // Window configuration
        WindowConfig.TumblingCountWindow(50),
    )
    // The result is logged
    .log();
```

### Split operations

Split operations split a streamlet into multiple streamlets with different id by getting the corresponding stream ids from each item in the origina streamlet. Here is an example:

```java
import java.util.Arrays;

Map<String, SerializablePredicate<String>> splitter = new HashMap();
    splitter.put("long_word", s -> s.length() >= 4);
    splitter.put("short_word", s -> s.length() < 4);

Builder builder = Builder.newBuilder()
    .newSource(() -> "Mary had a little lamb")
    // Convert each sentence into individual words
    .flatMap(sentence -> Arrays.asList(sentence.toLowerCase().split("\\s+")))
    // Splits the stream into streams of long and short words
    .split(splitter)
    // Choose the stream of the short words
    .withStream("short_word")
    // The result is logged
    .log();
```

### With stream operations

With stream operations select a stream with id from a streamlet that contains multiple streams. They are often used with [split](#split-operations).

### Apply operator operations

Apply operator operations apply a user defined operator (like a bolt) to each element of the original streamlet and return a new streamlet. Here is an example:

```java
import java.util.Arrays;

private class MyBoltOperator extends MyBolt implements IStreamletRichOperator<Double, Double> {
}

Builder builder = Builder.newBuilder()
    .newSource(() -> "Mary had a little lamb")
    // Convert each sentence into individual words
    .flatMap(sentence -> Arrays.asList(sentence.toLowerCase().split("\\s+")))
    // Apply user defined operation
    .applyOperator(new MyBoltOperator())
    // The result is logged
    .log();
```

### Repartition operations

When you assign a number of [partitions](#partitioning-and-parallelism) to a processing step, each step that comes after it inherits that number of partitions. Thus, if you assign 5 partitions to a `map` operation, then any `mapToKV`, `flatMap`, `filter`, etc. operations that come after it will also be assigned 5 partitions. But you can also change the number of partitions for a processing step (as well as the number of partitions for downstream operations) using `repartition`. Here's an example:

```java
import java.util.concurrent.ThreadLocalRandom;

Builder builder = Builder.newBuilder();

builder.newSource(() -> ThreadLocalRandom.current().nextInt(1, 11))
        .setNumPartitions(5)
        .map(i -> i + 1)
        .repartition(2)
        .filter(i -> i > 7 && i < 2)
        .log();
```

In this example, the supplier streamlet emits random integers between one and ten. That operation is assigned 5 partitions. After the `map` operation, the `repartition` function is used to assign 2 partitions to all downstream operations.

### Sink operations

In processing graphs like the ones you build using the Heron Streamlet API, **sinks** are essentially the terminal points in your graph, where your processing logic comes to an end. A processing graph can end with writing to a database, publishing to a topic in a pub-sub messaging system, and so on. With the Streamlet API, you can implement your own custom sinks. Here's an example:

```java
import org.apache.heron.streamlet.Context;
import org.apache.heron.streamlet.Sink;

public class FormattedLogSink implements Sink<T> {
    private String streamletName;

    public void setup(Context context) {
        streamletName = context.getStreamName();
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
Builder builder = Builder.newBuilder();

builder.newSource(() -> "Here is a string to be passed to the sink")
        .toSink(new FormattedLogSink());
```

> [Log operations](#log-operations) rely on a log sink that is provided out of the box. You'll need to implement other sinks yourself.

### Log operations

Log operations are special cases of consume operations that log streamlet elements to stdout.

> Streamlet elements will be using their `toString` representations and at the `INFO` level.

### Consume operations

Consume operations are like [sink operations](#sink-operations) except they don't require implementing a full sink interface. Consume operations are thus suited for simple operations like formatted logging. Here's an example:

```java
import java.util.concurrent.ThreadLocalRandom;

Builder builder = Builder.newBuilder()
        .newSource(() -> ThreadLocalRandom.current().nextInt(1, 11))
        .filter(i -> i % 2 == 0)
        .consume(i -> {
            String message = String.format("Even number found: %d", i);
            System.out.println(message);
        });
```
