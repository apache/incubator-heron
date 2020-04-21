---
id: heron-streamlet-concepts
title: Heron Streamlets
sidebar_label: Heron Streamlets
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

When it was first released, Heron offered a **Topology API**---heavily indebted to the [Storm API](http://storm.apache.org/about/simple-api.html)---for developing topology logic. In the original Topology API, developers creating topologies were required to explicitly:

* define the behavior of every [spout](topology-development-topology-api-java#spouts) and [bolt](topology-development-topology-api-java#bolts) in the topology 
* specify how those spouts and bolts are meant to be interconnected

### Problems with the Topology API

Although the Storm-inspired API provided a powerful low-level interface for creating topologies, the spouts-and-bolts model also presented a variety of drawbacks for Heron developers:

Drawback | Description
:--------|:-----------
Verbosity | In the original Topology API for both Java and Python, creating spouts and bolts required substantial boilerplate and forced developers to both provide implementations for spout and bolt classes and also to specify the connections between those spouts and bolts.
Difficult debugging | When spouts, bolts, and the connections between them need to be created "by hand," it can be challenging to trace the origin of problems in the topology's processing chain
Tuple-based data model | In the older topology API, spouts and bolts passed [tuples](https://en.wikipedia.org/wiki/Tuple) and nothing but tuples within topologies. Although tuples are a powerful and flexible data type, the topology API forced *all* spouts and bolts to implement their own serialization/deserialization logic.

### Advantages of the Streamlet API

In contrast with the Topology API, the Heron Streamlet API offers:

Advantage | Description
:---------|:-----------
Boilerplate-free code | Instead of needing to implement spout and bolt classes over and over again, the Heron Streamlet API enables you to create stream processing logic out of functions, such as map, flatMap, join, and filter functions, instead.
Easy debugging | With the Heron Streamlet API, you don't have to worry about spouts and bolts, which means that you can more easily surface problems with your processing logic.
Completely flexible, type-safe data model | Instead of requiring that all processing components pass tuples to one another (which implicitly requires serialization to and deserializaton from your application-specific types), the Heron Streamlet API enables you to write your processing logic in accordance with whatever types you'd like---including tuples, if you wish.<br /><br />In the Streamlet API for [Java](topology-development-streamlet-api), all streamlets are typed (e.g. `Streamlet<MyApplicationType>`), which means that type errors can be caught at compile time rather than at runtime.

## Streamlet API topology model

Instead of spouts and bolts, as with the Topology API, the Streamlet API enables you to create **processing graphs** that are then automatically converted to spouts and bolts under the hood. Processing graphs consist of the following components:

* **Sources** supply the processing graph with data from random generators, databases, web service APIs, filesystems, pub-sub messaging systems, or anything that implements the [source](#source-operations) interface.
* **Operators** supply the graph's processing logic, operating on data passed into the graph by sources.
* **Sinks** are the terminal endpoints of the processing graph, determining what the graph *does* with the processed data. Sinks can involve storing data in a database, logging results to stdout, publishing messages to a topic in a pub-sub messaging system, and much more.

The diagram below illustrates both the general model (with a single source, three operators, and one sink), and a more concrete example that includes two sources (an [Apache Pulsar](https://pulsar.incubator.apache.org) topic and the [Twitter API](https://developer.twitter.com/en/docs)), three operators (a [join](#join-operations), [flatMap](#flatmap-operations), and [reduce](#reduce-operations) operation), and two [sinks](#sink-operations) (an [Apache Cassandra](http://cassandra.apache.org/) table and an [Apache Spark](https://spark.apache.org/) job).

![Topology Operators](https://www.lucidchart.com/publicSegments/view/d84026a1-d12e-4878-b8d5-5aa274ec0415/image.png)

### Streamlets

The core construct underlying the Heron Streamlet API is that of the **streamlet**. A streamlet is an unbounded, ordered collection of **elements** of some data type (streamlets can consist of simple types like integers and strings or more complex, application-specific data types).

**Source streamlets** supply a Heron processing graph with data inputs. These inputs can come from a wide variety of sources, such as pub-sub messaging systems like [Apache
Kafka](http://kafka.apache.org/) and [Apache Pulsar](https://pulsar.incubator.apache.org) (incubating), random generators, or static files like CSV or [Apache Parquet](https://parquet.apache.org/) files.

Source streamlets can then be manipulated in a wide variety of ways. You can, for example:

* apply [map](#map-operations), [filter](#filter-operations), [flatMap](#flatmap-operations), and many other operations to them
* apply operations, such as [join](#join-operations) and [union](#union-operations) operations, that combine streamlets together
* [reduce](#reduce-by-key-and-window-operations) all elements in a streamlet to some single value, based on key
* send data to [sinks](#sink-operations) (store elements)

The diagram below shows an example streamlet:

![Streamlet](https://www.lucidchart.com/publicSegments/view/5c451e53-46f8-4e36-86f4-9a11ca015c21/image.png)


In this diagram, the **source streamlet** is produced by a random generator that continuously emits random integers between 1 and 100. From there:

* A filter operation is applied to the source streamlet that filters out all values less than or equal to 30
* A *new streamlet* is produced by the filter operation (with the Heron Streamlet API, you're always transforming streamlets into other streamlets)
* A map operation adds 15 to each item in the streamlet, which produces the final streamlet in our graph. We *could* hypothetically go much further and add as many transformation steps to the graph as we'd like.
* Once the final desired streamlet is created, each item in the streamlet is sent to a sink. Sinks are where items leave the processing graph. 

### Supported languages

The Heron Streamlet API is currently available for:

* [Java](topology-development-streamlet-api)
* [Scala](topology-development-streamlet-scala)

### The Heron Streamlet API and topologies

With the Heron Streamlet API *you still create topologies*, but only implicitly. Heron automatically performs the heavy lifting of converting the streamlet-based processing logic that you create into spouts and bolts and, from there, into containers that are then deployed using whichever [scheduler](schedulers-local.md) your Heron cluster relies upon.

From the standpoint of both operators and developers [managing topologies' lifecycles](#topology-lifecycle), the resulting topologies are equivalent. From a development workflow standpoint, however, the difference is profound. You can think of the Streamlet API as a highly convenient tool for creating spouts, bolts, and the logic that connects them.

The basic workflow looks like this:

![Streamlet](https://www.lucidchart.com/publicSegments/view/6b2e9b49-ef1f-45c9-8094-1e2cefbaed7b/image.png)

When creating topologies using the Heron Streamlet API, you simply write code (example [below](#java-processing-graph-example)) in a highly functional style. From there:

* that code is automatically converted into spouts, bolts, and the necessary connective logic between spouts and bolts
* the spouts and bolts are automatically converted into a [logical plan](topology-development-topology-api-java#logical-plan) that specifies how the spouts and bolts are connected to each other
* the logical plan is automatically converted into a [physical plan](topology-development-topology-api-java#physical-plan) that determines how the spout and bolt instances (the colored boxes above) are distributed across the specified number of containers (in this case two)

With a physical plan in place, the Streamlet API topology can be submitted to a Heron cluster.

#### Java processing graph example

The code below shows how you could implement the processing graph shown [above](#streamlets) in Java:

```java
import java.util.concurrent.ThreadLocalRandom;

import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Runner;

Builder builder = Builder.newBuilder();

// Function for generating random integers
int randomInt(int lower, int upper) {
    return ThreadLocalRandom.current().nextInt(lower, upper + 1);
}

// Source streamlet
builder.newSource(() -> randomInt(1, 100))
    // Filter operation
    .filter(i -> i > 30)
    // Map operation
    .map(i -> i + 15)
    // Log sink
    .log();

Config config = new Config();
// This topology will be spread across two containers
config.setNumContainers(2);

// Submit the processing graph to Heron as a topology
new Runner("IntegerProcessingGraph", config, builder).run();
```

As you can see, the Java code for the example streamlet processing graph requires very little boilerplate and is heavily indebted to Java [lambda](https://docs.oracle.com/javase/tutorial/java/javaOO/lambdaexpressions.html) patterns.

## Streamlet operations

In the Heron Streamlet API, processing data means *transforming streamlets into other streamlets*. This can be done using a wide variety of available operations, including many that you may be familiar with from functional programming:

Operation | Description
:---------|:-----------
[map](#map-operations) | Returns a new streamlet by applying the supplied mapping function to each element in the original streamlet
[flatMap](#flatMap-operations) | Like a map operation but with the important difference that each element of the streamlet is flattened into a collection type
[filter](#filter-operations) | Returns a new streamlet containing only the elements that satisfy the supplied filtering function
[union](#filter-operations) | Unifies two streamlets into one, without [windowing](#windowing) or modifying the elements of the two streamlets
[clone](#clone-operations) | Creates any number of identical copies of a streamlet
[transform](#transform-operations) | Transform a streamlet using whichever logic you'd like (useful for transformations that don't neatly map onto the available operations) | Modify the elements from an incoming streamlet and update the topology's state
[keyBy](#key-by-operations) | Returns a new key-value streamlet by applying the supplied extractors to each element in the original streamlet
[reduceByKey](#reduce-by-key-operations) | Produces a streamlet of key-value on each key and in accordance with a reduce function that you apply to all the accumulated values
[reduceByKeyAndWindow](#reduce-by-key-and-window-operations) |  Produces a streamlet of key-value on each key, within a [time window](#windowing), and in accordance with a reduce function that you apply to all the accumulated values
[countByKey](#count-by-key-operations) | A special reduce operation of counting number of tuples on each key
[countByKeyAndWindow](#count-by-key-and-window-operations) | A special reduce operation of counting number of tuples on each key, within a [time window](#windowing)
[split](#split-operations) | Split a streamlet into multiple streamlets with different id.
[withStream](#with-stream-operations) | Select a stream with id from a streamlet that contains multiple streams
[applyOperator](#apply-operator-operations) | Returns a new streamlet by applying an user defined operator to the original streamlet
[join](#join-operations) | Joins two separate key-value streamlets into a single streamlet on a key, within a [time window](#windowing), and in accordance with a join function
[log](#log-operations) | Logs the final streamlet output of the processing graph to stdout
[toSink](#sink-operations) | Sink operations terminate the processing graph by storing elements in a database, logging elements to stdout, etc.
[consume](#consume-operations) | Consume operations are like sink operations except they don't require implementing a full sink interface (consume operations are thus suited for simple operations like logging)

### Map operations

Map operations create a new streamlet by applying the supplied mapping function to each element in the original streamlet.

#### Java example

```java
import org.apache.heron.streamlet.Builder;

Builder processingGraphBuilder = Builder.newBuilder();

Streamlet<Integer> ones = processingGraphBuilder.newSource(() -> 1);
Streamlet<Integer> thirteens = ones.map(i -> i + 12);
```

In this example, a supplier streamlet emits an indefinite series of 1s. The `map` operation then adds 12 to each incoming element, producing a streamlet of 13s. The effect of this operation is to transform the `Streamlet<Integer>` into a `Streamlet<Integer>` with different values (map operations can also convert streamlets into streamlets of a different type).

### FlatMap operations

FlatMap operations are like [map operations](#map-operations) but with the important difference that each element of the streamlet is "flattened" into a collection type. In the Java example below, a supplier streamlet emits the same sentence over and over again; the `flatMap` operation transforms each sentence into a Java `List` of individual words.

#### Java example

```java
Streamlet<String> sentences = builder.newSource(() -> "I have nothing to declare but my genius");
Streamlet<List<String>> words = sentences
        .flatMap((sentence) -> Arrays.asList(sentence.split("\\s+")));
```

The effect of this operation is to transform the `Streamlet<String>` into a `Streamlet<List<String>>` containing each word emitted by the source streamlet.

### Filter operations

Filter operations retain some elements in a streamlet and exclude other elements on the basis of a provided filtering function.

#### Java example

```java
Streamlet<Integer> randomInts =
    builder.newSource(() -> ThreadLocalRandom.current().nextInt(1, 11));
Streamlet<Integer> lessThanSeven = randomInts
        .filter(i -> i <= 7);
```

In this example, a source streamlet consisting of random integers between 1 and 10 is modified by a filter operation that removes all streamlet elements that are greater than 7.

### Union operations

Union operations combine two streamlets of the same type into a single streamlet without modifying the elements.

#### Java example

```java
Streamlet<String> oohs = builder.newSource(() -> "ooh");
Streamlet<String> aahs = builder.newSource(() -> "aah");

Streamlet<String> combined = oohs
        .union(aahs);
```

Here, one streamlet is an endless series of "ooh"s while the other is an endless series of "aah"s. The `union` operation combines them into a single streamlet of alternating "ooh"s and "aah"s.

### Clone operations

Clone operations enable you to create any number of "copies" of a streamlet. Each of the "copy" streamlets contains all the elements of the original and can be manipulated just like the original streamlet.

#### Java example

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
        numberOfItems = (int) context.getState("number-of-items");
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

### Key by operations

Key by operations convert each item in the original streamlet into a key-value pair and return a new streamlet.

#### Java example

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

Reduce by key operations produce a new streamlet of key-value window objects (which include a key-value pair including the extracted key and calculated value).

#### Java example

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
* a [time window](heron-topology-concepts#window-operations) across which the operation will take place
* a reduce function that produces a single value for each key in the streamlet

Reduce by key and window operations produce a new streamlet of key-value window objects (which include a key-value pair including the extracted key and calculated value, as well as information about the window in which the operation took place).

#### Java example

```java
import java.util.Arrays;

import org.apache.heron.streamlet.WindowConfig;

Builder builder = Builder.newBuilder();

builder.newSource(() -> "Mary had a little lamb")
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

Count by key operations extract keys from data in the original streamlet and count the number of times a key has been encountered.

#### Java example

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

Count by key and window operations extract keys from data in the original streamlet and count the number of times a key has been encountered within each [time window](#windowing).

#### Java example

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

Split operations split a streamlet into multiple streamlets with different id by getting the corresponding stream ids from each item in the origina streamlet.

#### Java example

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

Apply operator operations apply a user defined operator (like a bolt) to each element of the original streamlet and return a new streamlet.

#### Java example

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

### Join operations

Join operations in the Streamlet API take two streamlets (a "left" and a "right" streamlet) and join them together:

* based on a key extractor for each streamlet
* over key-value elements accumulated during a specified [time window](#windowing)
* based on a [join type](#join-types) ([inner](#inner-joins), [outer left](#outer-left-joins), [outer right](#outer-right-joins), or [outer](#outer-joins))
* using a join function that specifies *how* values will be processed

You may already be familiar with `JOIN` operations in SQL databases, like this:

```sql
SELECT username, email
FROM all_users
INNER JOIN banned_users ON all_users.username NOT IN banned_users.username;
```

> If you'd like to unite two streamlets into one *without* applying a window or a join function, you can use a [union](#union-operations) operation, which are available for key-value streamlets as well as normal streamlets.

All join operations are performed:

1. Over elements accumulated during a specified [time window](#windowing)
1. In accordance with a key and value extracted from each streamlet element (you must provide extractor functions for both)
1. In accordance with a join function that produces a "joined" value for each pair of streamlet elements

#### Join types

The Heron Streamlet API supports four types of joins:

Type | What the join operation yields | Default?
:----|:-------------------------------|:--------
[Inner](#inner-joins) | All key-values with matched keys across the left and right stream | Yes
[Outer left](#outer-left-joins) | All key-values with matched keys across both streams plus unmatched keys in the left stream |
[Outer right](#outer-right-joins) | All key-values with matched keys across both streams plus unmatched keys in the left stream |
[Outer](#outer-joins) | All key-values across both the left and right stream, regardless of whether or not any given element has a matching key in the other stream |

#### Inner joins

Inner joins operate over the [Cartesian product](https://en.wikipedia.org/wiki/Cartesian_product) of the left stream and the right stream, i.e. over all the whole set of all ordered pairs between the two streams. Imagine this set of key-value pairs accumulated within a time window:

Left streamlet | Right streamlet
:--------------|:---------------
("player1", 4) | ("player1", 10)
("player1", 5) | ("player1", 12)
("player1", 17) | ("player2", 27)

An inner join operation would thus apply the join function to all key-values with matching keys, thus **3 &times; 2 = 6** in total, producing this set of key-values:

Included key-values |
:-------------------|
("player1", 4) |
("player1", 5) |
("player1", 10) |
("player1", 12) |
("player1", 17) |

> Note that the `("player2", 27)` key-value pair was *not* included in the stream because there's no matching key-value in the left streamlet.

If the supplied join function, say, added the values together, then the resulting joined stream would look like this:

Operation | Joined Streamlet
:---------|:----------------
4 + 10 | ("player1", 14)
4 + 12 | ("player1", 16)
5 + 10 | ("player1", 15)
5 + 12 | ("player1", 17)
17 + 10 | ("player1", 27)
17 + 12 | ("player1", 29)

> Inner joins are the "default" join type in the Heron Streamlet API. If you call the `join` method without specifying a join type, an inner join will be applied.

##### Java example

```java
class Score {
    String playerUsername;
    int playerScore;

    // Setters and getters
}

Streamlet<Score> scores1 = /* A stream of player scores */;
Streamlet<Score> scores2 = /* A second stream of player scores */;

scores1
    .join(
        scores2,
        // Key extractor for the left stream (scores1)
        score -> score.getPlayerUsername(),
        // Key extractor for the right stream (scores2)
        score -> score.getPlayerScore(),
        // Window configuration
        WindowConfig.TumblingCountWindow(50),
        // Join function (selects the larger score as the value using
        // using a ternary operator)
        (x, y) ->
            (x.getPlayerScore() >= y.getPlayerScore()) ?
                x.getPlayerScore() :
                y.getPlayerScore()
    )
    .log();
```

In this example, two streamlets consisting of `Score` objects are joined. In the `join` function, a key and value extractor are supplied along with a window configuration and a join function. The resulting, joined streamlet will consist of key-value pairs in which each player's username will be the key and the joined---in this case highest---score will be the value.

By default, an [inner join](#inner-joins) is applied in join operations but you can also specify a different join type. Here's a Java example for an [outer right](#outer-right-joins) join:

```java
import org.apache.heron.streamlet.JoinType;

scores1
    .join(
        scores2,
        // Key extractor for the left stream (scores1)
        score -> score.getPlayerUsername(),
        // Key extractor for the right stream (scores2)
        score -> score.getPlayerScore(),
        // Window configuration
        WindowConfig.TumblingCountWindow(50),
        // Join type
        JoinType.OUTER_RIGHT,
        // Join function (selects the larger score as the value using
        // using a ternary operator)
        (x, y) ->
            (x.getPlayerScore() >= y.getPlayerScore()) ?
                x.getPlayerScore() :
                y.getPlayerScore()
    )
    .log();
```

#### Outer left joins

An outer left join includes the results of an [inner join](#inner-joins) *plus* all of the unmatched keys in the left stream. Take this example left and right streamlet:

Left streamlet | Right streamlet
:--------------|:---------------
("player1", 4) | ("player1", 10)
("player2", 5) | ("player4", 12)
("player3", 17) |

The resulting set of key-values within the time window:

Included key-values |
:-------------------|
("player1", 4) |
("player1", 10) |
("player2", 5) |
("player3", 17) |

In this case, key-values with a key of `player4` are excluded because they are in the right stream but have no matching key with any element in the left stream.

#### Outer right joins

An outer right join includes the results of an [inner join](#inner-joins) *plus* all of the unmatched keys in the right stream. Take this example left and right streamlet (from [above](#outer-left-joins)):

Left streamlet | Right streamlet
:--------------|:---------------
("player1", 4) | ("player1", 10)
("player2", 5) | ("player4", 12)
("player3", 17) |

The resulting set of key-values within the time window:

Included key-values |
:-------------------|
("player1", 4) |
("player1", 10) |
("player2", 5) |
("player4", 17) |

In this case, key-values with a key of `player3` are excluded because they are in the left stream but have no matching key with any element in the right stream.

#### Outer joins

Outer joins include *all* key-values across both the left and right stream, regardless of whether or not any given element has a matching key in the other stream. If you want to ensure that no element is left out of a resulting joined streamlet, use an outer join. Take this example left and right streamlet (from [above](#outer-left-joins)):

Left streamlet | Right streamlet
:--------------|:---------------
("player1", 4) | ("player1", 10)
("player2", 5) | ("player4", 12)
("player3", 17) |

The resulting set of key-values within the time window:

Included key-values |
:-------------------|
("player1", 4)
("player1", 10)
("player2", 5)
("player4", 12)
("player3", 17)

> Note that *all* key-values were indiscriminately included in the joined set.

### Sink operations

In processing graphs like the ones you build using the Heron Streamlet API, **sinks** are essentially the terminal points in your graph, where your processing logic comes to an end. A processing graph can end with writing to a database, publishing to a topic in a pub-sub messaging system, and so on. With the Streamlet API, you can implement your own custom sinks.

#### Java example

```java
import org.apache.heron.streamlet.Context;
import org.apache.heron.streamlet.Sink;

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
Builder builder = Builder.newBuilder();

builder.newSource(() -> "Here is a string to be passed to the sink")
        .toSink(new FormattedLogSink());
```

> [Log operations](#log-operations) rely on a log sink that is provided out of the box. You'll need to implement other sinks yourself.

### Consume operations

Consume operations are like [sink operations](#sink-operations) except they don't require implementing a full sink interface. Consume operations are thus suited for simple operations like formatted logging.

#### Java example

```java
Builder builder = Builder.newBuilder()
        .newSource(() -> generateRandomInteger())
        .filter(i -> i % 2 == 0)
        .consume(i -> {
            String message = String.format("Even number found: %d", i);
            System.out.println(message);
        });
```

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
Builder builder = Builder.newBuilder();

Streamlet<Integer> zeroes = builder.newSource(() -> 0)
        .setName("zeroes");

builder.newSource(() -> ThreadLocalRandom.current().nextInt(1, 11))
        .setName("random-ints")
        .setNumPartitions(3)
        .map(i -> i + 1)
        .setName("add-one")
        .repartition(3)
        .union(zeroes)
        .setName("unify-streams")
        .repartition(2)
        .filter(i -> i != 2)
        .setName("remove-all-twos")
        .repartition(1)
        .log();
```

### Repartition operations

As explained [above](#partitioning), when you set a number of partitions for a specific operation (included for source streamlets), the same number of partitions is applied to all downstream operations *until* a different number is explicitly set.

```java
import java.util.Arrays;

Builder builder = Builder.newBuilder();

builder.newSource(() -> ThreadLocalRandom.current().nextInt(1, 11))
    .repartition(4, (element, numPartitions) -> {
        if (element > 5) {
            return Arrays.asList(0, 1);
        } else {
            return Arrays.asList(2, 3);
        }
    });
```

