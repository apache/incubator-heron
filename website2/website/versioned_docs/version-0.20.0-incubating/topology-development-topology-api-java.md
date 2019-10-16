---
id: version-0.20.0-incubating-topology-development-topology-api-java
title: The Heron Topology API for Java
sidebar_label: The Heron Topology API for Java
original_id: topology-development-topology-api-java
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

> This document pertains to the older, Storm-based, Heron Topology API.  Heron now offers two separate APIs for building topologies: the original, [Storm](https://storm.apache.org)-based Topology API, and the newer [Streamlet API](../../../concepts/topologies#the-heron-streamlet-api). Topologies created using the Topology API can still run on Heron and there are currently no plans to deprecate this API. We would, however, recommend that you use the Streamlet API for future work.


A topology specifies components like spouts and bolts, as well as the relation
between components and proper configurations. The
[`heron-api`](http://search.maven.org/#search%7Cgav%7C1%7Cg%3A%22org.apache.heron%22%20AND%20a%3A%22heron-api%22)
enables you to create topology logic in Java.

> If you're interested in creating stateful topologies with [effectively-once
> semantics](heron-delivery-semantics) in Java, see [this new
> guide](guides-effectively-once-java-topologies).

## Getting started

In order to use the Heron API for Java, you'll need to install the `heron-api` library, which is available
via [Maven Central](http://search.maven.org/).

### Maven setup

To install the `heron-api` library using Maven, add this to the `dependencies` block of your `pom.xml`
configuration file:

```xml
<dependency>
    <groupId>org.apache.heron</groupId>
    <artifactId>heron-api</artifactId>
    <version>{{heron:version}}</version>
</dependency>
```

#### Compiling a JAR with dependencies

In order to run a Java topology in a Heron cluster, you'll need to package your topology as a "fat" JAR with dependencies included. You can use the [Maven Assembly Plugin](https://maven.apache.org/plugins/maven-assembly-plugin/usage.html) to generate JARs with dependencies. To install the plugin and add a Maven goal for a single JAR, add this to the `plugins` block in your `pom.xml`:

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

### Writing your topology logic

Heron [topologies](heron-topology-concpets) are processing graphs consisting
of spouts that ingest data and bolts that process that data.

> **Don't want to manually create spouts and bolts? Try the Heron Streamlet API.**  If you find manually creating and connecting spouts and bolts to be overly cumbersome, we recommend trying out the [Heron Streamlet API](topology-development-streamlet-api-java) for Java, which enables you to create your topology logic using a highly streamlined logic inspired by functional programming concepts.

Once you've defined the spouts and bolts, a topology can be composed using a
[`TopologyBuilder`](/api/org/apache/heron/api/topology/TopologyBuilder.html). The
`TopologyBuilder` has two major methods used to specify topology components:

Method | Description
:------|:-----------
`setBolt(String id, IRichBolt bolt, Number parallelismHint)` | `id` is the unique identifier that assigned to a bolt, `bolt` is the one previously composed, and `parallelismHint` is a number that specifies the number of instances of this bolt.
`setSpout(String id, IRichSpout spout, Number parallelismHint)` | `id` is the unique identifier that assigned to a spout, `spout` is the one previously composed, and `parallelismHint` is a number that specifying the number of instances of this spout.

Here's a simple example:

```java

TopologyBuilder builder = new TopologyBuilder();
builder.setSpout("word", new TestWordSpout(), 5);
builder.setBolt("exclaim", new ExclamationBolt(), 4);
```

In addition to the component specification, you also need to specify how tuples
will be routed between your topology components. There are a few different grouping
strategies available:

Grouping strategy | Description
:-----------------|:-----------
Fields grouping | Tuples are transmitted to bolts based on a given field. Tuples with the same field will always go to the same bolt.
Global grouping | All tuples are transmitted to a single instance of a bolt with the lowest task id.
Shuffle Grouping | Tuples are randomly transmitted to different instances of a bolt.
None grouping | Currently, this is the same as shuffle grouping.
All grouping | All tuples are transmitted to all instances of a bolt.
Custom grouping | User-defined grouping strategy.

The following snippet is a simple example of specifying shuffle grouping
between a `word` spout and an `exclaim` bolt.

```java

builder.setBolt("exclaim", new ExclamationBolt(), 4)
  .shuffleGrouping("word");
```

Once the components and the grouping are specified, the topology can be built.

```java
HeronTopology topology = builder.createTopology();
```

See the [`ExclamationTopology`](https://github.com/apache/incubator-heron/blob/master/examples/src/java/org/apache/heron/examples/api/ExclamationTopology.java) for the complete example. More examples can be found in the  [`examples package`](https://github.com/apache/incubator-heron/tree/master/examples/src/java/org/apache/heron/examples).

## Spouts

A Heron **spout** is a source of streams, responsible for emitting
[tuples](../../developers/data-model) into the topology. A spout may, for
example, read data from a Kestrel queue or read tweets from the Twitter API and
emit tuples to one or more bolts.

Information on building spouts can be found in [Building
Spouts](../../developers/java/spouts).

### Implementing a Spout

Spouts must implement the [`ISpout`](/api/org/apache/heron/api/spout/ISpout.html) interface.

```java
public interface ISpout extends Serializable {
  void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector);
  void close();
  void activate();
  void deactivate();
  void nextTuple();
  void ack(Object msgId);
  void fail(Object msgId);
}
```

* The `open` method is called when the spout is initialized and provides the
spout with the executing environment.

* The `close` method is called when the spout is shutdown. There's no guarantee
that this method is called due to how the instance is killed.

* The `activate` method is called when the spout is asked to back into active
state.

* The `deactivate` method is called when the spout is asked to enter deactive
state.

* The `nextTuple` method is used to fetch tuples from input source and emit it
to [`OutputCollector`](/api/org/apache/heron/api/bolt/).

* The `ack` method is called when the `Tuple` with the `msgId` emitted by this
spout is successfully processed.

* The `fail` method is called when the `Tuple` with the `msgId` emitted by this
spout is not processed successfully.

See [`TestWordSpout`](https://github.com/apache/incubator-heron/blob/master/examples/src/java/org/apache/heron/examples/api/spout/TestWordSpout.java) for a simple spout example.

Instead of implementing the [`ISpout`](/api/org/apache/heron/api/spout/ISpout.html) interface directly, you can also implement [`IRichSpout`](/api/org/apache/heron/api/spout/IRichSpout.html).


## Bolts

A Heron **bolt** consumes streams of
[tuples](guides-data-model) emitted by spouts and performs some
set of user-defined processing operations on those tuples, which may include
performing complex stream transformations, performing storage operations,
aggregating multiple streams into one, emitting tuples to other bolts within the
topology, and much more.

### Implementing a Bolt


Spouts must implement the [`ISpout`](/api/org/apache/heron/api/spout/ISpout.html) interface.

```java
public interface ISpout extends Serializable {
  void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector);
  void close();
  void activate();
  void deactivate();
  void nextTuple();
  void ack(Object msgId);
  void fail(Object msgId);
}
```

* The `open` method is called when the spout is initialized and provides the
spout with the executing environment.

* The `close` method is called when the spout is shutdown. There's no guarantee
that this method is called due to how the instance is killed.

* The `activate` method is called when the spout is asked to back into active
state.

* The `deactivate` method is called when the spout is asked to enter deactive
state.

* The `nextTuple` method is used to fetch tuples from input source and emit it
to [`OutputCollector`](/api/org/apache/heron/api/bolt/).

* The `ack` method is called when the `Tuple` with the `msgId` emitted by this
spout is successfully processed.

* The `fail` method is called when the `Tuple` with the `msgId` emitted by this
spout is not processed successfully.

See [`TestWordSpout`](https://github.com/apache/incubator-heron/blob/master/examples/src/java/org/apache/heron/examples/api/spout/TestWordSpout.java) for a simple spout example.

Instead of implementing the [`ISpout`](/api/org/apache/heron/api/spout/ISpout.html) interface directly, you can also implement [`IRichSpout`](/api/org/apache/heron/api/spout/IRichSpout.html).

## Applying delivery semantics to topologies

```java
import org.apache.heron.api.Config;

Config topologyConfig = new Config();

config.setTopologyReliabilityMode(Config.TopologyReliabilityMode.EFFECTIVELY_ONCE);
```

There are three delivery semantics available corresponding to the three delivery semantics that Heron provides:

* `ATMOST_ONCE`
* `ATLEAST_ONCE`
* `EFFECTIVELY_ONCE`

## Acking

In distributed systems, an **ack** (short for "acknowledgment") is a message that confirms that some action has been taken. In Heron, you can create [bolts](#acking-bolts) that emit acks when some desired operation has occurred (for example data has been successfully stored in a database or a message has been successfully produced on a topic in a pub-sub messaging system). Those acks can then be received and acted upon by upstream [spouts](#ack-receiving-spouts).

> You can see acking at work in a complete Heron topology in [this topology](https://github.com/apache/incubator-heron/blob/master/examples/src/java/org/apache/heron/examples/api/AckingTopology.java).

Whereas acking a tuple indicates that some operation has succeeded, the opposite can be indicated when a bolt [fails](#failing) a tuple.

### Acking bolts

Each Heron bolt has an `OutputCollector` that can ack tuples using the `ack` method. Tuples can be acked inside the `execute` method that each bolt uses to process incoming tuples. *When* a bolt acks tuples is up to you. Tuples can be acked immediately upon receipt, after data has been saved to a database, after a message has been successfully published to a pub-sub topic, etc.

Here's an example of a bolt that acks tuples when they're successfully processed:

```java
import org.apache.heron.api.bolt.BaseRichBolt;
import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.topology.TopologyContext;

public class AckingBolt extends BaseRichBolt {
    private OutputCollector outputCollector;

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
    }

    private void applyProcessingOperation(Tuple tuple) throws Exception {
        // Some processing logic for each tuple received by the bolt
    }

    public void execute(Tuple tuple) {
        try {
            applyProcessingOperation(tuple);
            outputCollector.ack(tuple);
        } catch (Exception e) {
            outputCollector.fail(tuple);
        }
    }
}
```

In this bolt, there's an `applyProcessingOperation` function that processes each incoming tuple. One of two things can result from this function:

1. The operation succeeds, in which case the bolt sends an ack. Any upstream spouts---such as a spout like the `AckReceivingSpout` below---would then receive that ack, along with the message ID that the bolt provides.
1. The operation fails and throws an exception, in which case the tuple is failed rather than acked.

### Ack-receiving spouts

Heron spouts don't emit acks, but they can receive acks when downstream bolts have acked a tuple. In order to receive an ack from downstream bolts, spouts need to do two things:

1. [Specify](#specifying-a-message-id) a message ID when they emit tuples using the `nextTuple` method
1. [Implement](#specifying-ack-reception-logic) an `ack` function that specifies what will happen when an ack is received from downstream bolts

### Specifying a message ID

If you want a spout to receive acks from downstream bolts, the spout needs to specify a message ID every time the spout's `SpoutOutputCollector` emits a tuple to downstream bolts. Here's an example:

```java
import org.apache.heron.api.spout.BaseRichSpout;

public class AckReceivingSpout extends BaseRichSpout {
    private Object generateMessageId() {
        // Some logic to produce a unique ID
    }

    public void nextTuple() {
        collector.emit(new Values(someValue), generateMessageId());
    }
}
```

In this example, each tuple emitted by the spout includes a unique message ID. If no ID is specified, as in the example below, then the spout simply *will not receive acks*:

```java
public class NoAckReceivedSpout extends BaseRichSpout {
    public void nextTuple() {
        collector.emit(new Values(someValue));
    }
}
```

> When implementing acking logic---as well as [failing logic](#failing)---each tuple that is acked/failed **must have a unique ID**. Otherwise, the spout receiving the ack will not be able to identify *which* tuple has been acked/failed.

When specifying an ID for the tuple being emitted, the ID is of type `Object`, which means that you can serialize to/deserialize from any data type that you'd like. The message ID could thus be a simple `String` or `long` or something more complex, like a hash, `Map`, or POJO.

### Specifying ack reception logic

In order to specify what your spout does when an ack is received, you need to implement an `ack` function in your spout. That function takes a Java `Object` containing the tuple's ID, which means that you can potentially serialize the message ID to any type you'd like.

In this example, the spout simply logs the message ID:

```java
public class AckReceivingSpout extends BaseRichSpout {
    private Object generateMessageId() {
        // Some logic to produce a unique ID
    }

    public void nextTuple() {
        collector.emit(new Values(someValue), generateMessageId());
    }

    public void ack(Object messageId) {
        // This will simply print the message ID whenever an ack arrives
        System.out.println((String) messageId);
    }
}
```

In this example, the spout performs a series of actions when receiving the ack:

```java
public class AckReceivingSpout extends BaseRichSpout {
    public void nextTuple() {
        if (someCondition) {
            String randomHash = // Generate a random hash as a message ID
            collector.emit(new Values(val), randomHash);
        }
    }

    public void ack(Object messageId) {
        saveItemToDatabase(item);
        publishToPubSubTopic(message);
    }
}
```

### Failing

**Failing** a tuple is essentially the opposite of acking it, i.e. it indicates that some operation has failed. Bolts can fail tuples by calling the `fail` method on the `OutputCollector` rather than `ack`. Here's an example:


```java
public class AckingBolt extends BaseRichBolt {
    public void execute(Tuple tuple) {
        try {
            someProcessingOperation(tuple);
            collector.ack(tuple);
        } catch (Exception e) {
            collector.fail(tuple);
        }
    }
}
```

In this example, an exception-throwing processing operation is attempted. If it succeeds, the tuple is acked; if it fails and an exception is thrown, the tuple is failed.

As with acks, spouts can be set up to handle failed tuples by implementing the `fail` method, which takes the message ID as the argument (just like the `ack` method). Here's an example:

```java
public class AckReceivingSpout extends BaseRichSpout {
    public void nextTuple() {
        collector.emit(new Values(someValue), someMessageId);
    }

    public void fail(Object messageId) {
        // Process the messageId
    }
}
```

As with acking, spouts must include a message ID when emitting tuples or else they will not receive fail messages.

### Acking, failing, and timeouts

If you're setting up your spouts and bolts to include an ack/fail logic, you can specify that a tuple will automatically be failed if a timeout threshold is reached before the tuple is acked. In this example, all tuples passing through all bolts will be failed if not acked within 10 seconds:

```java
import org.apache.heron.api.Config;

Config config = new Config();
config.setMessageTimeoutSecs(10);
```