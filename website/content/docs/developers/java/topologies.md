---
title: Writing and Launching Topologies in Java
--- 

{{< alert "storm-api" >}}

A topology specifies components like [spouts](../spouts) and [bolts](../bolts), as well as the relation
between components and proper configurations. The
[`heron-api`](http://search.maven.org/#search%7Cgav%7C1%7Cg%3A%22com.twitter.heron%22%20AND%20a%3A%22heron-api%22)
enables you to create topology logic in Java.

> If you're interested in creating stateful topologies with [effectively-once
> semantics](../../../concepts/delivery-semantics) in Java, see [this new
> guide](../effectively-once).

## Getting started

In order to use the Heron API for Java, you'll need to install the `heron-api` library, which is available
via [Maven Central](http://search.maven.org/).

### Maven setup

To install the `heron-api` library using Maven, add this to the `dependencies` block of your `pom.xml`
configuration file:

```xml
<dependency>
    <groupId>com.twitter.heron</groupId>
    <artifactId>heron-api</artifactId>
    <version>{{< heronVersion >}}</version>
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

Heron [topologies](../../../concepts/topologies) are processing graphs consisting
of [spouts](../spouts) that ingest data and [bolts](../bolts) that process that data.

{{< alert "spouts-and-bolts" >}}

Once you've defined the spouts and bolts, a topology can be composed using a
[`TopologyBuilder`](/api/com/twitter/heron/api/topology/TopologyBuilder.html). The
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

See the [`ExclamationTopology`](https://github.com/twitter/heron/blob/master/examples/src/java/com/twitter/heron/examples/ExclamationTopology.java) for the complete example. More examples can be found in the  [`examples package`](https://github.com/twitter/heron/tree/master/examples/src/java/com/twitter/heron/examples).

## Applying delivery semantics to topologies

```java
import com.twitter.heron.api.Config;

Config topologyConfig = new Config();

config.setTopologyReliabilityMode(Config.TopologyReliabilityMode.EFFECTIVELY_ONCE);
```

There are three delivery semantics available corresponding to the three delivery semantics that Heron provides:

* `ATMOST_ONCE`
* `ATLEAST_ONCE`
* `EFFECTIVELY_ONCE`

## Acking

In distributed systems, an **ack** (short for "acknowledgment") is a message that confirms that some action has been taken. In Heron, you can create [bolts](#acking-bolts) that emit acks when some desired operation has occurred (for example data has been successfully stored in a database or a message has been successfully produced on a topic in a pub-sub messaging system). Those acks can then be received and acted upon by upstream [spouts](#ack-receiving-spouts).

> You can see acking at work in a complete Heron topology in [this topology](https://github.com/twitter/heron/blob/master/examples/src/java/com/twitter/heron/examples/api/AckingTopology.java).

Whereas acking a tuple indicates that some operation has succeeded, the opposite can be indicated when a bolt [fails](#failing) a tuple.

### Acking bolts

Each Heron bolt has an `OutputCollector` that can ack tuples using the `ack` method. Tuples can be acked inside the `execute` method that each bolt uses to process incoming tuples. *When* a bolt acks tuples is up to you. Tuples can be acked immediately upon receipt, after data has been saved to a database, after a message has been successfully published to a pub-sub topic, etc.

Here's an example of a bolt that acks tuples when they're successfully processed:

```java
import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.TopologyContext;

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
import com.twitter.heron.api.spout.BaseRichSpout;

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
import com.twitter.heron.api.Config;

Config config = new Config();
config.setMessageTimeoutSecs(10);
```