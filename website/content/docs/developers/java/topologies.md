---
title: Writing and Launching Topologies in Java
---

{{< alert "storm-api" >}}

A topology specifies components like [spouts](../spouts) and [bolts](../bolts), as well as the relation
between components and proper configurations. The
[`heron-api`](http://search.maven.org/#search%7Cgav%7C1%7Cg%3A%22com.twitter.heron%22%20AND%20a%3A%22heron-api%22)
enables you to create topology logic in Java.

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

Heron [topologies](../../../concepts/topologies)

[Spouts](../spouts) and [Bolts](../bolts) discuss how to implement a
spouts and bolts, respectively.

After defining the spouts and bolts, a topology can be composed using
[`TopologyBuilder`](/api/com/twitter/heron/api/topology/TopologyBuilder.html). The
`TopologyBuilder` has two major methods to specify the components:

* `setBolt(String id, IRichBolt bolt, Number parallelismHint)`: `id` is the
unique identifier that assigned to a bolt, `bolt` is the one previously
composed, and `parallelismHint` is a number that specifying the number of
instances of this bolt.

* `setSpout(String id, IRichSpout spout, Number parallelismHint)`: `id` is the
unique identifier that assigned to a spout, `spout` is the one previously
composed, and `parallelismHint` is a number that specifying the number of
instances of this spout.

A simple example is as follows:

```java

TopologyBuilder builder = new TopologyBuilder();
builder.setSpout("word", new TestWordSpout(), 5);
builder.setBolt("exclaim", new ExclamationBolt(), 4);

```

In addition to the component specification, how to transmit Tuples between the
components must also be specified. This is defined by different
grouping strategies:

* Fields Grouping: Tuples are transmitted to bolts based on a given field. Tuples
with the same field will always go to the same bolt.
* Global Grouping: All the Tuples are transmitted to a single instance of a bolt
with the lowest task id.
* Shuffle Grouping: Tuples are randomly transmitted to different instances of
a bolt.
* None Grouping: Currently, it equals to shuffle grouping.
* All Grouping: All Tuples are transmitted to all instances of a bolt.
* Custom Grouping: User-defined grouping strategy.

The following snippet is a simple example of specifying shuffle grouping
between our `word` spout and `exclaim` bolt.

```java

builder.setBolt("exclaim", new ExclamationBolt(), 4)
  .shuffleGrouping("word");

```

Once the components and the grouping are specified, the topology can be built.

```java
HeronTopology topology = builder.createTopology();
```

See the [`ExclamationTopology`](https://github.com/twitter/heron/blob/master/examples/src/java/com/twitter/heron/examples/ExclamationTopology.java) for the complete example. More examples can be found in the  [`examples package`](https://github.com/twitter/heron/tree/master/examples/src/java/com/twitter/heron/examples).
