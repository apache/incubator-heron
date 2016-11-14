---
title: Writing and Launching a Topology in Java
---

A topology specifies components like spouts and bolts, as well as the relation
between components and proper configurations.


### Install Heron APIs for development

Before getting started writing a topology, you need to install the Heron API
and import its library into your own topology project.

* Go to the [releases page](https://github.com/twitter/heron/releases)
for Heron and download the Heron API installation script for your platform.
The name of the script for Mac OS X (`darwin`), for example, would be
`heron-api-install-{{% heronVersion %}}-darwin.sh`.

* Once you've downloaded, run it with the `--user` flag set.

* After successful installation, import `~/.heronapi/heron-storm.jar` into
your project as a dependency. This allows you to use the Heron APIs that
are necessary to develop your own topology.

### Maven Integration

Alternatively, you can integrate the latest Heron API by including
the following lines in your project's `pom.xml` file.

```xml
<dependency>
  <groupId>com.twitter.heron</groupId>
  <artifactId>heron-storm</artifactId>
  <version>{{% heronVersion %}}</version>
</dependency>
```

### Writing your own topology

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

See the [`ExclamationTopology`](https://github.com/twitter/heron/blob/master/heron/examples/src/java/com/twitter/heron/examples/ExclamationTopology.java) for the complete example. More examples can be found in the  [`examples package`](https://github.com/twitter/heron/tree/master/heron/examples/src/java/com/twitter/heron/examples).
