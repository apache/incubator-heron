---
title: Writing a Topology
---

A topology needs to specify components like spouts and bolts, the relation between components and proper configurations.

[Here](../java/spouts) is about how to implement a spout. And [Here](../java/bolts) is about how to implement a bolt.

After defining the spouts and bolts, we now start to compose the topology using [`TopologyBuilder`](/api/link/to/TopologyBuilder). The `TopologyBuilder` has two major methods to specify the components:

* setBolt(String id, IRichBolt bolt, Number parallelismHint): `id` is the unique identifier that assigned to a bolt, `bolt` is the one we composed previously, and `parallelismHint` is a number that specifying the actual instance number of this bolt.

* setSpout(String id, IRichSpout spout, Number parallelismHint): `id` is the unique identifier that assigned to a spout, `spout` is the one we composed previously, and `parallelismHint` is a number that specifying the actual instance number of this spout.

These two methods are used to specify the components needed in a topology. A simple example is as follows:

```java

TopologyBuilder builder = new TopologyBuilder();
builder.setSpout("word", new TestWordSpout(), 5);
builder.setBolt("exclaim", new ExclamationBolt(), 4);

```

After specifying each component of the topology, a topology also needs to know how to transmit Tuples between the components. This is defined by different grouping strategies:

* FieldsGrouping: Tuples are transmitted to bolts based on given field. And Tuples with the same field will always go to the same bolt.
* GlobalGrouping: All the Tuples are transmitted to a single instance of a bolt.
* ShuffleGrouping: Tuples are randomly transmitted to different instances of a bolt.
* NoneGrouping: User don't care about the grouping strategy.
* AllGrouping: All Tuples are transmitted to all instances of a bolt.
* CustomGrouping: User defined grouping strategy.

The following snippet is a simple example of specifying shuffle grouping between our `word` spout and `exclaim`bolt.

```java

builder.setBolt("exclaim", new ExclamationBolt(), 4)
  .shuffleGrouping("word");

```

After specifying the components and the grouping of the components, we can build the actual topology.

```java
HeronTopology topology = builder.createTopology();
```

See the [`ExclamationTopology`](https://github.com/twitter/heron/blob/master/heron/examples/src/java/com/twitter/heron/examples/ExclamationTopology.java#L39) for the complete example. And you can find more [`Examples`](https://github.com/twitter/heron/tree/master/heron/examples/src/java/com/twitter/heron/examples) for reference.
