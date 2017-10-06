---
title: Effectively-once Java topologies
---

{{< alert "storm-api" >}}

You can create Heron topologies that have [effectively-once](../../../concepts/delivery-semantics#stateful-topologies) semantics by doing two things:

1. Set the [delivery semantics](#specifying-delivery-semantics) of the topology to `EFFECTIVELY_ONCE`.
2. Create topology processing logic in which each component (i.e. each spout and bolt) implements the [`IStatefulComponent`](/api/java/com/twitter/heron/api/topology/IStatefulComponent.html) interface.

## Specifying delivery semantics

You can specify the [delivery semantics](../../../concepts/delivery-semantics) of a Heron topology via configuration. To apply effectively-once semantics to a topology:

```java
import com.twitter.heron.api.Config;

Config topologyConfig = new Config();
topologyConfig.setTopologyReliabilityMode(Config.TopologyReliabilityMode.ATLEAST_ONCE);
```

The other possible values for the `TopologyReliabilityMode` enum are `ATMOST_ONCE` and `EFFECTIVELY_ONCE`.

> Instead of "delivery semantics" terminology, the original Topology API for Heron uses "reliability mode" terminology. In spite of the terminological difference, the two sets of terms are synonymous.

## Stateful components

Stateful spouts and bolts need to implement the [`IStatefulComponent`](/api/java/com/twitter/heron/api/topology/IStatefulComponent.html) interface, which requires implementing five methods (all of which are `void` methods):

Method | Input | Description
:------|:------|:-----------
`preSave` | Checkpoint ID (`String`)| The action taken immediately prior to the component's state being saved. 
`initState` | Initial state ([`State<K, V>`](/api/java/com/twitter/heron/examples/api/StatefulWordCountTopology.ConsumerBolt.html#initState-com.twitter.heron.api.state.State-)) | Initializes the state of the function or operator to that of a previous checkpoint.
`declareOutputFields` | [`OutputFieldsDeclarer`](/api/java/com/twitter/heron/api/topology/OutputFieldsDeclarer.html) | Declares the output fields for the spout or bolt (if any)
`execute` | Data input ([`Tuple`](/api/java/com/twitter/heron/api/tuple/Tuple.html)) | The tuple processing logic performed by the component

> Remember that stateful components automatically handle all state storage in the background using a State Manager (the currently available State Managers are [ZooKeeper](../../../operators/deployment/statemanagers/zookeeper) and the [local filesystem](../../../operators/deployment/statemanagers/localfs). You don't need to, for example, save state to an external database.

## The `State` class

Heron topologies with effectively-once semantics need to be stateful topologies (you can also create stateful topologies with at-least-once or at-most-once semantics). All state in stateful topologies is handled through a [`State`](/api/java/com/twitter/heron/api/state/State.html) class which has the same semantics as a standard Java [`Map`](https://docs.oracle.com/javase/8/docs/api/java/util/Map.html), and so it includes methods like `get`, `set`, `put`, `putIfAbsent`, `keySet`, `compute`, `forEach`, `merge`, and so on.

## Example effectively-once topology

In the sections below, we'll build a stateful topology with effectively-once semantics from scratch. The topology will work like this:

* A [`RandomIntSpout`](#example-stateful-spout) will continuously emit random integers between 1 and 100
* An [`AdditionBolt`](#example-stateful-bolt) will receive those random numbers and add each number to a running sum. When the sum reaches 1,000,000, it will go back to zero.

### Example stateful spout

The `RandomIntSpout` shown below continuously emits a never-ending series of random integers between 1 and 100 in the `random-int` field.

> It's important to note that *all* components in stateful topologies must be stateful (i.e. implement the `IStatefulComponent` interface) for the topology to provide effectively-once semantics. That includes spouts, even simple ones like the spout in this example.

```java
import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.state.State;
import com.twitter.heron.api.topology.IStatefulComponent;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Values;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class RandomIntSpout extends BaseRichSpout implements IStatefulComponent<String, Integer> {
    private SpoutOutputCollector spoutOutputCollector;
    private State<String, Integer> count;

    public RandomIntSpout() {
    }

    // Generates a random integer between 1 and 100
    private int randomInt() {
        return ThreadLocalRandom.current().nextInt(1, 101);
    }

    // These two methods are required to implement the IStatefulComponent interface
    @Override
    public void preSave(String checkpointId) {
        System.out.println(String.format("Saving spout state at checkpoint %s", checkpointId));
    }

    @Override
    public void initState(State<String, Integer> state) {
        count = state;
    }

    // These three methods are required to extend the BaseRichSpout abstract class
    @Override
    public void open(Map<String, Object> map, TopologyContext ctx, SpoutOutputCollector collector) {
        spoutOutputCollector = collector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("random-int"));
    }

    @Override
    public void nextTuple() {
        int randomInt = randomInt();
        collector.emit(new Values(randomInt));
    }
}
```

A few things to note in this spout:

* All state is handled by the `count` variable, which is of type `State<String, Integer>`. In that state object, the key is always `count`, while the value is the current sum.
* This is a very simple topology, so the `preSave` method simply logs the current checkpoint ID. This method could be used in a variety of more complex ways.
* The `initState` method simply accepts the current state as-is. This method can be used for a wide variety of purposes, for example deserializing the `State` object to a user-defined type.
* Only one field will be declared: the `random-int` field.

### Example stateful bolt

The `AdditionBolt`

```java
import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.state.State;
import com.twitter.heron.api.topology.IStatefulComponent;
import com.twitter.heron.api.topology.TopologyContext;

import java.util.Map;

public class AdditionBolt extends BaseRichBolt implements IStatefulComponent<String, Integer> {
    private OutputCollector outputCollector;

    public AdditionBolt() {
    }

    // These two methods are required to implement the IStatefulComponent interface
    @Override
    public void preSave(String checkpointId) {
        System.out.println(String.format("Saving spout state at checkpoint %s", checkpointId));
    }

    // These three methods are required to extend the BaseRichSpout abstract class
    @Override
    public void prepare(Map<String, Object>, TopologyContext ctx, OutputCollector collector) {
        outputCollector = collector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // This bolt has no output fields, so none will be declared
    }

    @Override
    public void execute(Tuple tuple) {
        // Extract the incoming random integer from the arriving tuple
        int incomingRandomInt = tuple.getInt(tuple.fieldIndex("random-int"));

        // Get the current sum from the count object, defaulting to zero in case
        // this is the first processing operation.
        int currentSum = count.getOrDefault("count", 0);

        int newSum = incomingValue + currentSum;

        // Reset the sum to zero if it exceeds 1,000,000
        if (newSum > 10^6) {
            newSum = 0;
        }

        // Update the count state
        count.put("count", newSum);
    }
}
```

A few things to notice in this bolt:

### Putting the topology together

Now that we have a stateful spout and bolt in place, we can build and configure the topology.

```java
import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.exception.AlreadyAliveException;
import com.twitter.heron.api.exception.InvalidTopologyException;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.api.tuple.Fields;

public class EffectivelyOnceTopology {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        Config topologyConfig = new Config();

        // Apply effectively-once semantics and set the checkpoint interval to 10 seconds
        topologyConfig.setTopologyReliabilityMode(Config.TopologyReliabilityMode.EFFECTIVELY_ONCE);
        topologyConfig.setTopologyStatefulCheckpointIntervalSecs(10);

        // Build the topology out of the example spout and bolt
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("random-int-spout", new RandomIntSpout());
        topologyBuilder.setBolt("addition-bolt", new AdditionBolt())
                .fieldsGrouping("random-int-spout", new Fields("random-int"));

        HeronSubmitter.submitTopology(args[0], config, topologyBuilder.buildTopology());
    }
}
```

### Submitting the topology

```bash
$ mvn assembly:assembly
$ heron submit local \
  target/example-effectively-once-topology-0.1.0-jar-with-dependencies.jar \
  com.example.topologies.EffectivelyOnceTopology \
  RunningSumTopology
```