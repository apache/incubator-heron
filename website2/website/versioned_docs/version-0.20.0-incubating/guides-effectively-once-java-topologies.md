---
id: version-0.20.0-incubating-guides-effectively-once-java-topologies
title: Effectively Once Java Topologies
sidebar_label: Effectively Once Java Topologies
original_id: guides-effectively-once-java-topologies
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

> **This document pertains to the older, Storm-based, Heron Topology API.** Heron now offers several APIs for building topologies. Topologies created using the Topology API can still run on Heron and there are currently no plans to deprecate this API. We would, however, recommend that you use the Streamlet API for future work.

You can create Heron topologies that have [effectively-once](heron-delivery-semantics#stateful-topologies) semantics by doing two things:

1. Set the [delivery semantics](#specifying-delivery-semantics) of the topology to `EFFECTIVELY_ONCE`.
2. Create topology processing logic in which each component (i.e. each spout and bolt) implements the [`IStatefulComponent`](/api/java/org/apache/heron/api/topology/IStatefulComponent.html) interface.

## Specifying delivery semantics

You can specify the [delivery semantics](heron-delivery-semantics) of a Heron topology via configuration. To apply effectively-once semantics to a topology:

```java
import org.apache.heron.api.Config;

Config topologyConfig = new Config();
topologyConfig.setTopologyReliabilityMode(Config.TopologyReliabilityMode.ATLEAST_ONCE);
```

The other possible values for the `TopologyReliabilityMode` enum are `ATMOST_ONCE` and `EFFECTIVELY_ONCE`.

> Instead of "delivery semantics" terminology, the original Topology API for Heron uses "reliability mode" terminology. In spite of the terminological difference, the two sets of terms are synonymous.

## Stateful components

Stateful spouts and bolts need to implement the [`IStatefulComponent`](/api/java/org/apache/heron/api/topology/IStatefulComponent.html) interface, which requires implementing two methods (both of which are `void` methods):

Method | Input | Description
:------|:------|:-----------
`preSave` | Checkpoint ID (`String`)| The action taken immediately prior to the component's state being saved. 
`initState` | Initial state ([`State<K, V>`](/api/java/org/apache/heron/examples/api/StatefulWordCountTopology.ConsumerBolt.html#initState-org.apache.heron.api.state.State-)) | Initializes the state of the function or operator to that of a previous checkpoint.

> Remember that stateful components automatically handle all state storage in the background using a State Manager (the currently available State Managers are [ZooKeeper](state-managers-zookeeper) and the [local filesystem](state-managers-local-fs). You don't need to, for example, save state to an external database.

## The `State` class

Heron topologies with effectively-once semantics need to be stateful topologies (you can also create stateful topologies with at-least-once or at-most-once semantics). All state in stateful topologies is handled through a [`State`](/api/java/org/apache/heron/api/state/State.html) class which has the same semantics as a standard Java [`Map`](https://docs.oracle.com/javase/8/docs/api/java/util/Map.html), and so it includes methods like `get`, `set`, `put`, `putIfAbsent`, `keySet`, `compute`, `forEach`, `merge`, and so on.

Each stateful spout or bolt must be associated with a single `State` object that handles the state, and that object must also be typed as `State<K, V>`, for example `State<String, Integer>`, `State<long, MyPojo>`, etc. An example usage of the state object can be found in the [example topology](#example-effectively-once-topology) below.

## Example effectively-once topology

In the sections below, we'll build a stateful topology with effectively-once semantics from scratch. The topology will work like this:

* A [`RandomIntSpout`](#example-stateful-spout) will continuously emit random integers between 1 and 100
* An [`AdditionBolt`](#example-stateful-bolt) will receive those random numbers and add each number to a running sum. When the sum reaches 1,000,000, it will go back to zero. The bolt won't emit any data but will simply log the current sum.

> You can see the code for another stateful Heron topology with effectively-once semantics in [this word count example](https://github.com/apache/incubator-heron/blob/master/examples/src/java/org/apache/heron/examples/api/StatefulWordCountTopology.java).

### Example stateful spout

The `RandomIntSpout` shown below continuously emits a never-ending series of random integers between 1 and 100 in the `random-int` field.

> It's important to note that *all* components in stateful topologies must be stateful (i.e. implement the `IStatefulComponent` interface) for the topology to provide effectively-once semantics. That includes spouts, even simple ones like the spout in this example.

```java
import org.apache.heron.api.spout.BaseRichSpout;
import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.state.State;
import org.apache.heron.api.topology.IStatefulComponent;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Values;

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

The `AdditionBolt` takes incoming tuples from the `RandomIntSpout` and adds each integer to produce a running sum. If the sum ever exceeds 1 million, then it resets to zero.

```java
import org.apache.heron.api.bolt.BaseRichBolt;
import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.state.State;
import org.apache.heron.api.topology.IStatefulComponent;
import org.apache.heron.api.topology.TopologyContext;

import java.util.Map;

public class AdditionBolt extends BaseRichBolt implements IStatefulComponent<String, Integer> {
    private OutputCollector outputCollector;
    private State<String, Integer> count;

    public AdditionBolt() {
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
        if (newSum > 1000000) {
            newSum = 0;
        }

        // Update the count state
        count.put("count", newSum);

        System.out.println(String.format("The current saved sum is: %d", newSum));
    }
}
```

A few things to notice in this bolt:

* As in the `RandomIntSpout`, all state is handled by the `count` variable, which is of type `State<String, Integer>`. In that state object, the key is always `count`, while the value is the current sum.
* As in the `RandomIntSpout`, the `preSave` method simply logs the current checkpoint ID.
* The bolt has no output (it simply logs the current stored sum), so no output fields need to be declared.

### Putting the topology together

Now that we have a stateful spout and bolt in place, we can build and configure the topology:

```java
import org.apache.heron.api.Config;
import org.apache.heron.api.HeronSubmitter;
import org.apache.heron.api.exception.AlreadyAliveException;
import org.apache.heron.api.exception.InvalidTopologyException;
import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.api.tuple.Fields;

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

        HeronSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
    }
}
```

### Submitting the topology

The code for this topology can be found in [this GitHub repository](https://github.com/streamlio/heron-java-effectively-once-example). You can clone the repo locally like this:

```bash
$ git clone https://github.com/streamlio/heron-java-effectively-once-example
```

Once you have the repo locally, you can submit the topology to a [running Heron installation](getting-started-local-single-node) like this (if you have [Maven](https://maven.apache.org/) installed):

```bash
$ cd heron-java-effectively-once-example
$ mvn assembly:assembly
$ heron submit local \
  target/effectivelyonce-latest-jar-with-dependencies.jar \
  io.streaml.example.effectivelyonce.RunningSumTopology \
  RunningSumTopology
```

> By default, Heron uses the [local filesystem](state-managers-local-fs) as a State Manager. If you're running Heron locally using the instructions in the [Quick Start Guide](getting-started-local-single-node) then you won't need to change any settings to run this example stateful topology with effectively-once semantics.

From there, you can see the log output for the bolt by running the [Heron Tracker](user-manuals-heron-tracker-runbook) and [Heron UI](user-manuals-heron-ui):

```bash
$ heron-tracker

# In a different terminal window
$ heron-ui
```

> For installation instructions for the Heron Tracker and the Heron UI, see the [Quick Start Guide](../../../getting-getting-started-local-single-node
Once the Heron UI is running, navigate to http://localhost:8889 and click on the `RunningSumTopology` link. You should see something like this in the window that opens up:

![Logical topology drilldown](assets/logical-topology.png)

Click on **addition-bolt** on the right (under **1 Container and 1 Instances**) and then click on the blug **logs** button. You should see log output like this:

```bash
[2017-10-06 13:39:07 -0700] [STDOUT] stdout: The current saved sum is: 0
[2017-10-06 13:39:07 -0700] [STDOUT] stdout: The current saved sum is: 68
[2017-10-06 13:39:07 -0700] [STDOUT] stdout: The current saved sum is: 93
[2017-10-06 13:39:07 -0700] [STDOUT] stdout: The current saved sum is: 117
[2017-10-06 13:39:07 -0700] [STDOUT] stdout: The current saved sum is: 123
[2017-10-06 13:39:07 -0700] [STDOUT] stdout: The current saved sum is: 185
```