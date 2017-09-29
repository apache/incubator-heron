---
title: Effectively-once Java topologies
---

{{< alert "storm-api" >}}

You can create Heron topologies with [effectively-once](../../../concepts/delivery-semantics#stateful-topologies) semantics by doing two things:

1. Setting the

## Specifying delivery semantics

You can specify the [delivery semantics](../../../concepts/delivery-semantics) of a Heron topology via configuration. Here's an example:

```java
import com.twitter.heron.api.Config;

Config topologyConfig = new Config();
topologyConfig.setTopologyReliabilityMode(Config.TopologyReliabilityMode.ATLEAST_ONCE);
```

The other possible values of the `TopologyReliabilityMode` enum are `ATMOST_ONCE` and `EFFECTIVELY_ONCE`.

> Instead of "delivery semantics" terminology, the original Topology API for Heron uses "reliability mode" terminology. In spite of the terminological difference, the two sets of terms are synonymous.

## Stateful components

Stateful spouts and bolts need to implement the [`IStatefulComponent`](/api/java/com/twitter/heron/api/topology/IStatefulComponent.html) interface, which requires implementing five methods (all of which return `void`):

Method | Input | Description
:------|:------|:-----------
`preSave` | Checkpoint ID (`String`)| The action taken immediately prior to the component's state being saved. 
`initState` | Initial state ([`State<K, V>`](/api/java/com/twitter/heron/examples/api/StatefulWordCountTopology.ConsumerBolt.html#initState-com.twitter.heron.api.state.State-)) | Initializes the state of the function or operator to that of a previous checkpoint.
`declareOutputFields` | [`OutputFieldsDeclarer`](/api/java/com/twitter/heron/api/topology/OutputFieldsDeclarer.html) | Declares the output fields for the spout or bolt (if any)
`execute` | Data input ([`Tuple`](/api/java/com/twitter/heron/api/tuple/Tuple.html)) | The tuple processing logic performed by the component

### Example stateful spout

```java
import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.state.State;
import com.twitter.heron.api.topology.IStatefulComponent;

public class StatefulSpout extends BaseRichSpout implements IStatefulComponent<String, Integer> {
    @Override
    public void preSave(String checkpointId) {

    }

    @Override
    public void initState(State<String, Integer> state) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        
    }
}
```