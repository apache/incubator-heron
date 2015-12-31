# Building Topologies

Heron was built to be fully backwards compatible with
[Apache Storm](http://storm.apache.org)'s [topology
API](http://storm.apache.org/tutorial.html).

For a more conceptual discussion, see [Heron
Topologies](../concepts/topologies.html).

## Bolts

Example Heron bolts:

* [`ExclamationBolt`]({{book.root_url}}/heron/examples/src/java/com/twitter/heron/examples/AckingTopology.java#L61)
* [`CountBolt`]({{book.root_url}}/heron/examples/src/java/com/twitter/heron/examples/TaskHookTopology.java#L179)


## Spouts

Example Heron spouts:

* [`AckingTestWordSpout`]({{book.root_url}}/heron/examples/src/java/com/twitter/heron/examples/AckingTopology.java#L25)
* [`TestWordSpout`]({{book.root_url}}/heron/examples/src/java/com/twitter/heron/examples/TestWordSpout.java)

## Task Hooks

## Serialization

## Heron's Data Model

For more on Heron's tuple-driven data model, see [this
document](data-model.html).

## Local Mode

[Storm
documentation](http://storm.apache.org/tutorial#running-exclamationtopology-in-local-mode)

