# Building Topologies

Heron was built to be fully backwards compatible with [Apache
Storm](http://storm.apache.org)'s [topology
API](http://storm.apache.org/tutorial.html).

For a more conceptual discussion, see [Heron
Topologies](../concepts/topologies.html).

## Storm Compatibility

Although Heron is fully backwards compatible with Storm's topology API, there
are a few differences between the systems that you should be aware of. Most
importantly, Heron has a different set of configurable parameters, which is
documented in the [Configuration](#configuration) section below.

Heron also has a slightly different API for running topologies in local mode,
which is documented in the [Local Mode](#local-mode) section below.

## Configuration

The most fundamental difference between Heron and Storm topologies lies in their
configuration. Storm had a system-specific
[`Config`](http://storm.apache.org/apidocs/backtype/storm/Config) class that was
used to configure topologies. Here's an example:

```java
import backtype.storm.Config;

Config conf = new Config();
conf.setNumWorkers(4);
conf.setDebug(true);
```

There is an analogous
[`Config`](../api/topology/com/twitter/heron/api/Config.html) for Heron that
functions in the same way but offers a different set of configurable parameters.
If there are Heron-specific parameters not available in Storm that you'd like to
use, you should use Heron's `Config` class instead of Storm's.

If you do choose to modify your Storm topology to user Heron's configuration,
you'll also need to use Heron's
[`HeronSubmitter`](../api/topology/com/twitter/heron/api/HeronSubmitter.html)
class to submit your topology rather than Storm's
[`StormSubmitter`](http://storm.apache.org/apidocs/backtype/storm/StormSubmitter)
class.

Here's an example using the `StormSubmitter`:

```java
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.StormSubmitter;

TopologyBuilder topologyBuilder = new TopologyBuilder();
// Build the topology
StormSubmitter.submitTopology("my-topology", conf, topologyBuilder.createTopology();
```

Here's the same topology submitted using the `HeronSubmitter` instead:

```java
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.topology.TopologyBuilder;

TopologyBuilder topologyBuilder = new TopologyBuilder();
// Build the topology
StormSubmitter.submitTopology("my-topology", conf, topologyBuilder.createTopology());
```

## Managing Topologies

Documentation on managing custom topologies in your Heron cluster using the
`heron-cli` tool (including submitting, starting, and killing topologies), see
[Managing Topologies](../operators/heron-cli.html).

## Bolts

Example Heron bolts:

* [`ExclamationBolt`]({{book.root_url}}/heron/examples/src/java/com/twitter/heron/examples/AckingTopology.java#L61)
* [`CountBolt`]({{book.root_url}}/heron/examples/src/java/com/twitter/heron/examples/TaskHookTopology.java#L179)


## Spouts

Example Heron spouts:

* [`AckingTestWordSpout`]({{book.root_url}}/heron/examples/src/java/com/twitter/heron/examples/AckingTopology.java#L25)
* [`TestWordSpout`]({{book.root_url}}/heron/examples/src/java/com/twitter/heron/examples/TestWordSpout.java)

## Task Hooks



[](http://storm.apache.org/documentation/Hooks.html)

## Serialization

## Heron's Data Model

For more on Heron's tuple-driven data model, see [this
document](data-model.html).

## Processing Semantics

## Common Patterns

[](http://storm.apache.org/documentation/Common-patterns.html)

## Local Mode

[Storm
documentation](http://storm.apache.org/tutorial#running-exclamationtopology-in-local-mode)

## Advanced Topics

* [Transactional Topologies using Trident](http://storm.apache.org/documentation/Trident-tutorial.html)
