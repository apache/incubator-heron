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

## Java Libraries

In order to create topologies using Heron's topology API, you need to import the
[API library](../api/topology/index.html) into your project.

Here's an example for Maven:

<pre><code class="lang-xml">&lt;dependency&gt;
  &lt;groupId>com.twitter.heron&lt;/groupId&gt;
  &lt;artifactId>api&lt;/artifactId&gt;
  &lt;version&gt;{{book.topology_api_version}}&lt;/version&gt;
&lt;/dependency&gt;</code></pre>

Here's an example for Gradle:

<pre><code class="lang-groovy">dependencies {
  compile group: "com.twitter.heron", name: "api", version: "{{book.topology_api_version}}"
}</code></pre>

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

## Streams

Streams are the core abstraction underlying Heron. Streams are initiated by
[spouts](#spouts), which feed unbounded sequences of tuples into a Heron
topology; those unbounded tuple sequences are then processed by any number of
[bolts](#bolts).

## Spouts

Spouts are the information source for Heron topologies. They feed
[streams](#streams) of tuples into a topology from any source you'd like, from a
[Kestrel](https://twitter.github.io/kestrel/) queue to a web service API to a
random list.

Here are some example Heron spouts:

* [`AckingTestWordSpout`]({{book.root_url}}/heron/examples/src/java/com/twitter/heron/examples/AckingTopology.java#L25)
* [`TestWordSpout`]({{book.root_url}}/heron/examples/src/java/com/twitter/heron/examples/TestWordSpout.java)

## Bolts

Bolts are the processing units within a Heron cluster. Bolts take tuples
from streams and perform any processing logic that you'd like on each
tuple. Here are some example Heron bolts:

* [`ExclamationBolt`]({{book.root_url}}/heron/examples/src/java/com/twitter/heron/examples/AckingTopology.java#L61)
* [`CountBolt`]({{book.root_url}}/heron/examples/src/java/com/twitter/heron/examples/TaskHookTopology.java#L179)

## Task Hooks

As in Storm, you can create task hooks in Heron that run custom code when
certain events occur within a topology. For more information, see the [Storm
documentation](http://storm.apache.org/documentation/Hooks.html).

## Serialization

For more on custom serialization for Heron tuples, see [Custom
Serialization](serialization.html).

## Heron's Data Model

For more on Heron's tuple-driven data model, see [this
document](data-model.html).

## Local Mode

As in Storm, in Heron you can run topologies in local mode for the sake of
debugging. For more information, see the [Storm
documentation](http://storm.apache.org/tutorial#running-exclamationtopology-in-local-mode)

## Integrations

A wide variety of extensions have been built for using Storm in
conjunction with other systems. Due to Heron's backwards compatibility
with Storm, you can use those extensions in your Heron topologies as
well. Here are some tutorials for a handful of existing extensions:

* [Kafka](http://storm.apache.org/documentation/storm-kafka.html)
* [HDFS](http://storm.apache.org/documentation/storm-hdfs.html)
* [HBase](http://storm.apache.org/documentation/storm-hbase.html)
* [Hive](http://storm.apache.org/documentation/storm-hive.html)
* [JDBC](http://storm.apache.org/documentation/storm-jdbc.html)
* [Redis](http://storm.apache.org/documentation/storm-redis.html)
* [Solr](http://storm.apache.org/documentation/storm-solr.html)
* [Microsoft Azure Event
  Hubs](http://storm.apache.org/documentation/storm-eventhubs.html)
* [Flux](http://storm.apache.org/documentation/flux.html)
