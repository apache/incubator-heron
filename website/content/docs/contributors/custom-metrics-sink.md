---
title: Implementing a Custom Metrics Sink
---

Each Heron topology has its own centralized [Metrics
Manager](../../concepts/architecture#metrics-manager) (MM), which collects
metrics from all instances in the topology. You can define how the MM processes
metrics by implementing a **metrics sink**, which specifies how the MM handles
incoming
[`MetricsRecord`](/api/com/twitter/heron/spi/metricsmgr/metrics/MetricsRecord.html)
objects.

Java is currently the only supported language for custom metrics sinks. This may
change in the future.

## Currently-supported Sinks

Heron comes equipped out of the box with three metrics sinks that you can apply
for a specific topology. The code for these sinks may prove helpful for
implementing your own.

* [`GraphiteSink`](/api/metrics/com/twitter/heron/metricsmgr/sink/GraphiteSink.html)
  &mdash; Sends each `MetricsRecord` object to a
  [Graphite](http://graphite.wikidot.com/) instance according to a Graphite
  prefix.
* [`ScribeSink`](/api/metrics/com/twitter/heron/metricsmgr/sink/ScribeSink.html)
  &mdash; Sends each `MetricsRecord` object to a
  [Scribe](https://github.com/facebookarchive/scribe) instance according to a
  Scribe category and namespace.
* [`FileSink`](/api/metrics/com/twitter/heron/metricsmgr/sink/FileSink.html)
  &mdash; Writes each `MetricsRecord` object to a JSON file at a specified path.

More on using those sinks in a Heron cluster can be found in [Metrics
Manager](../../operators/configuration/metrics-manager).

## Java Setup

In order to create a custom metrics sink, you need to import the
`metricsmgr-api` library into your project.

#### Maven

```xml
<dependency>
  <groupId>com.twitter.heron</groupId>
  <artifactId>metricsmgr-api</artifactId>
  <version>{{.Site.Params.versions.metricsapi}}</version>
</dependency>
```

#### Gradle

```groovy
dependencies {
  compile group: "com.twitter.heron", name: "metricsmgr-api", version: "{{.Site.Params.versions.metricsapi}}"
}
```

## The `IMetricsSink` Interface

Each metrics sink must implement the
[`IMetricsSink`](http://heronproject.github.io/metrics-api/com/twitter/heron/metricsmgr/IMetricsSink)
interface, which requires you to implement the following methods:

* `void init(Map<String, Object> conf, SinkContext context)` &mdash; Defines the
  initialization behavior of the sink. The `conf` map is the configuration that
  is passed to the sink by the `.yaml` configuration file at
  `heron/config/metrics_sink.yaml`; the
  [`SinkContext`](/api/com/twitter/heron/spi/metricsmgr/sink/SinkContext.html)
  object enables you to access values from the sink's runtime context
  (the ID of the metrics manager, the ID of the sink, and the name of the
  topology).
* `void processRecord(MetricsRecord record)` &mdash; Defines how each
  `MetricsRecord` that passes through the sink is processed.
* `void flush()` &mdash; Flush any buffered metrics; this function is called at
  the interval specified by the `flush-frequency-ms`. More info can be found in
  the [Stream Manager](../../operators/configuration/stmgr) document.
* `void close()` &mdash; Closes the stream and releases any system resources
  associated with it; if the stream is already closed, invoking `close()` has no
  effect.

Your implementation of those interfaces will need to be packaged into a JAR file
and distributed to the `metrics-mgr-classpath` folder of your [Heron
release](../../developers/compiling).

## Example Implementation

Below is an example implementation that simply prints the contents of each
metrics record as it passes through:

```java
import com.twitter.heron.metricsmgr.api.metrics.MetricsInfo;
import com.twitter.heron.metricsmgr.api.metrics.MetricsRecord;
import com.twitter.heron.metricsmgr.api.sink.IMetricsSink;
import com.twitter.heron.metricsmgr.api.sink.SinkContext;

public class PrintSink implements IMetricsSink {
    @Override
    public void init(Map<String, Object> conf, SinkContext context) {
        System.out.println("Sink configuration:");
        // This will print out each config in the supplied configuration
        for (Map.Entry<String, Object> config : conf.entrySet()) {
            System.out.println(String.format("%s: %s", config.getKey(), config.getValue());
        }
        System.out.println(String.format("Topology name: %s", context.getTopologyName());
        System.out.println(String.format("Sink ID: %s", context.getSinkId()));
    }

    @Override
    public void processRecord(MetricsRecord record) {
        String recordString = String.format("Record received: %s", record.toString());
        System.out.println(recordString);
    }

    @Override
    public void flush() {
        // Since we're just printing to stdout in this sink, we don't need to
        // specify any flush() behavior
    }

    @Override
    public void close() {
        // Since we're just printing to stdout in this sink, we don't need to
        // specify any close() behavior
    }
}
```

## Configuring Your Custom Sink

The configuration for your sink needs to be provided in the
[YAML](http://www.yaml.org/) file at `heron/config/metrics_sinks.yaml`.

At the top of that file there's a `sinks` parameter that lists each available
sink by name. You should add your sink to that list. Here's an example:

```yaml
sinks:
  - file-sink
  - scribe-sink
  - tmaster-sink
  - print-sink
```

For each sink you need to specify the following:

* `class` &mdash; The Java class name of your custom implementation of the
  `IMetricsSink` interface, e.g. `biz.acme.heron.metrics.PrintSink`.
* `flush-frequency-ms` &mdash; The frequency (in milliseconds) at which the
  `flush()` method is called in your implementation of `IMetricsSink`.
* `sink-restart-attempts` &mdash; The number of times that a sink will attempt to
  restart if it throws exceptions and dies. If you do not set this, the default
  is 0; if you set it to -1, the sink will attempt to restart forever.

Below is an example `metrics_sink.yaml` configuration:

```yaml
sinks:
  - print-sink

print-sink:
  class: "biz.acme.heron.metrics.PrintSink"
  flush-frequency-ms: 60000 # One minute
  sink-restart-attempts: -1 # Attempt to restart forever
```

## Using Your Custom Sink

Once you've made a JAR for your custom Java sink, distributed that JAR to
`metrics-mgr-classpath` folder, and changed the configuration in
`heron/config/metrics_sink.yaml`, you'll need to [re-compile
Heron](../../developers/compiling). Any topology submitted using that compiled
version of `heron-cli` will include the custom sink.
