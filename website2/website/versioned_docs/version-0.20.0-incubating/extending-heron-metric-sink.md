---
id: version-0.20.0-incubating-extending-heron-metric-sink
title: Implementing a Custom Metrics Sink
sidebar_label: Custom Metrics Sink
original_id: extending-heron-metric-sink
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

Each Heron container has its own centralized [Metrics
Manager](heron-architecture#metrics-manager) (MM), which collects
metrics from all [Heron Instances](heron-architecture#heron-instance) in
the container. You can define how the MM processes metrics by implementing a
**metrics sink**, which specifies how the MM handles incoming
[`MetricsRecord`](/api/org/apache/heron/spi/metricsmgr/metrics/MetricsRecord.html)
objects.

> Java is currently the only supported language for custom metrics sinks. This may change in the future.

## Currently supported Sinks

Heron comes equipped out of the box with three metrics sinks that you can apply
for a specific topology. The code for these sinks may prove helpful for
implementing your own.

Sink | How it works
:----|:------------
[Prometheus](observability-prometheus) | [`PrometheusSink`](/api/org/apache/heron/metricsmgr/sink/PrometheusSink.html) sends each `MetricsRecord` object to a specified path in the [Prometheus](https://prometheus.io) instance.
[Graphite](observability-graphite) | [`GraphiteSink`](/api/org/apache/heron/metricsmgr/sink/GraphiteSink.html) sends each `MetricsRecord` object to a [Graphite](http://graphite.wikidot.com/) instance according to a Graphite prefix.
[Scribe](observability-scribe) | [`ScribeSink`](/api/org/apache/heron/metricsmgr/sink/ScribeSink.html) sends each `MetricsRecord` object to a [Scribe](https://github.com/facebookarchive/scribe) instance according to a Scribe category and namespace.
Local filesystem | [`FileSink`](/api/org/apache/heron/metricsmgr/sink/FileSink.html) writes each `MetricsRecord` object to a JSON file at a specified path.

## Java Setup

In order to create a custom metrics sink, you need to import the `heron-spi`
library into your project.

#### Maven

```xml
<dependency>
  <groupId>org.apache.heron</groupId>
  <artifactId>heron-spi</artifactId>
  <version>{{% heronVersion %}}</version>
</dependency>
```

#### Gradle

```groovy
dependencies {
  compile group: "org.apache.heron", name: "heron-spi", version: "{{% heronVersion %}}"
}
```

## The `IMetricsSink` Interface

Each metrics sink must implement the
[`IMetricsSink`](/api/org/apache/heron/spi/metricsmgr/sink/IMetricsSink.html)
interface, which requires you to implement the following methods:

Method | Description
:------|:-----------
[`init`](/api/org/apache/heron/spi/metricsmgr/sink/IMetricsSink.html#init-java.util.Map-org.apache.heron.spi.metricsmgr.sink.SinkContext-) | Defines the initialization behavior of the sink. The `conf` map is the configuration that is passed to the sink by the `.yaml` configuration file at `heron/config/metrics_sink.yaml`; the [`SinkContext`](/api/org/apache/heron/spi/metricsmgr/sink/SinkContext.html) object enables you to access values from the sink's runtime context (the ID of the metrics manager, the ID of the sink, and the name of the topology).
[`processRecord`](/api/org/apache/heron/spi/metricsmgr/sink/IMetricsSink.html#processRecord-org.apache.heron.spi.metricsmgr.metrics.MetricsRecord-) | Defines how each [`MetricsRecord`](/api/org/apache/heron/spi/metricsmgr/metrics/MetricsRecord.html) that passes through the sink is processed.
[`flush`](/api/org/apache/heron/spi/metricsmgr/sink/IMetricsSink.html#flush--) | Flush any buffered metrics; this function is called at the interval specified by the `flush-frequency-ms` parameter. More info can be found in the [Stream Manager](../../concepts/architecture#stream-manager) documentation.
[`close`](/api/org/apache/heron/spi/metricsmgr/sink/IMetricsSink.html#close--) | Closes the stream and releases any system resources associated with it; if the stream is already closed, invoking `close()` has no effect.

Your implementation of those interfaces will need to be packaged into a JAR file
and distributed to the `heron-core/lib/metricsmgr` folder of your [Heron
release](compiling-overview).

## Example Implementation

Below is an example implementation that simply prints the contents of each
metrics record as it passes through:

```java
import org.apache.heron.metricsmgr.api.metrics.MetricsInfo;
import org.apache.heron.metricsmgr.api.metrics.MetricsRecord;
import org.apache.heron.metricsmgr.api.sink.IMetricsSink;
import org.apache.heron.metricsmgr.api.sink.SinkContext;

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
`metrics_sinks.yaml` configuration file in your scheduler's base configuration template.

At the top of that file there's a `sinks` parameter that lists each available
sink by name. You should add the sink you want to use to that list. Here's an example:

```yaml
sinks:
  - file-sink
  - scribe-sink
  - tmaster-sink
  - print-sink
  - prometheus-sink
```

For each sink you are required to specify the following:

Parameter | Description
:---------|:-----------
`class` | The Java class name of your custom implementation of the `IMetricsSink` interface, e.g. `biz.acme.heron.metrics.PrintSink`.
`flush-frequency-ms` | The frequency (in milliseconds) at which the `flush()` method is called in your implementation of `IMetricsSink`.
`sink-restart-attempts` | The number of times that a sink will attempt to restart if it throws exceptions and dies. If you do not set this, the default is 0; if you set it to -1, the sink will attempt to restart forever.

Here is an example `metrics_sinks.yaml` configuration:

```yaml
sinks:
  - custom-sink

print-sink:
  class: "biz.acme.heron.metrics.CustomSink"
  flush-frequency-ms: 60000 # One minute
  sink-restart-attempts: -1 # Attempt to restart forever
  some-other-config: false
```

It is optional to add other configurations for the sink. All configurations will be constructed
as an unmodifiable map `Map<String, Object> conf` and passed to the sink via the `init` function.

## Using Your Custom Sink

Once you've made a JAR for your custom Java sink, distributed that JAR to
`heron-core/lib/metricsmgr` folder, and changed the configuration in
`metrics_sinks.yaml` file in the base configuration template, any topology submitted using that configuration will include the custom sink.

You must [re-compile
Heron](compiling-overview) if you want to include the configuration in a new distribution of [Heron CLI](user-manuals-heron-cli).

