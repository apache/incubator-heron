# Implementing a Custom Metrics Sink

Each Heron topology has its own centralized [Metrics
Manager](../concepts/architecture.html#metrics-manager) (MM), which collects
metrics from all instances in the topology. You can define how the MM processes
metrics by implementing a **metrics sink**, which specifies how the MM handles
incoming
[`MetricsRecord`](../api/metrics/com/twitter/heron/metricsmgr/api/metrics/MetricsRecord.html)
objects.

Java is currently the only supported language for custom metrics sinks. This may
change in the future.

## Currently

Heron comes equipped out of the box with three metrics sinks that you can apply
for a specific topology:

* [`GraphiteSink`](../api/metrics/com/twitter/heron/metricsmgr/sink/Graphite.html)
  &mdash; Sends each `MetricsRecord` object to a
  [Graphite](http://graphite.wikidot.com/) instance according to a Graphite
  prefix.
* [`ScribeSink`](../api/metrics/com/twitter/heron/metricsmgr/sink/ScribeSink.html)
  &mdash; Sends each `MetricsRecord` object to a
  [Scribe](https://github.com/facebookarchive/scribe) instance according to a
  Scribe category and namespace.
* [`FileSink`](../api/metrics/com/twitter/heron/metricsmgr/sink/FileSink.html)
  &mdash; Writes each `MetricsRecord` object to a JSON file at a specified path.

More on using those sinks can be found in [Metrics
Manager](../operators/metrics-manager.html).

## Java Setup

In order to create a custom metrics sink, you need to import the
`metricsmgr-api` library into your project.

#### Maven

<pre><code class="lang-xml">&lt;dependency&gt;
  &lt;groupId>com.twitter.heron&lt;/groupId&gt;
  &lt;artifactId>metricsmgr-api&lt;/artifactId&gt;
  &lt;version&gt;{{book.metrics_api_version}}&lt;/version&gt;
&lt;/dependency&gt;</code></pre>

#### Gradle

<pre><code class="lang-groovy">dependencies {
  compile group: "com.twitter.heron", name: "metricsmgr-api", version: "{{book.metrics_api_version}}"
}</code></pre>

## The `IMetricsSink` Interface

Each metrics sink must implement the
[`IMetricsSink`](http://heronproject.github.io/metrics-api/com/twitter/heron/metricsmgr/IMetricsSink)
interface, which requires you to implement the following methods:

* `void init(Map<String, Object> conf, SinkContext context)` &mdash; Defines the
  initialization behavior of the sink. The `conf` map is the configuration that
  is passed to the sink by the `.yaml` configuration file; the `SinkContext`
  object enables you to both access values from the sink's runtime context
  (including the sink's ID and the topology name) and to
* `void processRecord(MetricsRecord record)` &mdash; Defines how each
  `MetricsRecord` that passes through the sink is processed.
* `void flush()` &mdash; Flush any buffered metrics; this function is called at the
  interval specified by the `flush-frequency-ms`
* `void close()` &mdash; Closes the stream and releases any system resources
  associated with it; if the stream is already closed, invoking `close()` has no
  effect

Your implementation of those interfaces will need to be on the
[classpath](https://docs.oracle.com/javase/tutorial/essential/environment/paths.html)
specified by the `metrics-mgr-classpath`
when you [compile Heron](../operators/compiling.html).

## Example Implementation

Below is an example implementation that simply prints the contents of each
metrics record as it passes through:

```java
import com.twitter.heron.metricsmgr.api.metrics.MetricsInfo;
import com.twitter.heron.metricsmgr.api.metrics.MetricsRecord;
import com.twitter.heron.metricsmgr.api.sink.IMetricsSink;
import com.twitter.heron.metricsmgr.api.sink.SinkContext;

public class PrintSink implements IMetricsSink {
    // We'll use this to extract a parameter from the supplied configuration
    private final String PARAMETER1 = "";

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
        System.out.println("Record received:");
        System.out.println(record.toString());
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

## Using Your Custom Sink

Configurable parameters for all sinks:

* `sinks` &mdash; A list of all sinks that you intend to use. Example:

   ```yaml
   sinks:
     - first-sink
     - second-sink
     - etc
   ```

* `class` &mdash; The Java class name of your custom implementation of the
  `IMetricsSink` interface, e.g. `com.example.heron.metrics.PrintSink`.
* `flush-frequency-ms` &mdash; The frequency (in milliseconds) at which the
  `flush()` method is called in your implementation of `IMetricsSink`.
* `sink-restart-attempts` &mdash; The number of times that a sink will attempt to
  restart if it throws exceptions and dies. If you do not set this, the default
  is 0; if you set it to -1, the sink will attempt to restart forever.

Alter `metrics_sinks.yaml` and add an entry for your custom sink:

```yaml
sinks:
  - print-sink

print-sink:
  class: "com.example.heron.metrics.PrintSink"
  flush-frequency-ms: 60000 # One minute
  sink-restart-attempts: -1 # Attempt to restart forever
```

If you're using multiple sinks your `metrics_sinks.yaml` file may look something
like this:

```yaml
sinks:
  - print-sink
  - 
```

## Classpath

`metrics-mgr-classpath`

Add Java class alongside the others

Modify `BUILD` file

Make sure binaries are synced (in the classpath) or else the class cannot be
found

