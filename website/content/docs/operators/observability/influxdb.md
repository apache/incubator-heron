---
title: Observability with InfluxDB
---

You can integrate Heron with
[InfluxDB](https://www.influxdata.com/time-series-platform/influxdb/) to
monitor and gather runtime metrics exported by Heron topologies.

## Exporting topology metrics from Heron to InfluxDB

To set up your Heron cluster to export to InfluxDB, you need to make two
changes to the `metrics_sinks.yaml` configuration file:

* Add `influxdb-sink` to the `sinks` list
* Add a `influxdb-sink` map to the file that sets values for the
  [parameters](#influxdb-parameters) listed below. You can uncomment the
  existing `influxdb-sink` map to get the default configuration.

## Batch processing

The InfluxDB sink has two operating modes. It can either send metrics as soon
as Heron sends them to the sink (batch processing disabled) or it can buffer
each metric until the flush frequency interval (configured via the
`flush-frequency-ms` key in the `metrics_sinks.yaml` file - [see
below](#influxdb-parameters)) ends and then send all the buffered metrics in
one batch (batch processing enabled).

### Disabled

With batch processing disabled the metrics will be issued at the rate defined
by the `heron.metrics.export.interval.sec` key in the scheduler's
`heron_internals.yaml` configuration file. Once each instance reaches the end
of this interval it will issue its metrics to the MetricsManager in the
container and they will be processed and send to the InfluxDB server. This
may have the effect of spreading the writes, to the InfluxDB server, over time.
However, there will be no error recovery for failed writes.

### Enabled

With batch processing enabled, the InfluxDB sink will buffer all metrics it
receives and write them in a batch when each flush frequency interval is
reached. This means that all metrics in that interval are written in one go,
which can lead to higher throughput but may result in load spikes at the
InfluxDB server for topologies with high instance counts.

The size of the write buffer can be configured via the `main-buffer-size` key
in the `metrics_sinks.yaml` file ([see below](#influxdb-parameters) for more
details). Please note that when this buffer is full, all metrics will be
written to the InfluxDB server, regardless of weather the flush frequency
interval is complete. Setting this value higher for topologies with high
instance counts (parallelism) will reduce the write frequency and increase
throughput but may result in the MetricsManager requiring more memory.

The InfluxDB sink, with batch processing enabled, will also buffer failed
writes to the server and will attempt to re-send them. The size of this buffer
is configured via the `fail-buffer-size` key in the `metrics_sinks.yaml` file
([see below](#influxdb-parameters) for more details). You should make sure that
the fail buffer size is greater than or equal to the main buffer size,
otherwise the fail buffer will not be used. If the fail buffer fills up and new
writes fail then the oldest writes in the fail buffer will be dropped. As with
the main buffer be mindful of memory requirements when setting the size of the
fail buffer.

## InfluxDB parameters

Parameter | Description | Default
:---------|:------------|:-------
`class` | The Java class used to send metrics to the InfluxDB server | [`org.apache.heron.metricsmgr.sink.InfluxDBSink`](/api/org/apache/heron/metricsmgr/sink/InfluxDBSink.html)
`flush-frequency-ms` | This is the time in milliseconds between metrics being written to the InfluxDB sever. **Note**: This will have no effect if `enable-batch-processing` is set to false. | 60000
`influx-host` | The address of the InfluxDB server host. | http://localhost
`influx-port` | The port that the InfluxDB server is listening on. | 8086
`influx-db-name` | The name of the database on the InfluxDB server that all metrics will be submitted to. | heron
`influx-db-username` | If the InfluxDB server has authentication enabled then supply the username to be used for metric submissions. | None
`influx-db-password` | If the InfluxDB server has authentication enabled then supply the password to be used for metric submissions. | None
`enable-batch-processing` | Boolean (either "true" or "false") indicating if [batch processing](#batch-processing) should be enabled  | `"true"`
`main-buffer-size` | If batch processing is enabled then this is the size of the write buffer. This will be flushed according to the `flush-frequency-ms` value or when it is full, which ever happend first. | 10000
`fail-buffer-size` | If batch processing is enabled then this is the size of the buffer for failed writes to the InfluxDB server. If this buffer is full and further failed writes will cause the oldest writes in the buffer to be dropped. | 10000
