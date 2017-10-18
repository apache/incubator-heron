---
title: Heron observability with Scribe
---

To set up your Heron cluster to export to Scribe, you need to make two changes to the `metrics_sinks.yaml` configuration file:

* Add `scribe-sink` to the `sinks` list (in addition to `file-sink`, `tmaster-sink`, and `metricscache-sink`).
* Add a `scribe-sink` map to the file that sets values for the [parameters](#scribe-parameters) listed below. You can start by uncommenting the existing `scribe-sink` map to get the default configuration.

### Scribe parameters

Parameter | Description | Default
:---------|:------------|:-------
`class` | The Java class used to control Prometheus sink behavior | [`com.twitter.heron.metricsmgr.sink.PrometheusSink`](/api/com/twitter/heron/metricsmgr/sink/PrometheusSink.html)
`flush-frequency-ms` | How frequently, in milliseconds, the `flush()` method is called | 60000 (one minute)
`sink-restart-attempts` | How many times Heron should attempt to publish metrics to Scribe before no longer attempting | -1 (forever)
`scribe-host` | The Scribe host to export metrics to | 127.0.0.1
`scribe-port` | The Scribe port to export metrics to | 1463
`scribe-category` | The Scribe category to export metrics to | `scribe-category`
`service-namespace` | The service name for the category in `scribe-category` | `heron`
`scribe-timeout-ms` | The timeout, in millisconds, when attempting to export metrics to Scribe | 200
`scribe-connect-server-attempts` | The maximum number of retry attempts when connecting to Scribe on the configured host and port | 2
`scribe-retry-attempts` | The maximum number of retry attempts when writing metrics to Scribe | 5
`scribe-retry-interval-ms` | The time interval, in milliseconds, at which Heron attempts to write metrics to Scribe | 100
