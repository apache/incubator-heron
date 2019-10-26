---
id: version-0.20.0-incubating-observability-scribe
title: Observability with Scribe
sidebar_label: Scribe
original_id: observability-scribe
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

You can integrate Heron with [Scribe](https://github.com/facebookarchive/scribe/wiki) to monitor and gather runtime metrics exported by Heron topologies.

## Exporting topology metrics from Heron to Scribe

Heron supports custom metric exporters from the Metrics Manager. You can either build your own Scribe metrics sink or use the [provided Scribe sink](extending-heron-metric-sink).

To set up your Heron cluster to export to Scribe, you need to make two changes to the `metrics_sinks.yaml` configuration file:

* Add `scribe-sink` to the `sinks` list
* Add a `scribe-sink` map to the file that sets values for the [parameters](#scribe-parameters) listed below. You can uncomment the existing `prometheus-sink` map to get the default configuration.

### Scribe parameters

Parameter | Description | Default
:---------|:------------|:-------
`class` | The Java class used to control Prometheus sink behavior | [`org.apache.heron.metricsmgr.sink.ScribeSink`](/api/org/apache/heron/metricsmgr/sink/ScribeSink.html)
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
