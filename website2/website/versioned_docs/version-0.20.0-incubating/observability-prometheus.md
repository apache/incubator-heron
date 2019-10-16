---
id: version-0.20.0-incubating-observability-prometheus
title: Observability with Prometheus
sidebar_label: Prometheus
original_id: observability-prometheus
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

You can integrate Heron with [Prometheus](https://prometheus.io/) to monitor and gather runtime metrics exported by Heron topologies.

## Exporting topology metrics from Heron to Prometheus

Heron supports custom metric exporters from the Metrics Manager. You can either build your own Prometheus metrics sink or use the [provided Prometheus sink](extending-heron-metric-sink).

To set up your Heron cluster to export to Prometheus, you need to make two changes to the `metrics_sinks.yaml` configuration file:

* Add `prometheus-sink` to the `sinks` list
* Add a `prometheus-sink` map to the file that sets values for the [parameters](#prometheus-parameters) listed below. You can uncomment the existing `prometheus-sink` map to get the default configuration.

### Prometheus parameters

Parameter | Description | Default
:---------|:------------|:-------
`class` | The Java class used to control Prometheus sink behavior | [`org.apache.heron.metricsmgr.sink.PrometheusSink`](/api/org/apache/heron/metricsmgr/sink/PrometheusSink.html)
`port` | The port on which Prometheus will scrape for metrics. **Note**: You must supply a `port` *or* `port-file` config. | 8080
`port-file` | The path to a text file that contains the port number to be used by Prometheus for metrics scraping. **Note**: You must supply a `port` *or* `port-file` config. | `metrics.port`
`path` | The Prometheus path on which to publish metrics | `/metrics`
`include-topology-name` | Whether the name of the Heron topology will be included in names for metrics | `true`
`metrics-cache-max-size` | The maximum number of metrics cached and published | 1000000
`metrics-cache-ttl-sec` | The time to live (TTL) for metrics, i.e. the time, in seconds after which a metric that was collected will stop being published | 600 (10 minutes)
