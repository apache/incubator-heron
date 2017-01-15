---
title: Topology Master
---

You can configure the [Topology
Master](../../../concepts/architecture#metrics-manager) (TM) for a topology
using the parameters below.

Parameter | Meaning | Default
:-------- |:------- |:-------
`heron.tmaster.metrics.collector.maximum.interval.min` | The maximum interval, in minutes, for metrics to be kept in the Topology Master | 180
`heron.tmaster.establish.retry.times` | The maximum time to retry to establish the Topology Master | 30
`heron.tmaster.establish.retry.interval.sec` | The interval to retry to establish the Topology Master | 1
`heron.tmaster.network.master.options.maximum.packet.mb` | The maximum packet size, in megabytes, of the Topology Master's network options for Stream Managers to connect to | 16
`heron.tmaster.network.controller.options.maximum.packet.mb` | The maximum packet size, in megabytes, of the Topology Master's network options for scheduler to connect to | 1
`heron.tmaster.network.stats.options.maximum.packet.mb` | The maximum packet size, in megabytes, of the Topology Master's network options for stat queries | 1
`heron.tmaster.metrics.collector.purge.interval.sec` | The interval, in seconds, at which the Topology Master purges metrics from the socket | 60
`heron.tmaster.metrics.collector.maximum.exception` | The maximum number of exceptions to be stored in the topology's metrics collector, to prevent potential out-of-memory issues | 256
`heron.tmaster.metrics.network.bindallinterfaces` | Whether the metrics reporter binds on all interfaces | `False`
`heron.tmaster.stmgr.state.timeout.sec` | The timeout, in seconds, for the Stream Manager, compared with (current time - last heartbeat time) | 60
