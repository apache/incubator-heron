# Topology Master Configuration

You can configure the [Topology
Master](../../concepts/architecture.html#metrics-manager) (TM) for a topology
using the parameters below.

Parameter | Meaning | Default
:-------- |:------- |:-------
`heron.check.tmaster.location.interval.sec` | The interval, in seconds, after which to check if the topology master location has been fetched or not | 120
`heron.tmaster.metrics.collector.maximum.interval.min` | The maximum interval, in minutes, for metrics to be kept in the topology master | 180
`heron.tmaster.establish.retry.times` | The maximum time to retry to establish the topology master | 30
`heron.tmaster.establish.retry.interval.sec` | The interval to retry to establish the topology master | 1
`heron.tmaster.network.master.options.maximum.packet.mb` | The maximum packet size in MB of the topology master's network options for stream managers to connect to | 16
`heron.tmaster.network.controller.options.maximum.packet.mb` | The maximum packet size in MB of topology master's network options for scheduler to connect to | 1
`heron.tmaster.network.stats.options.maximum.packet.mb` | The maximum packet size in MB of topology master's network options for stat queries | 1
`heron.tmaster.metrics.collector.purge.interval.sec` | The interval for topology master to purge metrics from socket | 60
`heron.tmaster.metrics.collector.maximum.exception` | The maximum # of exception to be stored in topology metrics collector, to prevent potential out-of-memory issues | 256
`heron.tmaster.metrics.network.bindallinterfaces` | Should the metrics reporter bind on all interfaces | False
`heron.tmaster.stmgr.state.timeout.sec` | The timeout in seconds for stream mgr, compared with (current time - last heartbeat time) | 60
