---
title: Metrics Manager
---

You can configure all of the [Metrics
Managers](../../../concepts/architecture#metrics-manager) (MMs) in a topology
using the parameters below.

## Network Configuration

You can configure how the MM collects and transmits data in one (but only one)
of two ways: **time based** or **size based**. If you choose time based, you can
specify the maximum batch time (in milliseconds) for reading from and writing to
the MM's socket; if you choose size based, you can specify maximum batch sizes
(in bytes) instead.

### Time-based Configuration

Config | Meaning | Default
:----- |:------- |:-------
`heron.metricsmgr.network.read.batch.time.ms` | The maximum batch time in milliseconds for the MM to read from the socket | 16
`heron.metricsmgr.network.write.batch.time.ms` | The maximum batch time in milliseconds for the MM to write to the socket | 16

### Size-based Configuration

Config | Meaning | Default
:----- |:------- |:-------
`heron.metricsmgr.network.read.batch.size.bytes` | Size based, the maximum batch size in bytes to read from socket | 32768
`heron.metricsmgr.network.write.batch.size.bytes` | Size based, the maximum batch size in bytes to write to socket | 32768

## Buffer Configuration

Each MM instance has a socket buffer for reading and writing metrics data. You
can set maximum buffer sizes for both send and receive buffers.

Config | Meaning | Default
:----- |:------- |:-------
`heron.metricsmgr.network.options.socket.send.buffer.size.bytes` | The maximum socket's send buffer size in bytes | 6553600
`heron.metricsmgr.network.options.socket.received.buffer.size.bytes` | The maximum socket's received buffer size in bytes of the metrics manager's network options | 8738000
