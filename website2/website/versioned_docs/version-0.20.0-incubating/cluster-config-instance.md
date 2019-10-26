---
id: version-0.20.0-incubating-cluster-config-instance
title: Heron Instance
sidebar_label: Heron Instance
original_id: cluster-config-instance
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

You can configure the behavior of the [Heron
Instances](heron-architecture#heron-instance) (HIs) in a
topology using the parameters below.

## Internal Configuration

These parameters deal with the TCP write and read queue for each instance.

Parameter | Meaning | Default
:-------- |:------- |:-------
`heron.instance.internal.bolt.read.queue.capacity` | The queue capacity (number of items) in bolt for buffer packets to read from stream manager | 128
`heron.instance.internal.bolt.write.queue.capacity` | The queue capacity (number of items) in bolt for buffer packets to write to stream manager | 128
`heron.instance.internal.spout.read.queue.capacity` | The queue capacity (number of items) in spout for buffer packets to read from stream manager | 1024
`heron.instance.internal.spout.write.queue.capacity` | The queue capacity (number of items) in spout for buffer packets to write to stream manager | 128
`heron.instance.internal.metrics.write.queue.capacity` | The queue capacity (number of items) for metrics packets to write to metrics manager | 128

## Network Configuration

You can configure how HIs collect and transmit data in one (but only one) of
two ways: **time based** or **size based**. If you choose time based, you can
specify the maximum batch time (in milliseconds) for reading from and writing
to the HI's socket; if you choose size based, you can specify maximum batch
sizes (in bytes) instead.

Parameter | Meaning | Default
:-------- |:------- |:-------
`heron.instance.network.read.batch.time.ms` | Time based, the maximum batch time in ms for instance to read from stream manager per attempt | 16
`heron.instance.network.read.batch.size.bytes` | Size based, the maximum batch size in bytes to read from stream manager | 32768
`heron.instance.network.write.batch.time.ms` | Time based, the maximum batch time in ms for instance to write to stream manager per attempt | 16
`heron.instance.network.write.batch.size.bytes` | Size based, the maximum batch size in bytes to write to stream manager | 32768

### Other Network Parameters

The following parameters do not need to be set in accordance with a time- or
size-based system.

Parameter | Meaning | Default
:-------- |:------- |:-------
`heron.instance.network.options.socket.send.buffer.size.bytes` | The maximum socket's send buffer size in bytes | 6553600
`heron.instance.network.options.socket.received.buffer.size.bytes` | The maximum socket's received buffer size in bytes of instance's network options | 8738000
`heron.instance.reconnect.streammgr.interval.sec` | Interval in seconds to reconnect to the stream manager, including the request timeout in connecting | 5
`heron.instance.reconnect.streammgr.times` | The maximum number of connection attempts made to the SM before the SM is forcibly restarted | 60

## Metrics Manager Configuration

These parameters deal with how each HI interacts with the topology's [Stream
Manager](heron-architecture#stream-manager).

Parameter | Meaning | Default
:-------- |:------- |:-------
`heron.instance.metrics.system.sample.interval.sec` | The interval, in seconds, at which an instance samples its system metrics, e.g. CPU load. | 10
`heron.instance.reconnect.metricsmgr.interval.sec` | Interval in seconds to reconnect to the metrics manager, including the request timeout in connecting | 5
`heron.instance.reconnect.metricsmgr.times` | The maximum number of connection attempts to the MM before the MM is forcibly restarted | 60

## Tuning

These parameters are used to dynamically tune the available sizes in read and
write queues to maintain high performance while avoiding garbage collection
issues.

Parameter | Meaning | Default
:-------- |:------- |:-------
`heron.instance.tuning.expected.bolt.read.queue.size` | The expected size on read queue in bolt | 5
`heron.instance.tuning.expected.bolt.write.queue.size` | The expected size on write queue in bolt | 5
`heron.instance.tuning.expected.spout.read.queue.size` | The expected size on read queue in spout | 512
`heron.instance.tuning.expected.spout.write.queue.size` | The expected size on write queue in spout | 5
`heron.instance.tuning.expected.metrics.write.queue.size` | The expected size on metrics write queue | 5
`heron.instance.tuning.current.sample.weight` | TODO | 0.8

## Other Parameters

Parameter | Meaning | Default
:-------- |:------- |:-------
`heron.instance.set.data.tuple.capacity` | The maximum number of data tuples to batch in a `HeronDataTupleSet` protobuf message | 256
`heron.instance.set.control.tuple.capacity` | The maximum number of control tuples to batch in a `HeronControlTupleSet` protobuf message | 256
`heron.instance.ack.batch.time.ms` | The maximum time in ms for an spout to do acknowledgement per attempt, the ack batch could also break if there are no more ack tuples to process |128
`heron.instance.emit.batch.time.ms` | The maximum time in ms for an spout instance to emit tuples per attempt | 16
`heron.instance.emit.batch.size.bytes` | The maximum batch size in bytes for an spout to emit tuples per attempt | 32768
`heron.instance.execute.batch.time.ms` | The maximum time in ms for an bolt instance to execute tuples per attempt | 16
`heron.instance.execute.batch.size.bytes` | The maximum batch size in bytes for an bolt instance to execute tuples per attempt | 32768
`heron.instance.state.check.interval.sec` | The time interval for an instance to check the state change, for instance, the interval a spout using to check whether activate/deactivate is invoked | 5
`heron.instance.acknowledgement.nbuckets` | TODO | 10
