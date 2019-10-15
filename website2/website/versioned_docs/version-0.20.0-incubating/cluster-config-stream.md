---
id: version-0.20.0-incubating-cluster-config-stream
title: Stream Manager
sidebar_label: Stream Manager
original_id: cluster-config-stream
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

You can configure the [Stream
Manager](heron-architecture#stream-manager) (SM) in a
topology using the parameters below, including how the SM handles [back
pressure](#back-pressure-parameters).

## Back Pressure Parameters

Parameter | Meaning | Default
:-------- |:------- |:-------
`heron.streammgr.network.backpressure.threshold` | The number of times the SM should wait to see a buffer full while enqueueing data before declaring the start of backpressure | `3`
`heron.streammgr.network.backpressure.highwatermark.mb` | The high water mark on the number of megabytes that can be left outstanding on a connection | `50`
`heron.streammgr.network.backpressure.lowwatermark.md` | The low water mark on the number of megabytes that can be left outstanding on a connection | `30`
`heron.streammgr.network.options.maximum.packet.mb` | The maximum packet size, in megabytes, for the SM's network options | `100`

## Timeout Interval

Parameter | Meaning | Default
:-------- |:------- |:-------
`heron.streammgr.xormgr.rotatingmap.nbuckets` | TODO | `3`

## Other Parameters

Parameter | Meaning | Default
:-------- |:------- |:-------
`heron.streammgr.packet.maximum.size.bytes` | Maximum size (in bytes) of packets sent out from the SM | `102400`
`heron.streammgr.cache.drain.frequency.ms` | The frequency (in milliseconds) at which the SM's tuple cache is drained | `10`
`heron.streammgr.cache.drain.size.mb` | The size threshold (in megabytes) at which the SM's tuple cache is drained | `100`
`heron.streammgr.client.reconnect.interval.sec` | The reconnect interval to other SMs for the SM client (in seconds) | `1`
`heron.streammgr.client.reconnect.tmaster.interval.sec` | The reconnect interval to the Topology Master for the SM client (in seconds) | `10`
`heron.streammgr.tmaster.heartbeat.interval.sec` | The interval (in seconds) at which a heartbeat is sent to the Topology Master | `10`
`heron.streammgr.connection.read.batch.size.mb` | The maximum batch size (in megabytes) at which the SM reads from the socket | `1`
`heron.streammgr.connection.write.batch.size.mb` | The maximum batch size (in megabytes) to write by the stream manager to the socket | `1`
