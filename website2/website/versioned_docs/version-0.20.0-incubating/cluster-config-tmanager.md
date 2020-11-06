---
id: version-0.20.0-incubating-cluster-config-tmanager
title: Topology Manager
sidebar_label: Topology Manager
original_id: cluster-config-tmanager
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

You can configure the [Topology
Manager](heron-architecture#topology-manager) (TM) for a topology
using the parameters below.

Parameter | Meaning | Default
:-------- |:------- |:-------
`heron.tmanager.metrics.collector.maximum.interval.min` | The maximum interval, in minutes, for metrics to be kept in the Topology Manager | 180
`heron.tmanager.establish.retry.times` | The maximum time to retry to establish the Topology Manager | 30
`heron.tmanager.establish.retry.interval.sec` | The interval to retry to establish the Topology Manager | 1
`heron.tmanager.network.server.options.maximum.packet.mb` | The maximum packet size, in megabytes, of the Topology Manager's network options for Stream Managers to connect to | 16
`heron.tmanager.network.controller.options.maximum.packet.mb` | The maximum packet size, in megabytes, of the Topology Manager's network options for scheduler to connect to | 1
`heron.tmanager.network.stats.options.maximum.packet.mb` | The maximum packet size, in megabytes, of the Topology Manager's network options for stat queries | 1
`heron.tmanager.metrics.collector.purge.interval.sec` | The interval, in seconds, at which the Topology Manager purges metrics from the socket | 60
`heron.tmanager.metrics.collector.maximum.exception` | The maximum number of exceptions to be stored in the topology's metrics collector, to prevent potential out-of-memory issues | 256
`heron.tmanager.metrics.network.bindallinterfaces` | Whether the metrics reporter binds on all interfaces | `False`
`heron.tmanager.stmgr.state.timeout.sec` | The timeout, in seconds, for the Stream Manager, compared with (current time - last heartbeat time) | 60
