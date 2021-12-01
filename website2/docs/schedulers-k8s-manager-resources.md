---
id: schedulers-k8s-manager-resources
title: Configuring a Topology's Manager Resources in Kubernetes (CLI)
sidebar_label: Configuring a Topology's Manager Resources in Kubernetes (CLI)
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

> This document demonstrates how you can configure a topology's `Manager` resource `Requests` and `Limits` through CLI commands.

<br/>

You may configure an individual toplogy `Manager`'s resource `Requests` and `Limits` during submission through CLI commands. The `Manager`'s configuration will be identical to that of the topology's `Executors` with the exception of when CLI parameters are provided for the `Manager`s resources.

***Note:*** In the event `Limits` are provided via CLI but `Requests` are not, the `Requests` will be set to the same values as those of the new `Limits`.


<br>

## Usage

The command pattern is as follows:
`heron.kubernetes.manager.[limits | requests].[OPTION]=[VALUE]`

The currently supported CLI `options` are:

* `cpu`
* `memory`

All associated `value`s must be natural numbers.

<br>

### Example

An example submission command is as follows.

***Limits and Requests:***

```bash
~/bin/heron submit kubernetes ~/.heron/examples/heron-api-examples.jar \
org.apache.heron.examples.api.AckingTopology acking \
--verbose \
--config-property heron.kubernetes.manager.limits.cpu=2 \
--config-property heron.kubernetes.manager.limits.memory=3 \
--config-property heron.kubernetes.manager.requests.cpu=1 \
--config-property heron.kubernetes.manager.requests.memory=2
```
