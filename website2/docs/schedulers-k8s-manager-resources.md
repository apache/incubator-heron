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

> This document demonstrates how you can configure a topology's `Executor` and/or `Manager` (hereinafter referred to as `Heron containers`) resource `Requests` and `Limits` through CLI commands.

<br/>

You may configure an individual topology's `Heron container`'s resource `Requests` and `Limits` during submission through CLI commands.

<br>

## Usage

The command pattern is as follows:
`heron.kubernetes.[executor | manager].[limits | requests].[OPTION]=[VALUE]`

The currently supported CLI `options` are:

* `cpu`: A natural number indicating the number of CPU's.
* `memory`: A positive decimal number indicating the amount of memory in `Megabytes`.

<br>

### Example

An example submission command is as follows.

***Limits and Requests:***

```bash
~/bin/heron submit kubernetes ~/.heron/examples/heron-api-examples.jar \
org.apache.heron.examples.api.AckingTopology acking \
--config-property heron.kubernetes.manager.limits.cpu=2 \
--config-property heron.kubernetes.manager.limits.memory=3 \
--config-property heron.kubernetes.manager.requests.cpu=1 \
--config-property heron.kubernetes.manager.requests.memory=2
```
