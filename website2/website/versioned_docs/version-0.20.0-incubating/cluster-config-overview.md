---
id: version-0.20.0-incubating-cluster-config-overview
title: Cluster Config Overview
sidebar_label: Cluster Config Overview
original_id: cluster-config-overview
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
Heron clusters can be configured at two levels:

1. **The system level** --- System-level configurations apply to the whole
Heron cluster rather than to any specific component (e.g. logging configurations).
2. **The component level** --- Component-level configurations enable you to establish 
default configurations for different components. 
These configurations are fixed at any stage of the topology's
[lifecycle](heron-topology-concepts#topology-lifecycle), once the topology
is deployed.

Neither system- nor component-level configurations can be overridden by topology developers.

All system-level configs and component-level defaults are declared in a
[YAML](http://www.yaml.org/) config file in `heron/config/src/yaml/conf/{cluster}/heron_internals.yaml`
in the Heron codebase. You can leave that file as is when [compiling
Heron](compiling-overview) or modify the values to suit your use
case.

## The System Level

There are a small handful of system-level configs for Heron. These are detailed
in [System-level Configuration](cluster-config-system-level).

## The Component Level

There is a wide variety of component-level configurations that you can establish
as defaults in your Heron cluster. These configurations tend to apply to
specific components in a topology and are detailed in the docs below:

* [Heron Instance](cluster-config-instance)
* [Heron Metrics Manager](cluster-config-metrics)
* [Heron Stream Manager](cluster-config-stream)
* [Heron Topology Master](cluster-config-tmaster)

### Overriding Heron Cluster Configuration

The Heron configuration applies globally to a cluster. 
It is discouraged to modify the configuration to suit one topology.
It is not possible to override the Heron configuration
for a topology via Heron client or other Heron tools.

More on Heron's CLI tool can be found in [Managing Heron
Topologies](user-manuals-heron-cli).
