---
id: version-0.20.0-incubating-guides-topology-tuning
title: Topology Tuning Guide
sidebar_label: Topology Tuning Guide
original_id: guides-topology-tuning
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

### Overview

This guide provides basic steps at tuning a topology to utilize resources efficiently. Currently, resources are primarily measured in terms of CPU cores and RAM. In Heron, some of the basic parameters that are available to tune a topology are, but not limited to, the following:

1. Container RAM
2. Container CPU
3. Component RAMs
4. Component Parallelisms
5. Number of Containers

Note that tuning a topology may be difficult and may take multiple iterations. Before
proceeding, please make sure you understand concepts to understand the
terminology, as well as the reasoning behind taking these steps.

### Steps to Tune a Topology

1. Launch the topology with an initial estimate of resources. These can be based
   on input data size, component logic, or experience from another working
   topology.

2. Resolve any backpressure issues by increasing the parallelism or container
   RAM, or CPU, or appropriately if backpressure is due to an external service.

3. Make sure there is no spout lag. In steady state, the topology should be able
   to read the whole of data.

4. Repeat steps 2 and 3 until there is no backpressure and no spout lag.

5. By now, the CPU usage and RAM usage are stable. Based on daily of weekly data
   trends, leave appropriate room for usage spikes, and cut down the rest of the
   unused resources allocated to topology.

While these steps seem simple, it might take some time to get the topology to
its optimal usage. Below are some of the tips that can be helpful during tuning
or in general.

### Additional Tips

1. If component RAMs for all the components is provided, that will the RAM
   assigned to those instances. Use this configuration according to their
   functionality to save of resources. By default, every instance is assigned
   1GB of RAM, which can be higher that what it requires. Note that if container
   RAM is specified, after setting aside some RAM for internal components of
   Heron, rest of it is equally divided among all the instances present in the
   container.

2. A memory intensive operation in bolts can result in GC issues. Be aware of
   objects that might enter old generation, and cause memory starvation.

3. You can use `Scheme`s in spouts to sample down the data. This can helpful
   when dealing with issues if writing to external services, or just trying to
   get an early estimate of usage without utilizing much resources. Note that
   this would still require 100% resource usage in spouts.
