---
id: version-0.20.0-incubating-guides-packing-algorithms
title: Packing Algorithms
sidebar_label: Packing Algorithms
original_id: guides-packing-algorithms
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

This guide provides basic steps at using and tuning the First Fit Decreasing packing algorithm in
order to  utilize the resources efficiently. This packing algorithm aims at utilizing as few
containers as possible, thus limiting the overall resources used. The algorithm is based on the
First Fit Decreasing heuristic for the [Binpacking problem](https://en.wikipedia.org/wiki/Bin_packing_problem).
The algorithm is useful in the following scenarios:

1. The user does not know how many containers to use. This algorithm decides the number of
   containers to be used and thus the user does not have to specify the number of containers
   in the topology configuration.
2. The user wants to minimize the resource consumption. The First Fit Decreasing packing algorithm
   uses a minimum number of containers in order to reduce the resources allocated to the topology.
   Note that for each new container, a stream manager process will be launched which will increase
   the amount of resources used. Thus, reducing the number of containers can result in further
   resource savings.
3. The user expects that the provisioned per-instance RAM would be either the one specified in the
   component RAM or the default value. The algorithm guarantees that the placement of instances in
   the containers will never result in an allocation that assigns to one or more instances a smaller
   amount of RAM than expected.

To enable the First Fit Decreasing
algorithm, update the `packing.yaml` file as follows:

```yaml
# packing algorithm for packing instances into containers
heron.class.packing.algorithm:    org.apache.heron.packing.binpacking.FirstFitDecreasingPacking
```

The algorithm accepts as input the values of the following parameters:

1. Component RAM
2. Hint for the maximum container RAM
   (`org.apache.heron.api.Config.TOPOLOGY_CONTAINER_MAX_RAM_HINT`)
3. Hint for the maximum container CPU
   (`org.apache.heron.api.Config.TOPOLOGY_CONTAINER_MAX_CPU_HINT`)
4. Hint for the maximum container Disk
   (`org.apache.heron.api.Config.TOPOLOGY_CONTAINER_MAX_DISK_HINT`)
5. Padding percentage (`org.apache.heron.api.Config.TOPOLOGY_CONTAINER_PADDING_PERCENTAGE`)
6. Component Parallelisms

Parameter 1 determines the RAM requirement of each component in the topology.
If the requirement is not specified then a default value of 1GB is used. The First Fit Decreasing
algorithm guarantees that the amount of memory allocated to a component is either the one specified
by the user or the default one.

The parameters 2-4 determine the maximum container size with respect to RAM, CPU cores and disk.
If one of these parameters is not specified by the user then
the hint for the corresponding maximum container resource is set to the default resource requirement
of 4 Heron instances.

Note that these values take into account only the resources allocated for the user's instances.
Additional per container resources for system-related processes such as the stream manager can be
added to the maximum container size defined above. Thus, the algorithm might eventually produce
containers slightly bigger that the boundary determined by parameters 2-4. The amount of the
additional resources allocated to each container to account for additional internal Heron resource
requirements, is determined by the padding percentage specified in parameter 5. If the user does
not specify the padding percentage, then the system will use a default value of 10.
In this case, after a container has been filled with user instances, an additional 10% of resources
will be allocated to it.

Based on these parameters, the algorithm decides how to place the instances in the containers
and how many containers to use. More specifically, the algorithm first sorts the instances in
decreasing order of their RAM requirements. It then picks the instance on the head of the sorted
list and places it in the first container that has enough resources (RAM, CPU cores, disk) to
accommodate it. If none of the existing containers have the requires resources, then a new container
is allocated. Note that if an the RAM requirements of an instance exceed the value of
parameter 2, then the algorithm returns an empty packing plan. After all the instances have
been allocated to the containers, the algorithm adds the per-container padding resources
as specified by parameter 5. The packing plan produced by the First Fit Decreasing packing algorithm
can contain heterogeneous containers. Note that the algorithm does not require the number of
containers as input.

### Configuring the First Fit Decreasing Packing Algorithm

1. The methods `org.apache.heron.api.Config.setContainerMaxRamHint(long bytes)`,
   `org.apache.heron.api.Config.setContainerMaxCpuHint(float ncpus)`,
   `org.apache.heron.api.Config.setContainerMaxDiskHint(long bytes)`
   can be used to set parameters 2-4 when defining a topology.

2. The `org.apache.heron.api.Config.setContainerPaddingPercentage(int percentage)`
   method can be used to set the padding percentage
   defined in parameter 5 when defining a topology.

   Here's an example code snippet for setting these parameters when defining a topology:

   ```java

     // Set up the topology and its config
     org.apache.heron.api.Config topologyConfig = new org.apache.heron.api.Config();

     long maxContainerRam = 10L * Constants.GB;

     topologyConfig.setContainerMaxRamHint(maxContainerRam);
     topologyConfig.setContainerPaddingPercentage(5);
   ```
