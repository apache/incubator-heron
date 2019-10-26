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
Heron processing topologies can be written using an API called the **Heron Streamlet API**. The Heron Streamlet API is currently available for the following languages:

* [Java](../../../developers/java/streamlet-api)
* [Scala](../../../developers/scala/streamlet-api)
<!-- * [Python](../../../developers/python/functional-api) -->

> Although this document covers the new Heron Streamlet API, topologies created using the original [topology API](../../../concepts/topologies) can still be used with Heron (which means that all of your older topologies will still run).

For a more in-depth conceptual guide to the new API, see [The Heron Streamlet API](../../../concepts/streamlet-api). A high-level overview can also be found in the section immediately [below](#the-heron-streamlet-api-vs-the-topology-api).

## The Heron Streamlet API vs. The Topology API

When Heron was first released, all Heron topologies needed to be written using an API based on the [Storm Topology API](../topologies). Although this API is quite powerful (and can still be used), the **Heron Streamlet API** enables you to create topologies without needing to implement spouts and bolts directly or to connect spouts and bolts together.

Here are some crucial differences between the two APIs:

Domain | Original Topology API | Heron Streamlet API
:------|:----------------------|:--------------------
Programming style | Procedural, processing component based | Functional
Abstraction level | **Low level**. Developers must think in terms of "physical" spout and bolt implementation logic. | **High level**. Developers can write processing logic in an idiomatic fashion in the language of their choice, without needing to write and connect spouts and bolts.
Processing model | [Spout](../spouts) and [bolt](../bolts) logic must be created explicitly, and connecting spouts and bolts is the responsibility of the developer | Spouts and bolts are created for you automatically on the basis of the processing graph that you build

The two APIs also have a few things in common:

* Topologies' [logical](../../../concepts/topologies#logical-plan) and [physical](../../../concepts/topologies#physical-plan) plans are automatically created by Heron
* Topologies are [managed](../../../operators/heron-cli) in the same way using the `heron` CLI tool