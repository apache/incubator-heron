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
Heron processing topologies can be written using an API called the **Heron ECO API**. The ECO API is currently available to work with spouts and bolts from the following packages:

* `org.apache.storm`

We will expand compatibility based on feedback we get from the community.

> Although this document focuses on the ECO API, both the [Streamlet API](../../../concepts/streamlet-api) and [Topology API](../../../concepts/topologies) topologies you have built can still be used with Heron

## The Heron ECO API vs. The Streamlet and Topology APIs

Heron's ECO offers one major difference over the Streamlet and Topology APIs and that is extensibility without recompilation.
With Heron's ECO developers now have a way to alter the way data flows through spouts and bolts without needing to get into their code and make changes.
Topologies can now be defined through a YAML based format.

## Why the name ECO?

/ˈekoʊ/ (Because all software should come with a pronunciation guide these days)
ECO is an acronym that stands for:
* Extensible
* Component
* Orchestrator