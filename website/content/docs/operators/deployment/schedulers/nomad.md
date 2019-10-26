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
---
title: Nomad
---

Heron supports [Hashicorp](https://hashicorp.com)'s [Nomad](https://nomadproject.io) as a scheduler. You can use Nomad for either small- or large-scale Heron deployments or to run Heron locally in [standalone mode](../standalone).

> Update: Heron now supports running on Nomad via [raw exec driver](https://www.nomadproject.io/docs/drivers/raw_exec.html) and [docker driver](https://www.nomadproject.io/docs/drivers/docker.html)

## Nomad setup

Setting up a nomad cluster will not be covered here. See the [official Nomad docs](https://www.nomadproject.io/intro/getting-started/install.html) for instructions.

Instructions on running Heron on Nomad via raw execs are located here:

[Running Heron via Raw Execs on Nomad](../nomad-raw-execs) 

Instructions on running Heron on Nomad via docker containers are located here:

[Running Heron via Docker Containers on Nomad](../nomad-docker) 