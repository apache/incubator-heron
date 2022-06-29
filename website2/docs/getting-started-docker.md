---
id: getting-started-docker
title: The official Apache Heron Docker Image(s)
sidebar_label: Heron & Docker
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

> The current version of Heron is {{heron:version}}

The official Apache Heron Docker image is located at the link below

<a target="_blank" href="https://hub.docker.com/repository/docker/apache/heron">https://hub.docker.com/repository/docker/apache/heron</a>

### Docker Quickstart
In one terminal execute to start Heron in a container

```bash
$ docker run -it  --rm \
   -p 8889:8889 \
   -p 8888:8888 \
   --name local-heron \
   apache/heron:0.20.4-incubating supervisord --nodaemon
```
In another terminal execute the following to deploy a job:
```bash
$ docker exec -it \
   local-heron \
   bash -c "heron submit sandbox  /heron/examples/heron-eco-examples.jar org.apache.heron.eco.Eco --eco-config-file /heron/examples/heron_wordcount.yaml"
```

View your job details by navigating to `localhost:8889` in your browser.  Congratulations, you've just deployed a Heron job in Docker!



