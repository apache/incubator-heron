---
id: getting-started-migrate-storm-topologies
title: Migrate Storm Topologies
sidebar_label: Migrate Storm Topologies
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

Heron is designed to be fully backwards compatible with existing [Apache
Storm](http://storm.apache.org/index.html) v1 projects, which means that you can
migrate an existing Storm [topology](heron-topology-concepts) to Heron by making
just a few adjustments to the topology's `pom.xml` [Maven configuration
file](https://maven.apache.org/pom.html).

## Step 1. Add Heron dependencies to  `pom.xml`

Copy the [`pom.xml`](https://maven.apache.org/pom.html) segments below and paste
them into your existing Storm `pom.xml` file in the [dependencies
block](https://maven.apache.org/pom.html#Dependencies).

```xml
<dependency>
  <groupId>org.apache.heron</groupId>
  <artifactId>heron-api</artifactId>
  <version>{{heron:version}}</version>
  <scope>compile</scope>
</dependency>
<dependency>
  <groupId>org.apache.heron</groupId>
  <artifactId>heron-storm</artifactId>
  <version>{{heron:version}}</version>
  <scope>compile</scope>
</dependency>
```

## Step 2. Remove Storm dependencies from `pom.xml`

Delete the Storm dependency, which looks like this:

```xml
<dependency>
  <groupId>org.apache.storm</groupId>
  <artifactId>storm-core</artifactId>
  <version>storm-VERSION</version>
  <scope>provided</scope>
</dependency>
```

## Step 3 (if needed). Remove the Clojure plugin from `pom.xml`

Delete the [Clojure plugin](https://maven.apache.org/pom.html#Plugins), which
should look like this:

```xml
<plugin>
  <groupId>com.theoryinpractise</groupId>
  <artifactId>clojure-maven-plugin</artifactId>
  <version>1.3.12</version>
  <extensions>true</extensions>
  <configuration>
    <sourceDirectories>
      <sourceDirectory>src/clj</sourceDirectory>
    </sourceDirectories>
  </configuration>
</plugin>
```

## Step 4. Run Maven commands

Run the following [Maven lifecycle](https://maven.apache.org/run.html) commands:

```bash
$ mvn clean
$ mvn compile
$ mvn package
```

> [Storm Distribute RPC](http://storm.apache.org/releases/0.10.0/Distributed-RPC.html) is deprecated in Heron.

## Step 4 (optional). Launch your upgraded Heron topology

You can launch the compiled Maven project on your [local
cluster](schedulers-local) using `heron submit`.

First, modify your project's base directory `{basedir}` and
`{PATH-TO-PROJECT}.jar`, which is located in `${basedir}/target` by [Maven
convention](https://maven.apache.org/guides/getting-started/). Then modify the
`TOPOLOGY-FILE-NAME` and `TOPOLOGY-CLASS-NAME` for your project:

```bash
$ heron submit local \
  ${basedir}/target/PATH-TO-PROJECT.jar \
  TOPOLOGY-FILE-NAME \
  TOPOLOGY-CLASS-NAME
```

Here's an example submit command using the example topology from the [Quick
Start Guide](getting-started-local-single-node) guide:

```bash
$ heron submit local \
  ~/.heron/examples/heron-api-examples.jar \ # The path of the topology's jar file
  org.apache.heron.examples.api.ExclamationTopology \ # The topology's Java class
  ExclamationTopology # The name of the topology
```

### Next Steps

* [Deploy topologies](deployment-overview) in clustered, scheduler-driven
  environments (such as on [Aurora](schedulers-aurora-cluster)
  and
  [locally](schedulers-local)
* [Develop topologies](heron-architecture) for Heron
