---
id: version-0.20.0-incubating-deployment-api-server
title: The Heron API Server
sidebar_label: The Heron API Server
original_id: deployment-api-server
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

The **Heron API server** is a necessary component

> If you're running Heron [locally](../../getting-started) on your laptop, you won't need to run the Heron API server separately; its functions will be handled automatically.

## Installation

The Heron API server executable (`heron-apiserver`) is installed automatically when you install the [Heron tools](getting-started-local-single-node#step-1-download-the-heron-tools).

## Running the Heron API server

You can start up the Heron API server using the `heron-apiserver` command. When you do so you'll need to specify two things:

* A [base template](#base-templates) for the scheduler that the API server will be interacting with
* A [cluster name](#cluster-name) for the Heron cluster

Here's an example:

```bash
$ heron-apiserver \
  --base-template mesos \
  --cluster sandbox
```

## Base templates

The Heron API server works by accepting incoming commands from the [Heron CLI tool](user-manuals-heron-cli) and interacts with a variety of Heron components, including:

* a scheduler ([Mesos](schedulers-mesos-local-mac), [Aurora](schedulers-aurora-cluster), the [local filesystem](schedulers-local), etc.)
* an uploader ([Amazon S3](uploaders-amazon-s3), the [local filesystem](uploaders-local-fs), etc.)

When you [install](#installation) the Heron tools, a directory will automatically be created in `~/.herontools/conf` on MacOS and `/usr/local/herontools/conf` on other platforms. That directory contains a number of base templates for all of the currently supported schedulers. Modify the configuration for your scheduler, for example [Mesos](schedulers-mesos-local-mac) using the YAML files in the `mesos` folder, and then select the proper base template using the `--base-template` flag. Here's an example for Mesos:

```bash
$ heron-apiserver \
  --base-template mesos \
  --cluster my-cluster
```

> For a full guide to Heron configuration, see [Configuring a cluster](cluster-config-overview).

## Cluster name

In addition to specifying a base template when starting up the API server, you also need to specify a name for the cluster that the Heron API server will be serving. Here's an example:

```bash
$ heron-apiserver \
  --base-template mesos \
  --cluster us-west-prod
```

{{< alert "api-server-cluster-name" >}}

## Other options

In addition to specifying a base template and cluster name, you can also specify:

Flag | Description
:----|:-----------
`--config-path` | A non-default path to a base configuration
`--port` | The port to bind to (the default is 9000)

## Configuration overrides

When you specify a [base template](#base-templates) when running the Heron API server, the server will use whatever configuration is found in the template files. You can override configuration on a per-parameter basis, however, using the `-D` flag. Here's an example:

```bash
$ heron-apiserver \
  --base-template aurora \
  --cluster us-west-prod \
  -D heron.statemgr.connection.string=zk-1:2181,zk-2:2181,zk-3:2181 \
  -D heron.class.uploader=com.acme.uploaders.MyCustomUploader
```