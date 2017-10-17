---
title: The Heron API server
---

The **Heron API server** is a necessary component

> If you're running Heron [locally](../../getting-started) on your laptop, you won't need to run the Heron API server separately; its functions will be handled automatically.

## Installation

The Heron API server executable (`heron-apiserver`) is installed automatically when you install the [Heron tools](../../getting-started#step-1-download-the-heron-tools).

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

The Heron API server works by accepting incoming commands from the [Heron CLI tool](../heron-cli) and interacts with a variety of Heron components, including:

* a scheduler ([Mesos](../deployment/schedulers/mesos), [Aurora](../deployment/schedulers/aurora), the [local filesystem](../deployment/schedulers/localfs), etc.)
* an uploader ([Amazon S3](../deployment/uploaders/s3), the [local filesystem](../deployment/uploaders/localfs), etc.)

When you [install](#installation) the Heron tools, a directory will automatically be created in `~/.herontools/conf` on MacOS and `/usr/local/herontools/conf` on other platforms. That directory contains a number of base templates for all of the currently supported schedulers. Modify the configuration for your scheduler, for example [Mesos](../deployment/schedulers/mesos) using the YAML files in the `mesos` folder, and then select the proper base template using the `--base-template` flag. Here's an example for Mesos:

```bash
$ heron-apiserver \
  --base-template mesos \
  --cluster my-cluster
```

> For a full guide to Heron configuration, see [Configuring a cluster](../deployment/configuration).

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