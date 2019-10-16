---
id: version-0.20.0-incubating-user-manuals-heron-cli
title: Managing Topologies with Heron CLI
sidebar_label: Heron Client
original_id: user-manuals-heron-cli
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

The **Heron CLI** us used to to manage every aspect of the
[topology lifecycle](heron-topology-concepts#topology-lifecycle).

## Deploying the `heron` CLI Executable

To use `heron` CLI, download the `heron-install` for your platfrom from
[release binaries](https://github.com/apache/incubator-heron/releases) and  run the
installation script. For example, if you have downloaded the version `0.17.6`,
you invoke the installation script as follows

```bash
$ chmod +x heron-install-0.17.6-darwin.sh
$ ./heron-install-0.17.6-darwin.sh --user
Heron client installer
----------------------

Uncompressing......

Heron is now installed!

Make sure you have "/Users/$USER/bin" in your path.

See http://heronstreaming.io/docs/getting-started.html on how to use Heron!

....
```

Alternatively, generate a full [Heron release](compiling-overview) and
distribute the resulting `heron` CLI to all machines used to manage topologies.

### Common CLI Args

All topology management commands (`submit`, `activate`, `deactivate`,
`restart`, `update` and `kill`) take the following required arguments:

* `cluster` --- The name of the cluster where the command needs to be executed.

* `role` --- This represents the user or the group depending on deployment.
  If not provided, it defaults to the unix user.

* `env` --- This is a tag for including additional information (e.g) a
   topology can be tagged as PROD or DEVEL to indicate whether it is in production
   or development. If `env` is not provided, it is given a value `default`

`cluster`, `role` and `env` are specified as a single argument in the form of
`cluster/role/env` (e.g) `local/ads/PROD` to refer the cluster `local` with
role `ads` and the environment `PROD`. If you just want to specify `cluster`, the
argument will be simply `local`.

### Optional CLI Flags

CLI supports a common set of optional flags for all topology management commands
(`submit`, `activate`, `deactivate`, `restart`, `update` and `kill`):

* `--config-path` --- Every heron cluster must provide a few configuration
  files that are kept under a directory named after the cluster. By default,
  when a cluster is provided in the command, it searches the `conf` directory
  for a directory with the cluster name. This flag enables you to specify a
  non standard directory to search for the cluster directory.

* `--config-property` --- Heron supports several configuration parameters
  that be overridden. These parameters are specified in the form of `key=value`.

* `--verbose` --- When this flag is provided, `heron` CLI prints logs
  that provide detailed information about the execution.

Below is an example topology management command that uses one of these flags:

```bash
$ heron activate --config-path ~/heronclusters devcluster/ads/PROD AckingTopology
```

## Submitting a Topology

To run a topology in a Heron cluster, submit it using the `submit` command.
Topologies can be submitted in either an activated (default) or deactivated state
(more on [activation](#activating-a-topology) and [deactivation](#deactivating-a-topology)
below).

Below is the basic syntax:

```bash
$ heron help submit
usage: heron submit [options] cluster/[role]/[env] topology-file-name topology-class-name [topology-args]

Required arguments:
  cluster/[role]/[env]  Cluster, role, and env to run topology
  topology-file-name    Topology jar/tar/zip file
  topology-class-name   Topology class name

Optional arguments:
  --config-path (a string; path to cluster config; default: "/Users/$USER/.heron/conf")
  --config-property (key=value; a config key and its value; default: [])
  --deploy-deactivated (a boolean; default: "false")
  --topology-main-jvm-property Define a system property to pass to java -D when running main.
  --verbose (a boolean; default: "false")
```

Arguments of the `submit` command:

* **cluster/[role]/[env]** --- The cluster where topology needs to be submitted,
  optionally taking the role and environment. For example,`local/ads/PROD` or just `local`

* **topology-file-name** --- The path of the file in which you've packaged the
  topology's code. For Java topologies this will be a `.jar` file; for
  topologies in other languages (not yet supported), this could be a
  `.tar` file. For example, `/path/to/topology/my-topology.jar`

* **topology-class-name** --- The name of the class containing the `main` function
  for the topology. For example, `com.example.topologies.MyTopology`

* **topology-args** (optional) --- Arguments specific to the topology.
  You will need to supply additional args only if the `main` function for your
  topology requires them.

### Example Topology Submission Command

Below is an example command that submits a topology to a cluster named `devcluster`
with a main class named `com.example.topologies.MyTopology` packaged in `my-topology.jar`,
along with the optional `--config-path` where the config for `devcluster` can be found:

```bash
$ heron submit --config-path ~/heronclusters devcluster /path/to/topology/my-topology.jar \
    com.example.topologies.MyTopology my-topology
```

### Other Topology Submission Options

| Flag                           | Meaning                                                                 |
|:-------------------------------|:------------------------------------------------------------------------|
| `--deploy-deactivated`         | If set, the topology is deployed in a deactivated state.                |
| `--topology-main-jvm-property` | Defines a system property to pass to java -D when running topology main |


## Activating a Topology

Topologies are submitted to the cluster in the activated state by default. To
activate a deactivated topology use the `activate` command. Below is the basic
syntax:

```bash
$ heron help activate
usage: heron activate [options] cluster/[role]/[env] topology-name

Required arguments:
  cluster/[role]/[env]  Cluster, role, and env to run topology
  topology-name         Name of the topology

Optional arguments:
  --config-path (a string; path to cluster config; default: "/Users/$USER/.heron/conf")
  --config-property (key=value; a config key and its value; default: [])
```

Arguments of the `activate` command:

* **cluster/[role]/[env]** --- The cluster where topology needs to be submitted,
  optionally taking the role and environment. For exampple, `local/ads/PROD` or just `local`

* **topology-name**  --- The name of the already-submitted topology that you'd
  like to activate.

### Example Topology Activation Command

```bash
$ heron activate local/ads/PROD my-topology
```

## Deactivating a Topology

You can deactivate a running topology at any time using the `deactivate`
command. Here's the basic syntax:

```bash
$ heron help deactivate
usage: heron deactivate [options] cluster/[role]/[env] topology-name

Required arguments:
  cluster/[role]/[env]  Cluster, role, and env to run topology
  topology-name         Name of the topology

Optional arguments:
  --config-path (a string; path to cluster config; default: "/Users/kramasamy/.heron/conf")
  --config-property (key=value; a config key and its value; default: [])
  --verbose (a boolean; default: "false")

```

Arguments of the `deactivate` command:

* **cluster/[role]/[env]** --- The cluster where topology needs to be submitted,
  optionally taking the role and environment. For example, `local/ads/PROD` or just `local`

* **topology-name** --- The name of the topology that you'd like to deactivate.

## Restarting a Topology

You can restart a deactivated topology using the `restart` command (assuming
that the topology has not yet been killed, i.e. removed from the cluster).

```bash
$ heron help restart
usage: heron restart [options] cluster/[role]/[env] topology-name [container-id]

Required arguments:
  cluster/[role]/[env]  Cluster, role, and env to run topology
  topology-name         Name of the topology
  container-id          Identifier of the container to be restarted

Optional arguments:
  --config-path (a string; path to cluster config; default: "/Users/kramasamy/.heron/conf")
  --config-property (key=value; a config key and its value; default: [])
  --verbose (a boolean; default: "false")
```

Arguments of the `restart` command:

* **cluster/[role]/[env]** --- The cluster where topology needs to be submitted,
  optionally taking the role and environment. For example, `local/ads/PROD` or just `local`

* **topology-name** --- The name of the topology that you'd like to restart.

* **container-id** (optional) --- This enables you to specify the container ID to be
  restarted if you want to restart only a specific container of the topology.

### Example Topology Restart Command

```bash
$ heron restart local/ads/PROD my-topology
```

## Updating a Topology

You can update the parallelism of any of the components of a deployed
topology using the `update` command.

```bash
$ heron help update
usage: heron update [options] cluster/[role]/[env] <topology-name> --component-parallelism <name:value>

Required arguments:
  cluster/[role]/[env]  Cluster, role, and environment to run topology
  topology-name         Name of the topology

Optional arguments:
  --component-parallelism COMPONENT_PARALLELISM
                        Component name and the new parallelism value colon-
                        delimited: [component_name]:[parallelism]
  --config-path (a string; path to cluster config; default: "/Users/billg/.heron/conf")
  --config-property (key=value; a config key and its value; default: [])
  --verbose (a boolean; default: "false")
```

Arguments of the `update` command include **cluster/[role]/[env]** and
**topology-name** as well as:

* **--component-parallelism** --- This argument can be included multiple
times to change the parallelism of components in the deployed topology.

### Example Topology Update Command

```bash
$ heron update local/ads/PROD my-topology \
  --component-parallelism=my-spout:2 \
  --component-parallelism=my-bolt:4
```

## Killing a Topology

If you've submitted a topology to your Heron cluster and would like to remove
knowledge of the topology entirely, you can remove it using the `kill` command.
Here's the basic syntax:

```bash
$ heron kill <killer-overrides> <topology>
```

Arguments of the `kill` command:

* **cluster/[role]/[env]** --- The cluster where topology needs to be submitted,
  optionally taking the role and environment.  For example, `local/ads/PROD` or just
  `local`

* **topology-name** --- The name of the topology that you'd like to kill.

### Example Topology Kill Command

```bash
$ heron kill local my-topology
```

## Heron CLI configuration

When using the Heron CLI tool to interact with Heron clusters, there are two ways to provide configuration for the tool:

* Via command-line flags, such as `--service-url`
* Using the `heron config` interface, which enables you to set, unset, and list configs in the local filesystem

### Available parameters

The following parameters can currently be set using the `heron config` interface:

Parameter | Description | Corresponding CLI flag
:---------|:------------|:----------------------
`service_url` | The service URL for the Heron cluster | `--service-url`

### Set configuration

You can set a config using the `set` command. Here's an example:

```bash
$ heron config us-west-staging set service_url http://us-west.staging.example.com:9000
```

### Unset configuration

You can remove a parameter using the `unset` command. Here's an example:

```bash
$ heron config apac-australia unset service_url
```

### List configuration

You can list all of the CLI configs for a Heron cluster using the `list` command. This will return the configs as a list of `parameter = value` pairs. Here's an example:

```bash
$ heron config local list
service_url = http://localhost:9000
```

### Configuration example

Let's say that you need to interact with a Heron cluster called `apac-japan-staging` which has a service
URL of http://apac-japan.staging.example.com:9000. If you specified the service URL via CLI flags, you'd need
to set the flag every time you perform an operation involving that cluster:

```bash
$ heron deactivate apac-japan-staging MyTopology \
  --service-url http://apac-japan.staging.example.com:9000
```

Using `heron config`, however, you could set the service URL for that cluster once and for all:

```bash
$ heron config apac-japan-staging set service_url http://apac-japan.staging.example.com:9000
$ heron deactivate apac-japan-staging MyTopology
```

## Other Commands

### Version

Run the `version` command at any time to see which version of `heron` you're
using:

```bash
$ heron version
heron.build.version : '0.17.6'
heron.build.time : Wed Feb 28 12:08:52 PST 2018
heron.build.timestamp : 1519848532000
heron.build.host : ci-server-01
heron.build.user : release-agent1
heron.build.git.revision : ada6052f6f841e27416b9f9bb4be73183d5a8cd8
heron.build.git.status : Clean
```
