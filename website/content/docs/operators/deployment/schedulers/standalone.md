---
title: Heron standalone
---

Heron enables you to easily run a multi-node cluster in **standalone mode**. The difference between standalone mode and [local mode](../local) for Heron is that standalone mode involves running multiple compute nodes---using [Hashicorp](https://www.hashicorp.com/)'s [Nomad](https://www.nomadproject.io/) as a scheduler---rather than just one.

## Installation

You can use Heron in standalone mode using the `heron-admin` CLI tool, which can be installed using the instructions [here](../../../../getting-started).

## Requirements

In order to run Heron in standalone mode, you'll need to run a [ZooKeeper](https://zookeeper.apache.org) cluster.

## Configuration

Once you have the `heron-admin` CLI tool installed, you need to provide a list of hosts for both the Heron cluster itself and for [ZooKeeper](https://zookeeper.apache.org).

You can easily do this by running the following command:

```bash
$ heron-admin standalone set
```

That will open up an `inventory.yaml` file in whichever editor is specified in your `EDITOR` environment variable. The default is [Vim](http://www.vim.org/). That YAML file looks like this:

```yaml
cluster:
- 127.0.0.1
zookeepers:
- 127.0.0.1
```

You can modify the file to include all hosts for your standalone cluster and for ZooKeeper. Once you've added the lists of hosts for the Heron standalone cluster and ZooKeeper and saved the file, you can move on to [starting the cluster](#starting-and-stopping-the-cluster).

> To run Heron in standalone mode locally on your laptop, use the defaults that are already provided in the `inventory.yaml` file.

## Starting and stopping the cluster

To start Heron in standalone mode once the host configuration has been applied:

```bash
$ heron-admin standalone cluster start
```

You should see output like this:

```bash
[2018-01-22 10:37:06 -0800] [INFO]: Roles:
[2018-01-22 10:37:06 -0800] [INFO]:  - Master Servers: ['127.0.0.1']
[2018-01-22 10:37:06 -0800] [INFO]:  - Slave Servers: ['127.0.0.1']
[2018-01-22 10:37:06 -0800] [INFO]:  - Zookeeper Servers: ['127.0.0.1']
[2018-01-22 10:37:06 -0800] [INFO]: Updating config files...
[2018-01-22 10:37:06 -0800] [INFO]: Starting master on 127.0.0.1
[2018-01-22 10:37:06 -0800] [INFO]: Done starting masters
[2018-01-22 10:37:06 -0800] [INFO]: Starting slave on 127.0.0.1
[2018-01-22 10:37:06 -0800] [INFO]: Done starting slaves
[2018-01-22 10:37:06 -0800] [INFO]: Waiting for cluster to come up... 0
[2018-01-22 10:37:08 -0800] [INFO]: Starting Heron API Server on 127.0.0.1
[2018-01-22 10:37:08 -0800] [INFO]: Waiting for apiserver to come up... 0
[2018-01-22 10:37:09 -0800] [INFO]: Waiting for apiserver to come up... 1
[2018-01-22 10:37:16 -0800] [INFO]: Done starting Heron API Server
[2018-01-22 10:37:16 -0800] [INFO]: Starting Heron Tools on 127.0.0.1
[2018-01-22 10:37:16 -0800] [INFO]: Waiting for apiserver to come up... 0
[2018-01-22 10:37:17 -0800] [INFO]: Done starting Heron Tools
[2018-01-22 10:37:17 -0800] [INFO]: Heron standalone cluster complete!
```

If you see the `Heron standalone cluster complete!` message, that means that the cluster is ready for you to [submit](#submitting-a-topology) and manage topologies.

You can stop the cluster at any time using the `stop` command:

```bash
$ heron-admin standalone cluster stop
```

You will be prompted to confirm that you want to stop the cluster by typing **yes** or **y** (or **no** or **n** if you don't want to). If you enter **yes** or **y** and press **Enter**, all Heron-related jobs will be de-scheduled on Nomad.

## Submitting a topology

Once your standalone cluster is up and running, you can submit and manage topologies using the [Heron CLI tool](../../../heron-cli) and specifying the `standalone` cluster. Here's an example topology submission command:

```bash
$ heron submit standalone \
  ~/.heron/examples/heron-streamlet-examples.jar \
  com.twitter.heron.examples.streamlet.WindowedWordCountTopology \
  WindowedWordCount
```

Once the topology has been submitted, it can be deactivated, killed, updated, and so on, just like topologies on any other scheduler.

## Fetching info about your standalone cluster

At any time, you can retrieve information about your standalone cluster by running:

```bash
$ heron-admin standalone info
```

This will return a JSON string containing a list of hosts for Heron and ZooKeeper as well as URLs for the [Heron API server](../../../heron-api-server), [Heron UI](../../../heron-ui), and [Heron Tracker](../../../heron-tracker). Here is a cluster info JSON string if all defaults are retained:

```json
{
  "numNodes": 1,
  "nodes": [
    "127.0.0.1"
  ],
  "roles": {
    "masters": [
      "127.0.0.1"
    ],
    "slaves": [
      "127.0.0.1"
    ],
    "zookeepers": [
      "127.0.0.1"
    ]
  },
  "urls": {
    "serviceUrl": "http://127.0.0.1:9000",
    "heronUi": "http://127.0.0.1:8889",
    "heronTracker": "http://127.0.0.1:8888"
  }
}
```

You can also get more specific bits of info using the `get` command:

```bash
# Heron Tracker URL
$ heron-admin standalone get heron-tracker-url

# Heron UI URL
$ heron-admin standalone get heron-ui-url

# Heron cluster service URL
$ heron-admin standalone get service-url
```

## Managing Nomad

Heron standalone uses [Nomad](https://www.nomadproject.io/) as a scheduler. For the most part, you shouldn't need to interact with Nomad when managing your Heron standalone cluster. If you do need to manage Nomad directly, however, you can do so using the `heron-nomad` executable, which is installed at `~/.heron/bin/heron-nomad`. That executable is essentially an alias for the `nomad` CLI tool. You can find documentation in the [official Nomad docs](https://www.nomadproject.io/docs/commands/index.html).

You can also access the [Nomad Web UI](https://www.nomadproject.io/guides/ui.html) on port 4646 of any master node in the Heron cluster. You can see a list of master nodes by running `heron-admin standalone info`. If you're running a standalone cluster locally on your machine, you can access the Nomad UI at `localhost:4646`.