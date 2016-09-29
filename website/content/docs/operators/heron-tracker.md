---
title: Heron Tracker
---

**Heron Tracker** is a web service that continuously gathers a wide
variety of information about Heron topologies and exposes
that information through a [JSON REST API](../heron-tracker-api).
More on the role of the Tracker can be found
[here](../../concepts/architecture#heron-tracker).

## Building Heron Tracker

Heron uses [bazel](http://bazel.io/) for compiling.
[Compiling](../../developers/compiling/compiling) describes how to setup bazel
for heron.

```bash
# Build heron-tracker
$ bazel build heron/tools/tracker/src/python:heron-tracker

# The location of heron-tracker pex executable is
# bazel-bin/heron/tools/tracker/src/python/heron-tracker
# To run using default options:
$ ./bazel-bin/heron/tools/tracker/src/python/heron-tracker
```

`heron-tracker` is a self executable
[pex](https://pex.readthedocs.io/en/latest/whatispex.html) archive.

### Heron Tracker Config File

The config file is a `yaml` file that should contain the following information.

#### 1. State Manager locations

This is a list of locations where topology writes its states. An example of
[zookeeper state manager](../deployment/statemanagers/zookeeper) and
[local file state manager](../deployment/statemanagers/localfs) look like this:

```yaml
## Contains the sources where the states are stored.
# Each source has these attributes:
# 1. type - type of state manager (zookeeper or file, etc.)
# 2. name - name to be used for this source
# 3. hostport - only used to connect to zk, must be of the form 'host:port'
# 4. rootpath - where all the states are stored
# 5. tunnelhost - if ssh tunneling needs to be established to connect to it
statemgrs:
  -
    type: "file"
    name: "local"
    rootpath: "~/.herondata/repository/state/local"
    tunnelhost: "localhost"
#
# To use 'localzk', launch a zookeeper server locally
# and create the following path:
#  *. /heron/topologies
#
#  -
#    type: "zookeeper"
#    name: "localzk"
#    hostport: "localhost:2181"
#    rootpath: "/heron"
#    tunnelhost: "localhost" -
```

Topologies from all the state managers would be read and can be queried from
Tracker.

Note that Tracker does not create any zookeeper nodes itself. It is a readonly
service. If you launch the Tracker without creating `/topologies` node under the
rootpath, Tracker will fail with a `NoNodeError`.

#### 2. Viz URL Format

This is an optional config. If it is present, then it will show up for each
topology as the viz link as shown below. For each topology, these parameters
will be filled appropriately. This parameter can be used to link metrics
dashboards with topology UI page.

![Viz Link](/img/viz-link.png)

```yaml
# The URL that points to a topology's metrics dashboard.
# This value can use following parameters to create a valid
# URL based on the topology. All parameters are self-explanatory.
# These are found in the execution state of the topology.
#
#   ${CLUSTER}
#   ${ENVIRON}
#   ${TOPOLOGY}
#   ${ROLE}
#   ${USER}
#
# This is a sample, and should be changed to point to corresponding dashboard.
#
# viz.url.format: "http://localhost/${CLUSTER}/${ENVIRON}/${TOPOLOGY}/${ROLE}/${USER}"
```

### Heron Tracker Args

* `--port` - Port to run the heron-tracker on. Default port is `8888`.
* `--config-file` - The location of the config file for tracker. Default config
  file is `~/.herontools/conf/heron_tracker.yaml`.

```bash
$ heron-tracker
# is equivalent to
$ heron-ui --port=8888 --config-file=~/.herontools/conf/heron_tracker.yaml
```
