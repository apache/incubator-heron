---
title: Setting Up ZooKeeper State Manager
---

Heron relies on ZooKeeper for a wide variety of cluster coordination tasks. You
can use either a shared or dedicated ZooKeeper cluster.

There are a few things you should be aware of regarding Heron and ZooKeeper:

* Heron uses ZooKeeper only for coordination, *not* for message passing, which
  means that ZooKeeper load should generally be fairly low. A single-node
  and/or shared ZooKeeper *may* suffice for your Heron cluster, depending on
  usage.
* Heron uses ZooKeeper more efficiently than Storm. This makes Heron less likely
  than Storm to require a bulky or dedicated ZooKeeper cluster, but your use
  case may require one.
* We strongly recommend running ZooKeeper [under
  supervision](http://zookeeper.apache.org/doc/r3.3.3/zookeeperAdmin.html#sc_supervision).

### ZooKeeper State Manager Configuration

You can make Heron aware of the ZooKeeper cluster by modifying the
`statemgr.yaml` config file specific for the Heron cluster. You'll
need to specify the following for each cluster:

* `heron.class.state.manager` &mdash; Indicates the class to be loaded for managing 
the state in ZooKeeper and this class is loaded using reflection. You should set this 
to `com.twitter.heron.statemgr.zookeeper.curator.CuratorStateManager`

* `heron.statemgr.connection.string` &mdash; The host IP address and port to connect to ZooKeeper 
cluster (e.g) "127.0.0.1:2181". 

* `heron.statemgr.root.path` &mdash; The root ZooKeeper node to be used by Heron. We recommend 
providing Heron with an exclusive root node; if you do not, make sure that the following child 
nodes are unused: `/tmasters`, `/topologies`, `/pplans`, `/executionstate`, `/schedulers`.

* `heron.statemgr.zookeeper.is.initialize.tree` &mdash; Indicates whether the nodes under ZooKeeper 
root `/tmasters`, `/topologies`, `/pplans`, `/executionstate`, and `/schedulers` need to created, 
if they are not found. Set it to `True` if you could like Heron to create those nodes. If those 
nodes are already there, set it to `False`. The absence of this configuration implies `True`.

* `heron.statemgr.zookeeper.session.timeout.ms` &mdash; Specifies how much time in milliseconds 
to wait before declaring the ZooKeeper session is dead.

* `heron.statemgr.zookeeper.connection.timeout.ms` &mdash; Specifies how much time in milliseconds 
to wait before the connection to ZooKeeper is dead.

* `heron.statemgr.zookeeper.retry.count` &mdash; Count of the number of retry attempts to connect 
to ZooKeeper

* `heron.statemgr.zookeeper.retry.interval.ms`: Time in milliseconds to wait between each retry

### Example ZooKeeper State Manager Configuration

Below is an example configuration (in `statemgr.yaml`) for a ZooKeeper running in `localhost`:

<pre><code>
# local state manager class for managing state in a persistent fashion
heron.class.state.manager: com.twitter.heron.statemgr.zookeeper.curator.CuratorStateManager

# local state manager connection string
heron.statemgr.connection.string:  "127.0.0.1:2181"

# path of the root address to store the state in a local file system
heron.statemgr.root.path: "/heron"

# create the zookeeper nodes, if they do not exist
heron.statemgr.zookeeper.is.initialize.tree: True

# timeout in ms to wait before considering zookeeper session is dead
heron.statemgr.zookeeper.session.timeout.ms: 30000

# timeout in ms to wait before considering zookeeper connection is dead
heron.statemgr.zookeeper.connection.timeout.ms: 30000

# timeout in ms to wait before considering zookeeper connection is dead
heron.statemgr.zookeeper.retry.count: 10

# duration of time to wait until the next retry
heron.statemgr.zookeeper.retry.interval.ms: 10000
</code></pre>
