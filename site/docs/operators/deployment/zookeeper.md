# Setting Up ZooKeeper

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

## ZooKeeper Configuration

You can make Heron aware of one or more ZooKeeper clusters by modifying the
`zkstateconf.yaml` config file, which is contained in `heron/state/conf`. You'll
need to specify the following for each cluster:

* `type` &mdash; You should set this to `zookeeper` (the other possibility is
  `file`, which is used in conjunction with [local deployment](local.html)).
* `name` &mdash; The name you'd like to assign to the cluster. This is
  especially important if you're using more than one cluster.
* `host`
* `port`
* `rootpath` &mdash; The root ZooKeeper node to be used by Heron. We recommend
  providing Heron with an exclusive root node; if you do not, make sure that the
  following child nodes are unused: `/tmasters`, `/topologies`, `/pplans`,
  `/executionstate`, `/schedulers`.

You can also specify an SSH tunnel using the `tunnelhost` parameter.

## Example ZooKeeper Configuration

Below is an example configuration (in `heron/state/conf/zkstateconf.yaml`):

```yaml
-
    type: "zookeeper"
    name: "zk"
    host: "zk.acme.biz"
    port: 2181
    rootpath: "/heron/cluster"
    tunnelhost: "tunnel.acme.biz"
```

## Multiple Clusters

If you'd like to run multiple Heron clusters, e.g. for a multi-data center
deployment, you can provide configuration for more than ZooKeeper cluster in
`zkstateconf.yaml`. Here's an example:

```yaml
-
	type: "zookeeper"
	name: "zk1"
	host: "zk1.acme.biz"
    port: 2818
	rootpath: "/heron/cluster"
	tunnelhost: "tunnel.zk1.acme.biz"
-
	type: "zookeeper"
	name: "zk2"
	host: "zk2.acme.biz"
    port: 2818
	rootpath: "/heron/cluster"
	tunnelhost: "tunnel.zk2.acme.biz"
```
