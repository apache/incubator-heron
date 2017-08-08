---
title: Kubernetes
---

Heron supports deployment on [Kubernetes (k8s)](https://kubernetes.io/). Deployments on Kubernetes 
are done via Docker and the k8s API.

## How Heron on Kubernetes Works

When deploying to k8s, each Heron container is deployed as a Pod within a Docker container. If there
are 20 containers that are going to be deployed with a topoology, then there will be 20 pods
deployed onto your k8s cluster for that topology.

### Current Limitations

Only the `submit` and `kill` commands are currently supported. Other CLI commands (`activate`,
`deactivate`, `restart`, `update`) are currently in development and will be released soon.

## Zookeeper

To run Heron on k8s, you will need a Zookeeper cluster. You can choose to use a zookeeper cluster 
outside of k8s if you'd like (only if it's accessible from the k8s cluster nodes), but most often
you will probably want to deploy your own Zookeeper cluster inside of k8s. This can be done and 
a good example of how to deploy one can be found [here](https://cloudplatform.googleblog.com/2016/04/taming-the-herd-using-Zookeeper-and-Exhibitor-on-Google-Container-Engine.html).

## Uploading the Topologies

Heron uses an uploader to upload the topology to a shared location so that a worker can fetch
the topology to its sandbox. The configuration for an uploader is in the `uploader.yaml`
config file. For k8s deployments, Heron can use the `S3Uploader`.
Details on configuring the S3 Uploader can be found in the documentation [here](../../uploaders/s3).

## Kubernetes Scheduler Configuration

To configure Heron to use the Kubernetes scheduler, modify the `scheduler.yaml`
config file specific for the Heron cluster. The following must be specified
for each cluster:

* `heron.class.scheduler` --- Indicates the class to be loaded for Kubernetes scheduler.
You should set this to `com.twitter.heron.scheduler.kubernetes.KubernetesScheduler`

* `heron.class.launcher` --- Specifies the class to be loaded for launching and
submitting topologies. To configure the Kubernetes launcher, set this to
`com.twitter.heron.scheduler.kubernetes.KubernetesLauncher`

* `heron.directory.sandbox.java.home` --- Specifies the location of the Java 8 JRE. Depending on 
the Docker container you're using for executor image, this may be different. If you're using
the Streaml.io-hosted containter, this will already be available within the environment as 
`$JAVA_HOME`

* `heron.directory.home` --- Specifies the directory for the core binaries. This should be set to
`$HERON_HOME`

* `heron.director.conf` --- Specifies the directory where the Heron configuration files are located.
This should be set to `/opt/heron/heron-conf/`

* `heron.kubernetes.scheduler.uri` --- Specifies the URI for the Kubernetes API service. If you're 
using `kubectl proxy` to access your API, this will need to be set to `http://localhost:8001` if 
you're using the standard proxy configuration.

* `heron.scheduler.is.service` --- This config indicates whether the scheduler
is a service. In the case of Kubernetes, it should be set to `False`.

* `heron.executor.docker.image` --- Specified the Docker image to be used for running the executor.
To use the Docker image created for the latest release, you can set this value to 
`streamlio/heron:<version>-<os>`. Example: `streamlio/heron:0.14.8-ubuntu14.04`

### Example Kubernetes Scheduler Configuration

```yaml
# scheduler class for distributing the topology for execution
heron.class.scheduler:                       com.twitter.heron.scheduler.kubernetes.KubernetesScheduler

# launcher class for submitting and launching the topology
heron.class.launcher:                        com.twitter.heron.scheduler.kubernetes.KubernetesLauncher

# location of java - pick it up from shell environment
heron.directory.sandbox.java.home:          $JAVA_HOME

heron.directory.conf:                       "/opt/heron/heron-conf/"

heron.directory.home:                       $HERON_HOME

# The URI of kubernetes API
heron.kubernetes.scheduler.uri: "http://localhost:8001"

# Invoke the IScheduler as a library directly
heron.scheduler.is.service:                  False

# docker repo for executor
heron.executor.docker.image: 'streamlio/heron:0.14.8-ubuntu14.04'
```

## State Manager Configuration

To configure Heron to use the correct State Manager configuration, modify the `statemgr.yaml` 
configuration file for your Kubernetes cluster. 

* `heron.class.state.manager` --- Specifies the class of the state manager you want to use. In a 
Kubernetes cluster, you'll want to use Zookeeper so set this to 
`com.twitter.heron.statemgr.zookeeper.curator.CuratorStateManager`

* `heron.statemgr.connection.string` --- Specifies the connection string to the zookeeper cluster. 
This will look something like `<zookeeper_ip>:2181`.

* `heron.statemgr.root.path` --- Specifies the node within Zookeeper where information should be 
stored.

* `heron.statemgr.zookeeper.is.initialize.tree` --- Specifies whether or not the State Manager can
initialize the tree in Zookeeper. This should most often be set to true

Most often with Heron deployments in a cluster environment, you are going to need to create a 
tunneled connection to the cluster to submit and kill topology jars since communication with TMaster
instances is needed. 

To tunnel, you will need to add these extra configurations within your `statemgr.yaml` file.

* `heron.statemgr.is.tunnel.needed` --- Flag for enabling the tunnel to a server within your 
cluster. If you need the tunnel, which you most likely will, set this flag to `true`

* `heron.statemgr.tunnel.connection.timeout.ms` --- The connection timeout in ms when trying to 
connect to zk server

* `heron.statemgr.tunnel.connection.retry.count` --- The count of retry to check whether has direct 
access on zk server

* `heron.statemgr.tunnel.retry.interval.ms` --- The interval in ms between two retry checking 
whether has direct access on zk server

* `heron.statemgr.tunnel.verify.count` --- The count of retry to verify whether seting up a tunnel 
process

* `heron.statemgr.tunnel.host` --- The host to SSH tunnel through in `<user>@<host>` format

### Example State Manager Configuration

```yaml
# local state manager class for managing state in a persistent fashion
heron.class.state.manager: com.twitter.heron.statemgr.zookeeper.curator.CuratorStateManager

# local state manager connection string
heron.statemgr.connection.string:  "<zk_ip>:2181"

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


heron.statemgr.is.tunnel.needed: True

# The connection timeout in ms when trying to connect to zk server
heron.statemgr.tunnel.connection.timeout.ms:    1000

# The count of retry to check whether has direct access on zk server
heron.statemgr.tunnel.connection.retry.count:   2

# The interval in ms between two retry checking whether has direct access on zk server
heron.statemgr.tunnel.retry.interval.ms:        1000

# The count of retry to verify whether seting up a tunnel process
heron.statemgr.tunnel.verify.count:             10

# SSH tunnel host
heron.statemgr.tunnel.host:              "<user>@<host>"
```

## Working with Topologies

After setting up ZooKeeper and setting your configurations like above, you can run topologies on any
machine in the cluster. Since every deployment is done via Docker, there's no need to worry about 
individual agent configurations. When launching topologies using the `heron submit` command, you can
look in your Kubernetes UI to see the launched topology pods.



