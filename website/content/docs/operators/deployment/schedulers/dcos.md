---
title: DC/OS Deployments
---

Heron supports deployment on [DC/OS](http://dcos.io/). Deployments on DC/OS are done via Marathon
and Docker. Further command-line support for DC/OS is forthcoming.

## How Heron on DC/OS Works

Heron with DC/OS works by creating its topology deployments in Marathon under a single group. Note
that only the `submit` and `kill` Heron CLI commands are currently supported with DC/OS and Marathon.

## ZooKeeper

To run Heron on DC/OS, you'll need a Zookeeper cluster. In non-production environments you can use 
the same ZK cluster as the Mesos masters, but this is not recommended for production environments.
Within a Mesos cluster, the zookeeper nodes can be addressed via the `leader.mesos` hostname.

## Hosting Binaries

To deploy Heron, the Mesos cluster needs access to the
Heron core binary, which can be hosted wherever you'd like, so long as
it's accessible to all agents (for example in [Amazon
S3](https://aws.amazon.com/s3/) or using a local blob storage solution). You
can download the core binary from Github or build it using the instructions
in [Creating a New Heron Release](../../../../developers/compiling#building-a-full-release-package).

The only method for fetching the binaries currently is via fetching them from a URI. Fetching them
via an S3 bucket or any other URI is supported, as long as it is accessible from every agent in the 
cluster.

## Uploading the Topologies

Heron uses an uploader to upload the topology to a shared location so that a worker can fetch
the topology to its sandbox. The configuration for an uploader is in the `uploader.yaml`
config file. For DC/OS deployments, Heron can use the `S3Uploader`.
Details on configuring the S3 Uploader can be found in the documentation [here](../../uploaders/s3).

## Marathon Scheduler Configuration

To configure Heron to use the Marathon scheduler, modify the `scheduler.yaml`
config file specific for the Heron cluster. The following must be specified
for each cluster:

* `heron.class.scheduler` --- Indicates the class to be loaded for Aurora scheduler.
You should set this to `org.apache.heron.scheduler.marathon.MarathonScheduler`

* `heron.class.launcher` --- Specifies the class to be loaded for launching and
submitting topologies. To configure the Aurora launcher, set this to
`org.apache.heron.scheduler.marathon.MarathonLauncher

* `heron.directory.sandbox.java.home` --- Specifies the location of the Java 8 JRE. Depending on 
the Docker container you're using for executor image, this may be different. Most often, this 
will already be available within the environment as `$JAVA_HOME`

* `heron.directory.home` --- Specifies the directory for the core binaries

* `heron.director.conf` --- Specifies the directory where the Heron configuration files are located.

* `heron.marathon.scheduler.uri` --- Specifies the URI for the Marathon service. Within DC/OS
environments on AWS, this will be available at `<core.dcos_url>/service/marathon`. You can find your
`core.dcos_url` by executing `dcos config show` from your terminal if you have the DC/OS CLI 
installed.

* `heron.marathon.scheduler.auth.token` --- Provides an auth token to use when submitting a topology
to a marathon instance that requires authentication. One can retrieve an auth token by logging in
to the cluster via the DC/OS CLI (`dcos auth login`) and then copying the token (`dcos_acs_token`)
that is stored in `~/.dcos/dcos.toml`

* `heron.scheduler.is.service` --- This config indicates whether the scheduler
is a service. In the case of Marathon, it should be set to `False`.

* `heron.package.core.uri` --- URI of where the core heron binaries can be found. This should be
available via Github in the latest release version. You could also build this project yourself and
make them available somewhere that is accessible by every agent in the Mesos cluster (like an S3
bucket).

* `heron.executor.docker.image` --- Specified the Docker image to be used for running the executor.
For the time being, a public image has been made available at `ndustrialio/heron-executor:jre8`. 
However, you can create your own version of this Docker image by creating a Docker image with
the Dockerfile provided at `heron/docker/Dockerfile.dist.ubuntu14.04` and pushing it to your own 
publicly-available Docker repository.

### Example Marathon Scheduler Configuration

```yaml
# scheduler class for distributing the topology for execution
heron.class.scheduler:                       org.apache.heron.scheduler.marathon.MarathonScheduler

# launcher class for submitting and launching the topology
heron.class.launcher:                        org.apache.heron.scheduler.marathon.MarathonLauncher

# location of java - pick it up from shell environment
heron.directory.sandbox.java.home:          $JAVA_HOME

# location of heron - pick it up from shell environment
heron.directory.home:                       $HERON_HOME

heron.directory.conf:                       "./heron-conf/"

# The URI of marathon scheduler
heron.marathon.scheduler.uri: "<core.dcos_url>/service/marathon"

# The token of the marathon scheduler
heron.marathon.scheduler.auth.token: "<auth_token>"

# Invoke the IScheduler as a library directly
heron.scheduler.is.service:                  False

# docker repo for heron with core packages installed
heron.executor.docker.image: 'streamlio/heron:latest-ubuntu14.04'
```


## State Manager Configuration

To configure Heron to use the correct State Manager configuration, modify the `statemgr.yaml` 
configuration file for your Marathon cluster. 

* `heron.class.state.manager` --- Specifies the class of the state manager you want to use. In a 
Marathon cluster, you'll want to use Zookeeper so set this to 
`org.apache.heron.statemgr.zookeeper.curator.CuratorStateManager`

* `heron.statemgr.connection.string` --- Specifies the connection string to the zookeeper cluster. 
This can be `leader.mesos:2181` within DC/OS-based Mesos clusters if you're in the non-production
environment

* `heron.statemgr.root.path` --- Specifies the node within Zookeeper where information should be 
stored.

* `heron.statemgr.zookeeper.is.initialize.tree` --- Specifies whether or not the State Manager can
initialize the tree in Zookeeper. This should most often be set to true

Most often with Mesos clusters living in a cloud environment, you are going to need to create a 
tunneled connection to the cluster to submit and kill topology jars since communication with TMaster
instances is needed. It does not particularly matter which node you use for your tunnel, but you 
will need to have a private key in your list of authorized keys on the Mesos node you're going to 
use. If you need a quick refresher on this, here's a CLI command you can use (assuming you've 
already generated a key already in your `~/.ssh/` directory: 
`cat ~/.ssh/id_rsa.pub | ssh <user>@<server_ip> "mkdir -p ~/.ssh && cat >>  ~/.ssh/authorized_keys"`

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

* `heron.statemgr.tunnel.host` --- The host to SSH tunnel through in <user>@<host> format

### Example State Manager Configuration

```yaml
# local state manager class for managing state in a persistent fashion
heron.class.state.manager: org.apache.heron.statemgr.zookeeper.curator.CuratorStateManager

# local state manager connection string
heron.statemgr.connection.string:  "leader.mesos:2181"

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
heron.statemgr.tunnel.host:              "centos@<host>"
```

## Working with Topologies

After setting up ZooKeeper and generating a DCOS-accessible Heron core binary
release, you can run topologies on any machine in the cluster. Since every deployment
is done via Docker, there's no need to worry about individual agent configurations. When launching
topologies using the `heron submit` command, you can look in your DC/OS Services UI to see the 
launched topology being launched under a group.

## Troubleshooting

### Stuck in Waiting State
Once a topology is launched, depending on how many resources are available in you DC/OS cluster,
the topology may get stuck in the "Waiting" phase of deployment within Marathon. What this most 
likely means is that you don't have enough resources in your cluster. When clicking into the 
topology group within the UI, you can see how many resources each Heron container is requesting. If
the CPUs and Memory are larger than what a single node can handle (depending on what size your 
agent nodes are), Marathon won't be able to accept any of the offers for resources. In this case,
either increase the number of State Managers your topology is trying to use, or use different agent
nodes with more resources.

Another reason for this could be that you don't have enough nodes. In this case, either tune your
topology to use less resources, or increase the number of nodes in your cluster.

**NOTE:** In order to kill a topology that is stuck in the "Waiting" state, you MUST "Suspend" that
group within Marathon. If you fail to do this and try to kill the topology using the `heron kill`
command, it won't work. You must suspend the topology first! Also, do not ever "Destroy" the group
from within Marathon. Doing this doesn't clear out the appropriate information in Zookeeper and
future attempts to submit the topology using `heron submit` will be unsuccessful until ZK is
cleaned up for that topology name.


